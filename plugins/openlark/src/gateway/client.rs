//! One connection to the gateway's agent socket: JSON-RPC requests out, answers and
//! `changes.batch` notifications in.

use std::collections::HashMap;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::wire::{Change, HelloResult, PROTOCOL};
use crate::config::{Endpoints, Timing};

/// The gateway's close code for a revoked credential.
const CLOSE_REVOKED: u16 = 4401;

/// Why a gateway call did not give a result.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GwError {
    #[error("the gateway cannot be reached")]
    Down,
    #[error("the gateway refused the credential")]
    Unauthorized,
    #[error("the gateway did not answer in time")]
    Timeout,
    /// An error the gateway answered, by its `data.code`.
    #[error("the gateway answered {code}")]
    Rpc { code: String, retry_after_ms: Option<u64> },
    #[error("the gateway did not follow its protocol: {0}")]
    Protocol(&'static str),
}

impl GwError {
    /// The gateway's error code, when it answered one.
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Rpc { code, .. } => Some(code),
            _ => None,
        }
    }
}

struct Request {
    method: &'static str,
    params: Value,
    reply: Option<oneshot::Sender<Result<Value, GwError>>>,
}

/// An open connection, past its hello.
pub struct GwConn {
    tx: mpsc::Sender<Request>,
    closed: watch::Receiver<Option<GwError>>,
    call_timeout: Duration,
    pub hello: HelloResult,
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connects, authenticates and says hello. With `changes`, the connection asks for the change
/// feed and hands every batch to that channel; a full channel holds the socket back.
pub async fn connect(
    gateway: &Endpoints,
    token: &str,
    timing: &Timing,
    changes: Option<mpsc::Sender<Vec<Change>>>,
) -> Result<GwConn, GwError> {
    let mut request = gateway.agent.as_str().into_client_request().map_err(|_| GwError::Protocol("a bad address"))?;
    let bearer = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| GwError::Unauthorized)?;
    request.headers_mut().insert("Authorization", bearer);
    let socket = match tokio::time::timeout(timing.call_timeout, tokio_tungstenite::connect_async(request)).await {
        Err(_) => return Err(GwError::Timeout),
        Ok(Err(tungstenite::Error::Http(resp))) if matches!(resp.status().as_u16(), 401 | 403) => {
            return Err(GwError::Unauthorized);
        }
        Ok(Err(_)) => return Err(GwError::Down),
        Ok(Ok((socket, _))) => socket,
    };
    let wants_changes = changes.is_some();
    let (tx, rx) = mpsc::channel(64);
    let (closed_tx, closed) = watch::channel(None);
    tokio::spawn(pump(socket, rx, changes, timing.gateway_idle, closed_tx));
    let mut conn = GwConn { tx, closed, call_timeout: timing.call_timeout, hello: HelloResult::default() };
    let hello: HelloResult = conn
        .call(
            "hello",
            json!({
                "protocol": PROTOCOL,
                "client": {"name": env!("CARGO_PKG_NAME"), "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {"changes": wants_changes},
            }),
        )
        .await?;
    if hello.protocol != PROTOCOL {
        return Err(GwError::Protocol("another protocol version"));
    }
    conn.hello = hello;
    Ok(conn)
}

impl GwConn {
    /// One request and its typed result.
    pub async fn call<T: DeserializeOwned>(&self, method: &'static str, params: Value) -> Result<T, GwError> {
        let (reply, answer) = oneshot::channel();
        if self.tx.send(Request { method, params, reply: Some(reply) }).await.is_err() {
            return Err(self.end());
        }
        match tokio::time::timeout(self.call_timeout, answer).await {
            Err(_) => Err(GwError::Timeout),
            Ok(Err(_)) => Err(self.end()),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Ok(Ok(v))) => serde_json::from_value(v).map_err(|_| GwError::Protocol("an unexpected result")),
        }
    }

    /// A request whose answer nobody waits for. Dropped when the connection is busy or gone.
    pub fn post(&self, method: &'static str, params: Value) {
        let _ = self.tx.try_send(Request { method, params, reply: None });
    }

    /// Why the connection ended, or `Down` while that is not known.
    pub fn end(&self) -> GwError {
        self.closed.borrow().clone().unwrap_or(GwError::Down)
    }

    /// Resolves when the connection has ended.
    pub async fn closed(&self) -> GwError {
        let mut closed = self.closed.clone();
        loop {
            if let Some(e) = closed.borrow_and_update().clone() {
                return e;
            }
            if closed.changed().await.is_err() {
                return GwError::Down;
            }
        }
    }
}

/// Owns the socket until it ends or every handle is dropped.
async fn pump(
    mut socket: Socket,
    mut requests: mpsc::Receiver<Request>,
    changes: Option<mpsc::Sender<Vec<Change>>>,
    idle: Duration,
    closed: watch::Sender<Option<GwError>>,
) {
    let mut pending: HashMap<u64, oneshot::Sender<Result<Value, GwError>>> = HashMap::new();
    let mut next_id = 1u64;
    let mut last_seen = Instant::now();
    let end = loop {
        tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else { break GwError::Down };
                let id = next_id;
                next_id += 1;
                let frame = json!({"jsonrpc": "2.0", "id": id, "method": request.method, "params": request.params});
                if let Some(reply) = request.reply {
                    pending.insert(id, reply);
                }
                if socket.send(Message::Text(frame.to_string().into())).await.is_err() {
                    break GwError::Down;
                }
            }
            frame = socket.next() => {
                last_seen = Instant::now();
                match frame {
                    None | Some(Err(_)) => break GwError::Down,
                    Some(Ok(Message::Text(text))) => {
                        let Ok(v) = serde_json::from_str::<Value>(text.as_str()) else {
                            break GwError::Protocol("a frame that is not JSON");
                        };
                        match incoming(v, &mut pending) {
                            Some(batch) => {
                                // a feed nobody reads any more ends the connection
                                let Some(feed) = &changes else { continue };
                                if feed.send(batch).await.is_err() {
                                    break GwError::Down;
                                }
                            }
                            None => continue,
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let revoked = frame.is_some_and(|f| u16::from(f.code) == CLOSE_REVOKED);
                        break if revoked { GwError::Unauthorized } else { GwError::Down };
                    }
                    // ping and pong are answered by the socket itself
                    Some(Ok(_)) => {}
                }
            }
            () = tokio::time::sleep_until(last_seen + idle) => break GwError::Down,
        }
    };
    let _ = socket.close(None).await;
    for (_, reply) in pending.drain() {
        let _ = reply.send(Err(end.clone()));
    }
    let _ = closed.send(Some(end));
}

/// Routes one frame: an answer goes to its caller, a change batch is returned.
fn incoming(v: Value, pending: &mut HashMap<u64, oneshot::Sender<Result<Value, GwError>>>) -> Option<Vec<Change>> {
    if let Some(method) = v.get("method").and_then(Value::as_str) {
        if method != "changes.batch" {
            return None;
        }
        let items = v.get("params").and_then(|p| p.get("items")).and_then(Value::as_array)?;
        // an entry that cannot be read is dropped here; the positions around it still arrive
        let batch: Vec<Change> = items.iter().filter_map(|i| serde_json::from_value(i.clone()).ok()).collect();
        return if batch.is_empty() { None } else { Some(batch) };
    }
    let reply = pending.remove(&v.get("id").and_then(Value::as_u64)?)?;
    let result = match v.get("error") {
        Some(e) => Err(rpc_error(e)),
        None => Ok(v.get("result").cloned().unwrap_or(Value::Null)),
    };
    let _ = reply.send(result);
    None
}

fn rpc_error(e: &Value) -> GwError {
    let data = e.get("data");
    let code = data.and_then(|d| d.get("code")).and_then(Value::as_str).unwrap_or("internal");
    let retry_after_ms = data.and_then(|d| d.get("retry_after_ms")).and_then(Value::as_u64);
    GwError::Rpc { code: code.to_string(), retry_after_ms }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_error_is_read_by_its_code() {
        let e = rpc_error(
            &json!({"code": -32000, "message": "slow", "data": {"code": "rate_limited", "retry_after_ms": 700}}),
        );
        assert_eq!(e, GwError::Rpc { code: "rate_limited".into(), retry_after_ms: Some(700) });
        assert_eq!(rpc_error(&json!({"code": -32603})).code(), Some("internal"));
    }

    #[test]
    fn answers_and_batches_are_told_apart() {
        let mut pending = HashMap::new();
        let (tx, mut rx) = oneshot::channel();
        pending.insert(4, tx);
        assert!(incoming(json!({"jsonrpc": "2.0", "id": 4, "result": {"a": 1}}), &mut pending).is_none());
        assert_eq!(rx.try_recv().unwrap(), Ok(json!({"a": 1})));
        let batch = incoming(
            json!({"jsonrpc": "2.0", "method": "changes.batch", "params": {"items": [{"seq": 3, "kind": "message"}]}}),
            &mut pending,
        )
        .unwrap();
        assert_eq!(batch[0].seq, 3);
        assert!(incoming(json!({"jsonrpc": "2.0", "method": "other", "params": {}}), &mut pending).is_none());
    }
}
