//! One connection of the Hub: hello, the requests, the feed's window and redelivery, the status
//! notifications and the deadlines (§2, §3, §5.4, §9).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::Instant;

use super::proto::{
    HISTORY_MAX_LIMIT, MAX_ATTACHMENT_BYTES, MAX_BATCH_ITEMS, MAX_FRAME_BYTES, MAX_IDS_PER_CALL, MAX_INFLIGHT_REQUESTS,
    MAX_TEXT_BYTES, MAX_UNACKED_BATCHES, Reason, Rpc, RpcError, VERSIONS, WRITE_BURST, WRITE_PER_SEC, negotiate,
    notification, now_ms, response,
};
use crate::bridge::{self, Bridge};
use crate::feed::{self, Batch, Event, Feed, Started};
use crate::gateway::link::{LinkState, LinkStatus, Session};
use crate::map;

const CLOSE_GOING_AWAY: u16 = 1001;
const CLOSE_NOT_ACCEPTED: u16 = 1003;
const CLOSE_POLICY: u16 = 1008;
const CLOSE_TOO_BIG: u16 = 1009;
const CLOSE_RESTART: u16 = 1012;
const CLOSE_TRY_LATER: u16 = 1013;

/// What to do after a step of the connection.
enum Flow {
    Go,
    /// Close with a code and a reason the Hub may log.
    Close(u16, &'static str),
    /// The Hub is gone; there is nobody to say anything to.
    Gone,
}

/// The account the hello of this socket announced.
struct Account {
    user: String,
    tenant: String,
}

/// The socket's subscription with its window (§5.4).
struct Sub {
    name: String,
    feed: Feed,
    /// The batches sent and not acked yet, oldest first.
    outstanding: VecDeque<Batch>,
    highest_sent: u64,
    redeliver_at: Instant,
}

/// A `changes.subscribe` whose gateway connection is still being made.
struct Opening {
    id: Value,
    name: String,
    result: oneshot::Receiver<Rpc<(Feed, Started)>>,
}

struct Conn {
    bridge: Arc<Bridge>,
    sink: SplitSink<WebSocket, Message>,
    account: Option<Account>,
    sub: Option<Sub>,
    opening: Option<Opening>,
    /// How many subscriptions this socket has asked for; names them.
    subs: u64,
    told_session: (&'static str, Option<&'static str>, &'static str),
    told_upstream: (&'static str, Option<&'static str>),
    inflight: Arc<Semaphore>,
    replies: mpsc::Sender<String>,
}

async fn next_event(sub: &mut Option<Sub>) -> Option<Event> {
    match sub {
        Some(sub) => sub.feed.events.recv().await,
        None => std::future::pending().await,
    }
}

async fn opened_feed(opening: &mut Option<Opening>) -> Option<Rpc<(Feed, Started)>> {
    match opening {
        Some(opening) => (&mut opening.result).await.ok(),
        None => std::future::pending().await,
    }
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Serves one upgraded socket until either side ends it.
pub async fn run(socket: WebSocket, bridge: Arc<Bridge>) {
    // held so that the bridge can wait for this connection on its way out
    let _alive = bridge.alive.upgrade();
    let timing = bridge.settings.timing.clone();
    let (sink, mut stream) = socket.split();
    let (replies_tx, mut replies) = mpsc::channel::<String>(64);
    let mut link = bridge.link.watch();
    let mut stop = bridge.stop.clone();
    let opened = Instant::now();
    let mut heard = opened;
    let mut conn = Conn {
        bridge,
        sink,
        account: None,
        sub: None,
        opening: None,
        subs: 0,
        told_session: ("ok", None, "forbidden"),
        told_upstream: ("ok", None),
        inflight: Arc::new(Semaphore::new(MAX_INFLIGHT_REQUESTS)),
        replies: replies_tx,
    };
    let closing = loop {
        let ready = conn.account.is_some();
        let deadline = if ready { heard + timing.idle } else { opened + timing.hello_deadline };
        let window_open = conn.sub.as_ref().is_some_and(|s| s.outstanding.len() < MAX_UNACKED_BATCHES);
        let redeliver_at = conn.sub.as_ref().filter(|s| !s.outstanding.is_empty()).map(|s| s.redeliver_at);
        let flow = tokio::select! {
            _ = stop.changed() => Flow::Close(CLOSE_GOING_AWAY, "the plugin is stopping"),
            frame = stream.next() => {
                heard = Instant::now();
                match frame {
                    None | Some(Err(_) | Ok(Message::Close(_))) => Flow::Gone,
                    Some(Ok(Message::Text(text))) => conn.on_text(text.as_str()).await,
                    Some(Ok(Message::Binary(_))) => Flow::Close(CLOSE_NOT_ACCEPTED, "binary frames are not part of the protocol"),
                    // ping and pong are answered by the socket itself
                    Some(Ok(_)) => Flow::Go,
                }
            }
            Some(frame) = replies.recv() => conn.out(frame).await,
            event = next_event(&mut conn.sub), if window_open => conn.on_event(event).await,
            result = opened_feed(&mut conn.opening), if conn.opening.is_some() => conn.on_opened(result).await,
            () = sleep_until(redeliver_at), if redeliver_at.is_some() => conn.redeliver().await,
            changed = link.changed(), if ready => match changed {
                Ok(()) => conn.tell().await,
                Err(_) => Flow::Close(CLOSE_GOING_AWAY, "the plugin is stopping"),
            },
            () = tokio::time::sleep_until(deadline) => {
                if ready {
                    Flow::Close(CLOSE_GOING_AWAY, "no frame from the Hub for too long")
                } else {
                    Flow::Close(CLOSE_POLICY, "no hello in time")
                }
            }
        };
        match flow {
            Flow::Go => {}
            Flow::Gone => break None,
            Flow::Close(code, reason) => break Some((code, reason)),
        }
    };
    if let Some((code, reason)) = closing {
        tracing::debug!(code, "closing the Hub's connection: {reason}");
        let frame = CloseFrame { code, reason: reason.into() };
        let goodbye = async {
            if conn.sink.send(Message::Close(Some(frame))).await.is_ok() {
                // let the Hub answer the close before the socket goes
                while let Some(Ok(_)) = stream.next().await {}
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(1), goodbye).await;
    }
}

impl Conn {
    /// Puts one frame on the wire. A Hub that takes nothing for the idle time is gone.
    async fn out(&mut self, frame: String) -> Flow {
        let idle = self.bridge.settings.timing.idle;
        match tokio::time::timeout(idle, self.sink.send(Message::Text(frame.into()))).await {
            Ok(Ok(())) => Flow::Go,
            _ => Flow::Gone,
        }
    }

    async fn reply(&mut self, id: &Value, result: Rpc) -> Flow {
        self.out(response(id, &result)).await
    }

    async fn on_text(&mut self, text: &str) -> Flow {
        if text.len() > MAX_FRAME_BYTES {
            return Flow::Close(CLOSE_TOO_BIG, "a frame above max_frame_bytes");
        }
        let Ok(Value::Object(frame)) = serde_json::from_str::<Value>(text) else {
            let e = RpcError::new(Reason::ParseError, "the frame is not a JSON-RPC object");
            let _ = self.reply(&Value::Null, Err(e)).await;
            return Flow::Close(CLOSE_NOT_ACCEPTED, "a frame that is not a JSON-RPC object");
        };
        // the server asks the Hub nothing, so an answer has no question; a notification of the
        // Hub's has no meaning in v1
        let Some(method) = frame.get("method").and_then(Value::as_str) else { return Flow::Go };
        let Some(id) = frame.get("id").filter(|id| !id.is_null()) else { return Flow::Go };
        let params = frame.get("params").cloned().unwrap_or_else(|| json!({}));
        if self.account.is_none() {
            if method != "hello" {
                let e = RpcError::new(Reason::NotReady, "hello must be the first request");
                let _ = self.reply(id, Err(e)).await;
                return Flow::Close(CLOSE_POLICY, "hello must be the first request");
            }
            return self.hello(id, &params).await;
        }
        match method {
            "hello" => self.reply(id, Err(RpcError::invalid("hello was already answered on this socket"))).await,
            "ping" => self.reply(id, Ok(json!({"time_ms": now_ms()}))).await,
            "changes.subscribe" => self.subscribe(id, &params).await,
            "changes.ack" => {
                let result = self.ack(&params);
                self.reply(id, result).await
            }
            _ => self.request(id, method, params).await,
        }
    }

    async fn hello(&mut self, id: &Value, params: &Value) -> Flow {
        let version = |key: &str| {
            let v = params.get("protocol").and_then(|p| p.get(key)).and_then(Value::as_u64);
            v.and_then(|v| u32::try_from(v).ok())
        };
        let (Some(min), Some(max)) = (version("min"), version("max")) else {
            return self.reply(id, Err(RpcError::invalid("`protocol` needs `min` and `max`"))).await;
        };
        if negotiate(min, max, VERSIONS).is_none() {
            let e = RpcError::new(Reason::UnsupportedVersion, "this server speaks protocol version 1")
                .with("supported", json!(VERSIONS));
            let _ = self.reply(id, Err(e)).await;
            return Flow::Close(CLOSE_POLICY, "no common protocol version");
        }
        // a bridge that has just started may not have heard from the gateway yet
        let settled = |st: &LinkState| {
            st.status != LinkStatus::Connecting && (st.identity.is_some() || st.status == LinkStatus::Refused)
        };
        let mut link = self.bridge.link.watch();
        let _ = tokio::time::timeout(self.bridge.settings.timing.hello_wait, link.wait_for(settled)).await;
        let st = self.bridge.link.state();
        let Some(me) = &st.identity else {
            // without an account there is no realm to answer with
            let refused = "the openlark gateway refuses the plugin's credential";
            let e = match (st.status, st.session) {
                (LinkStatus::Refused, _) => {
                    RpcError::new(Reason::SessionExpired, refused).with("detail", "gateway_credential_refused")
                }
                (LinkStatus::Up, Session::Expired) => {
                    RpcError::new(Reason::SessionExpired, "the Lark session has expired at the gateway")
                }
                (LinkStatus::Up, Session::Locked) => {
                    RpcError::new(Reason::SessionLocked, "the Lark session is locked at the gateway")
                }
                _ => RpcError::new(Reason::UpstreamUnavailable, "the openlark gateway cannot be reached"),
            };
            let _ = self.reply(id, Err(e)).await;
            return Flow::Close(CLOSE_TRY_LATER, "the signed-in account is not known yet");
        };
        let session = bridge::session_status(&st);
        let upstream = bridge::upstream_status(&st);
        let mut result = json!({
            "protocol": 1,
            "server": {"name": env!("CARGO_PKG_NAME"), "version": env!("CARGO_PKG_VERSION")},
            "realm": map::realm(&me.tenant_id),
            "time_precision_ms": 1,
            "time_ms": now_ms(),
            "features": bridge::features(&st),
            "limits": {
                "max_frame_bytes": MAX_FRAME_BYTES,
                "max_text_bytes": MAX_TEXT_BYTES,
                "max_batch_items": MAX_BATCH_ITEMS,
                "max_unacked_batches": MAX_UNACKED_BATCHES,
                "max_ids_per_call": MAX_IDS_PER_CALL,
                "history_max_limit": HISTORY_MAX_LIMIT,
                "write_per_sec": WRITE_PER_SEC,
                "write_burst": WRITE_BURST,
                "max_attachment_bytes": MAX_ATTACHMENT_BYTES,
                "max_inflight_requests": MAX_INFLIGHT_REQUESTS,
            },
            "session": {"status": session.0},
            "upstream": {"status": upstream.0},
            "write_policy": session.2,
        });
        if let Some(reason) = session.1 {
            result["session"]["reason"] = reason.into();
        }
        self.account = Some(Account { user: me.user.id.clone(), tenant: me.tenant_id.clone() });
        self.told_session = session;
        self.told_upstream = upstream;
        self.reply(id, Ok(result)).await
    }

    /// Tells the Hub what changed at the gateway since it was last told (§9.2), and only that.
    async fn tell(&mut self) -> Flow {
        let st = self.bridge.link.state();
        if let (Some(told), Some(now)) = (&self.account, &st.identity)
            && (told.user != now.user.id || told.tenant != now.tenant_id)
        {
            // a new hello gives the Hub the new realm and user
            return Flow::Close(CLOSE_RESTART, "another account is signed in at the gateway");
        }
        let session = bridge::session_status(&st);
        if session != self.told_session {
            self.told_session = session;
            let mut params = json!({"status": session.0, "write_policy": session.2});
            if let Some(reason) = session.1 {
                params["reason"] = reason.into();
            }
            if let Flow::Gone = self.out(notification("session.status", params)).await {
                return Flow::Gone;
            }
        }
        let upstream = bridge::upstream_status(&st);
        if upstream != self.told_upstream {
            self.told_upstream = upstream;
            let mut params = json!({"status": upstream.0, "since_ms": now_ms()});
            if let Some(reason) = upstream.1 {
                params["reason"] = reason.into();
            }
            return self.out(notification("push.status", params)).await;
        }
        Flow::Go
    }

    /// `changes.subscribe` (§5.1). The gateway connection is made beside the socket's loop, so
    /// that pings are answered meanwhile; the answer follows in `on_opened`.
    async fn subscribe(&mut self, id: &Value, params: &Value) -> Flow {
        if !bridge::features(&self.bridge.link.state()).contains(&"changes") {
            let e = RpcError::new(Reason::Unsupported, "the gateway or its credential does not offer the change feed");
            return self.reply(id, Err(e)).await;
        }
        let from = match params.get("from") {
            Some(Value::String(now)) if now == "now" => Some(feed::From::Now),
            Some(Value::Object(from)) => match from.get("cursor").and_then(Value::as_str) {
                Some(cursor) if !cursor.is_empty() && cursor.len() <= 1024 => Some(feed::From::Cursor(cursor.to_string())),
                _ => None,
            },
            _ => None,
        };
        let Some(from) = from else {
            return self.reply(id, Err(RpcError::invalid("`from` is \"now\" or {\"cursor\": …}"))).await;
        };
        // one subscription per socket: a new one replaces the old
        self.sub = None;
        if let Some(earlier) = self.opening.take() {
            let e = RpcError::invalid("replaced by a later changes.subscribe");
            if let Flow::Gone = self.reply(&earlier.id, Err(e)).await {
                return Flow::Gone;
            }
        }
        self.subs += 1;
        let name = format!("s{}", self.subs);
        let (tx, result) = oneshot::channel();
        let bridge = self.bridge.clone();
        let subscription = name.clone();
        tokio::spawn(async move {
            // a result nobody waits for any more drops the feed, which ends it
            let _ = tx.send(feed::open(&bridge, from, subscription).await);
        });
        self.opening = Some(Opening { id: id.clone(), name, result });
        Flow::Go
    }

    async fn on_opened(&mut self, result: Option<Rpc<(Feed, Started)>>) -> Flow {
        let Some(Opening { id, name, .. }) = self.opening.take() else { return Flow::Go };
        match result {
            Some(Ok((feed, started))) => {
                let answer = json!({
                    "subscription": name,
                    "watermark_ms": started.watermark_ms,
                    "resumed": started.resumed,
                    "next_seq": started.after + 1,
                });
                self.sub = Some(Sub {
                    name,
                    feed,
                    outstanding: VecDeque::new(),
                    highest_sent: started.after,
                    redeliver_at: Instant::now(),
                });
                self.reply(&id, Ok(answer)).await
            }
            Some(Err(e)) => self.reply(&id, Err(e)).await,
            None => self.reply(&id, Err(RpcError::new(Reason::Internal, "the subscription could not be opened"))).await,
        }
    }

    /// `changes.ack` (§5.4): cumulative, and a no-op for what is acked already.
    fn ack(&mut self, params: &Value) -> Rpc {
        let name = params.get("subscription").and_then(Value::as_str);
        let seq = params.get("seq").and_then(Value::as_u64);
        let (Some(name), Some(seq)) = (name, seq) else {
            return Err(RpcError::invalid("`subscription` and `seq` are needed"));
        };
        // an ack that was on its way when its subscription was replaced
        let Some(sub) = self.sub.as_mut().filter(|s| s.name == name) else { return Ok(json!({})) };
        if seq > sub.highest_sent {
            return Err(RpcError::invalid("`seq` is above the highest batch sent"));
        }
        let before = sub.outstanding.len();
        while sub.outstanding.front().is_some_and(|b| b.seq <= seq) {
            sub.outstanding.pop_front();
        }
        if sub.outstanding.len() != before {
            sub.redeliver_at = Instant::now() + self.bridge.settings.timing.ack_timeout;
            sub.feed.ack(seq);
        }
        Ok(json!({}))
    }

    async fn on_event(&mut self, event: Option<Event>) -> Flow {
        let Some(Event::Batch(batch)) = event else {
            // the Hub's next subscribe with its cursor gets `unknown_cursor` and repairs by time
            return Flow::Close(CLOSE_RESTART, "the change feed has to start over");
        };
        let Some(sub) = self.sub.as_mut() else { return Flow::Go };
        if sub.outstanding.is_empty() {
            sub.redeliver_at = Instant::now() + self.bridge.settings.timing.ack_timeout;
        }
        sub.highest_sent = batch.seq;
        let frame = batch.frame.clone();
        sub.outstanding.push_back(batch);
        self.out(frame).await
    }

    /// Sends every unacked batch again, unchanged and in order.
    async fn redeliver(&mut self) -> Flow {
        let Some(sub) = self.sub.as_mut() else { return Flow::Go };
        sub.redeliver_at = Instant::now() + self.bridge.settings.timing.ack_timeout;
        let frames: Vec<String> = sub.outstanding.iter().map(|b| b.frame.clone()).collect();
        for frame in frames {
            if let Flow::Gone = self.out(frame).await {
                return Flow::Gone;
            }
        }
        Flow::Go
    }

    /// Every other request runs beside the socket's loop, at most `max_inflight_requests` at once.
    async fn request(&mut self, id: &Value, method: &str, params: Value) -> Flow {
        let Ok(permit) = self.inflight.clone().try_acquire_owned() else {
            let e = RpcError::new(Reason::RateLimited, "more open requests than max_inflight_requests")
                .with("retry_after_ms", 200);
            return self.reply(id, Err(e)).await;
        };
        let (bridge, replies, id, method) = (self.bridge.clone(), self.replies.clone(), id.clone(), method.to_string());
        tokio::spawn(async move {
            let result = bridge.dispatch(&method, &params).await;
            let mut frame = response(&id, &result);
            if frame.len() > MAX_FRAME_BYTES {
                let e = RpcError::new(Reason::TooLarge, "the answer does not fit a frame; ask for less at once");
                frame = response(&id, &Err(e));
            }
            let _ = replies.send(frame).await;
            drop(permit);
        });
        Flow::Go
    }
}
