//! What the end-to-end tests run against: an in-process fake of the gateway (the part of
//! openlark-protocol v0 the bridge calls), the bridge itself on a loopback port, and a small
//! client that speaks the external channel protocol the way the Hub does.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ws::{CloseFrame, Message as AxMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use steve_plugin_openlark::config::{Settings, Timing, endpoints};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub const GW_TOKEN: &str = "olk_test_credential_0001";
pub const HUB_TOKEN: &str = "hub-token-0123456789";
pub const ME: &str = "100";
pub const GRACE: &str = "9";
pub const TENANT: &str = "t1";
/// The group of the default world, by its gateway id and by its shared id.
pub const GROUP: &str = "7";
pub const GROUP_OC: &str = "oc_7design";
/// The direct chat of the default world; the gateway has no shared id for it.
pub const DM: &str = "8";

pub fn now_ms() -> i64 {
    let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    i64::try_from(since.as_millis()).unwrap()
}

// ---------------------------------------------------------------------------------------------
// The fake gateway
// ---------------------------------------------------------------------------------------------

/// A gateway error: its `data.code` and, for `rate_limited`, the wait.
pub type Fail = (String, Option<u64>);

fn fail(code: &str) -> Fail {
    (code.to_string(), None)
}

pub struct Media {
    pub status: u16,
    pub content_type: Option<&'static str>,
    pub body: Vec<u8>,
    /// Sent without a length.
    pub chunked: bool,
    pub location: Option<&'static str>,
}

impl Media {
    pub fn file(content_type: &'static str, body: &[u8]) -> Media {
        Media { status: 200, content_type: Some(content_type), body: body.to_vec(), chunked: false, location: None }
    }

    pub fn status(status: u16) -> Media {
        Media { status, content_type: None, body: Vec::new(), chunked: false, location: None }
    }
}

/// Everything the fake gateway knows; tests script it directly.
pub struct World {
    pub token: String,
    pub revoked: bool,
    /// `ok`, `expired` or `locked`.
    pub session: String,
    /// Whether hello names the signed-in user.
    pub hello_user: bool,
    pub me: String,
    pub tenant: String,
    pub scopes: Vec<String>,
    pub features: Vec<String>,
    pub push_connected: bool,
    pub users: BTreeMap<String, Value>,
    pub chats: BTreeMap<String, Value>,
    /// How many more `chats.get` answers leave a chat's shared id out.
    pub oc_missing: HashMap<String, usize>,
    /// Every message, in the order of creation.
    pub messages: Vec<Value>,
    next_id: u64,
    /// The changes still kept, oldest first.
    pub journal: Vec<Value>,
    pub head: u64,
    /// How many changes one `changes.batch` carries at most.
    pub batch_max: usize,
    pub acks: Vec<u64>,
    idem: HashMap<String, (String, Value)>,
    /// Chats the write policy refuses.
    pub forbidden_chats: HashSet<String>,
    pub reactions: Vec<(String, String)>,
    /// Methods that fail, and how.
    pub fail: HashMap<String, Fail>,
    /// Every request received, hello included.
    pub calls: Vec<(String, Value)>,
    pub media: HashMap<String, Media>,
}

impl World {
    fn new() -> World {
        let users = BTreeMap::from([
            (
                ME.to_string(),
                json!({"id": ME, "name": "Ada", "email": "Ada@Example.com", "union_id": "on_ada", "tenant_id": TENANT}),
            ),
            (GRACE.to_string(), json!({"id": GRACE, "name": "Grace", "email": "grace@example.com", "tenant_id": TENANT})),
            ("50".to_string(), json!({"id": "50", "name": "Helper", "kind": "bot", "tenant_id": TENANT})),
        ]);
        let chats = BTreeMap::from([
            (GROUP.to_string(), json!({"id": GROUP, "oc": GROUP_OC, "kind": "group", "name": "Design", "external": false})),
            (DM.to_string(), json!({"id": DM, "kind": "p2p", "name": "Grace"})),
        ]);
        let all = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        World {
            token: GW_TOKEN.to_string(),
            revoked: false,
            session: "ok".into(),
            hello_user: true,
            me: ME.into(),
            tenant: TENANT.into(),
            scopes: all(&["chats.read", "messages.read", "messages.send", "messages.recall", "reactions.write", "people.read"]),
            features: all(&["changes.v1", "im.v1", "recall.v1", "media.v1", "org.v1", "push_status.v1"]),
            push_connected: true,
            users,
            chats,
            oc_missing: HashMap::new(),
            messages: Vec::new(),
            next_id: 500,
            journal: Vec::new(),
            head: 0,
            batch_max: 100,
            acks: Vec::new(),
            idem: HashMap::new(),
            forbidden_chats: HashSet::new(),
            reactions: Vec::new(),
            fail: HashMap::new(),
            calls: Vec::new(),
            media: HashMap::new(),
        }
    }

    /// The sender as a message carries it: the gateway gives only the id, name and kind there.
    fn sender(&self, id: &str) -> Value {
        let mut s = json!({"id": id});
        if let Some(u) = self.users.get(id) {
            for key in ["name", "kind"] {
                if let Some(v) = u.get(key) {
                    s[key] = v.clone();
                }
            }
        }
        s
    }

    /// Stores a text message without telling the journal.
    pub fn store(&mut self, chat: &str, sender: &str, text: &str, created_ms: i64) -> Value {
        self.next_id += 1;
        let m = json!({"id": self.next_id.to_string(), "chat": chat, "sender": self.sender(sender),
            "created_ms": created_ms, "content": {"type": "text", "text": text}, "mentions": []});
        self.messages.push(m.clone());
        m
    }

    pub fn message(&self, id: &str) -> Option<&Value> {
        self.messages.iter().find(|m| m["id"] == id)
    }

    fn earliest(&self) -> u64 {
        self.journal.first().and_then(|c| c["seq"].as_u64()).unwrap_or(0)
    }

    /// Discards every change up to and including `seq`.
    pub fn prune_through(&mut self, seq: u64) {
        self.journal.retain(|c| c["seq"].as_u64().unwrap() > seq);
    }

    pub fn called(&self, method: &str) -> Vec<Value> {
        self.calls.iter().filter(|(m, _)| m == method).map(|(_, p)| p.clone()).collect()
    }
}

pub struct Fake {
    world: Mutex<World>,
    head: watch::Sender<u64>,
    kick: watch::Sender<u64>,
}

impl Fake {
    pub fn w(&self) -> MutexGuard<'_, World> {
        self.world.lock().unwrap()
    }

    /// Appends a change to the journal and tells every subscriber. Returns its position.
    pub fn append(&self, mut change: Value) -> u64 {
        let seq = {
            let mut w = self.w();
            w.head += 1;
            change["seq"] = w.head.into();
            let seq = w.head;
            w.journal.push(change);
            seq
        };
        self.head.send_replace(seq);
        seq
    }

    /// A message somebody posts now: stored and journaled. Returns the message.
    pub fn post(&self, chat: &str, sender: &str, text: &str) -> Value {
        self.post_at(chat, sender, text, now_ms())
    }

    pub fn post_at(&self, chat: &str, sender: &str, text: &str, created_ms: i64) -> Value {
        let (m, mine) = {
            let mut w = self.w();
            let m = w.store(chat, sender, text, created_ms);
            (m, sender == w.me)
        };
        self.append(json!({"kind": "message", "op": "upsert", "message": m, "mine": mine, "at_ms": created_ms}));
        m
    }

    /// Closes every open agent connection.
    pub fn kick(&self) {
        self.kick.send_modify(|n| *n += 1);
    }

    /// Withdraws the credential: open connections end with 4401 and new ones get 401.
    pub fn revoke(&self) {
        self.w().revoked = true;
        self.kick();
    }

    fn handle(&self, conn: &mut ConnState, method: &str, p: &Value) -> Result<Value, Fail> {
        let mut w = self.w();
        w.calls.push((method.to_string(), p.clone()));
        if let Some(f) = w.fail.get(method) {
            return Err(f.clone());
        }
        let invalid = || fail("invalid");
        // the real gateway refuses a field it does not know
        let keys = |allowed: &[&str]| match p.as_object() {
            Some(o) if o.keys().all(|k| allowed.contains(&k.as_str())) => Ok(()),
            _ => Err(invalid()),
        };
        let text = |key: &str| p.get(key).and_then(Value::as_str).map(str::to_string).ok_or_else(invalid);
        if method == "hello" {
            keys(&["protocol", "client", "capabilities"])?;
            if p["protocol"] != "openlark/0" {
                return Err(invalid());
            }
            conn.hello = true;
            conn.changes = p["capabilities"]["changes"] == true;
            let mut account = json!({"tenant": {"id": w.tenant}, "session": w.session});
            if w.hello_user {
                account["user"] = w.sender(&w.me);
            }
            return Ok(json!({"protocol": "openlark/0", "gateway": {"name": "fake-gateway", "version": "0"},
                "account": account, "scopes": w.scopes, "features": w.features, "limits": {}}));
        }
        if !conn.hello {
            return Err(invalid());
        }
        let need = |w: &World, scope: &str| if w.scopes.iter().any(|s| s == scope) { Ok(()) } else { Err(fail("scope")) };
        match method {
            "push.status" => {
                keys(&[])?;
                need(&w, "messages.read")?;
                return Ok(json!({"connected": w.push_connected}));
            }
            "changes.subscribe" => {
                keys(&["after", "from_head", "kinds"])?;
                need(&w, "messages.read")?;
                if !conn.changes {
                    return Err(invalid());
                }
                let after = if p["from_head"] == true { w.head } else { p["after"].as_u64().unwrap_or(0) };
                if after > w.head {
                    return Err(invalid());
                }
                conn.sent = Some(after);
                return Ok(json!({"after": after, "head": w.head, "earliest": w.earliest()}));
            }
            "changes.ack" => {
                keys(&["seq"])?;
                w.acks.push(p["seq"].as_u64().ok_or_else(invalid)?);
                return Ok(json!({}));
            }
            _ => {}
        }
        // everything else needs the Lark session
        if w.session != "ok" {
            return Err(fail("session"));
        }
        match method {
            "me.get" => {
                keys(&[])?;
                Ok(json!({"user": w.users[&w.me], "tenant": {"id": w.tenant}}))
            }
            "chats.get" => {
                keys(&["chat"])?;
                need(&w, "chats.read")?;
                let id = text("chat")?;
                let mut chat = w.chats.get(&id).cloned().ok_or_else(|| fail("not_found"))?;
                if let Some(left) = w.oc_missing.get_mut(&id).filter(|left| **left > 0) {
                    *left -= 1;
                    chat.as_object_mut().unwrap().remove("oc");
                }
                Ok(chat)
            }
            "chats.resolve" => {
                keys(&["oc"])?;
                need(&w, "chats.read")?;
                let oc = text("oc")?;
                w.chats.values().find(|c| c["oc"] == oc.as_str()).cloned().ok_or_else(|| fail("not_found"))
            }
            "messages.get" => {
                keys(&["message"])?;
                need(&w, "messages.read")?;
                w.message(&text("message")?).cloned().ok_or_else(|| fail("not_found"))
            }
            "messages.history" => {
                keys(&["chat", "before", "after", "limit"])?;
                need(&w, "messages.read")?;
                let chat = text("chat")?;
                if !w.chats.contains_key(&chat) {
                    return Err(fail("not_found"));
                }
                let all: Vec<&Value> = w.messages.iter().filter(|m| m["chat"] == chat.as_str()).collect();
                let limit = usize::try_from(p["limit"].as_u64().unwrap_or(50)).unwrap().clamp(1, 100);
                // a cursor is a position in the chat: `p<n>`
                let position = |key: &str| match p.get(key) {
                    None | Some(Value::Null) => Ok(None),
                    Some(c) => {
                        let n = c.as_str().and_then(|c| c.strip_prefix('p')).and_then(|n| n.parse::<usize>().ok());
                        n.filter(|n| *n <= all.len()).map(Some).ok_or_else(invalid)
                    }
                };
                let (start, end) = match (position("before")?, position("after")?) {
                    (Some(_), Some(_)) => return Err(invalid()),
                    (Some(before), None) => (before.saturating_sub(limit), before),
                    (None, Some(after)) => (after, (after + limit).min(all.len())),
                    (None, None) => (all.len().saturating_sub(limit), all.len()),
                };
                let mut page = json!({"items": all[start..end]});
                if start > 0 {
                    page["prev"] = format!("p{start}").into();
                }
                // the newest page has a `next` too: new messages may follow it
                if end > start {
                    page["next"] = format!("p{end}").into();
                }
                Ok(page)
            }
            "messages.send" | "messages.reply" => {
                let reply = method == "messages.reply";
                if reply {
                    keys(&["message", "in_thread", "content", "mentions", "idempotency_key", "expected_sender"])?;
                } else {
                    keys(&["chat", "content", "mentions", "idempotency_key", "expected_sender"])?;
                }
                need(&w, "messages.send")?;
                if p.get("expected_sender").and_then(Value::as_str).is_some_and(|e| e != w.me) {
                    return Err(fail("sender_changed"));
                }
                let key = text("idempotency_key")?;
                if key.is_empty() || key.len() > 64 || p["content"]["type"] != "text" {
                    return Err(invalid());
                }
                let mut what = p.clone();
                what.as_object_mut().unwrap().remove("idempotency_key");
                let what = what.to_string();
                match w.idem.get(&key) {
                    Some((first, m)) if *first == what => return Ok(m.clone()),
                    Some(_) => return Err(fail("conflict")),
                    None => {}
                }
                let parent = if reply {
                    Some(w.message(&text("message")?).cloned().ok_or_else(|| fail("not_found"))?)
                } else {
                    None
                };
                let chat = match &parent {
                    Some(parent) => parent["chat"].as_str().unwrap().to_string(),
                    None => text("chat")?,
                };
                if !w.chats.contains_key(&chat) {
                    return Err(fail("not_found"));
                }
                if w.forbidden_chats.contains(&chat) {
                    return Err(fail("forbidden"));
                }
                let me = w.me.clone();
                let mut m = w.store(&chat, &me, p["content"]["text"].as_str().unwrap_or(""), now_ms());
                if let Some(parent) = &parent {
                    m["parent"] = parent["id"].clone();
                    if p["in_thread"] == true {
                        m["thread"] = parent.get("thread").filter(|t| !t.is_null()).unwrap_or(&parent["id"]).clone();
                    }
                }
                if let Some(mentions) = p.get("mentions") {
                    m["mentions"] = mentions.clone();
                }
                *w.messages.last_mut().unwrap() = m.clone();
                w.idem.insert(key, (what, m.clone()));
                w.head += 1;
                let seq = w.head;
                let created = m["created_ms"].clone();
                w.journal.push(json!({"seq": seq, "kind": "message", "op": "upsert", "message": m, "mine": true, "at_ms": created}));
                drop(w);
                self.head.send_replace(seq);
                Ok(m)
            }
            "messages.recall" => {
                keys(&["message"])?;
                need(&w, "messages.recall")?;
                let id = text("message")?;
                let m = w.messages.iter_mut().find(|m| m["id"] == id.as_str()).ok_or_else(|| fail("not_found"))?;
                m["recalled"] = true.into();
                Ok(json!({}))
            }
            "reactions.add" | "reactions.remove" => {
                keys(&["message", "emoji"])?;
                need(&w, "reactions.write")?;
                let (message, emoji) = (text("message")?, text("emoji")?);
                if w.message(&message).is_none() {
                    return Err(fail("not_found"));
                }
                if method == "reactions.add" {
                    w.reactions.push((message, emoji));
                } else {
                    w.reactions.retain(|r| *r != (message.clone(), emoji.clone()));
                }
                Ok(json!({}))
            }
            "people.batch" => {
                keys(&["users"])?;
                need(&w, "people.read")?;
                let ids: Vec<&str> = p["users"].as_array().ok_or_else(invalid)?.iter().filter_map(Value::as_str).collect();
                let people: Vec<Value> = ids
                    .iter()
                    .filter_map(|id| w.users.get(*id))
                    .map(|u| {
                        // other people's open ids are not known to the gateway
                        let mut person = u.clone();
                        person.as_object_mut().unwrap().remove("union_id");
                        person["avatar_url"] = format!("https://cdn.example.com/{}.png", u["id"].as_str().unwrap()).into();
                        person
                    })
                    .collect();
                let missing: Vec<&str> = ids.iter().copied().filter(|id| !w.users.contains_key(*id)).collect();
                Ok(json!({"people": people, "missing": missing}))
            }
            _ => Err(fail("unsupported")),
        }
    }

    /// The `changes.batch` frames that bring a subscriber from `sent` to the head.
    fn pending(&self, sent: &mut u64) -> Vec<String> {
        let w = self.w();
        let mut items: Vec<Value> = Vec::new();
        let first_kept = if w.journal.is_empty() { w.head + 1 } else { w.earliest() };
        if *sent + 1 < first_kept {
            // what was discarded is one gap, at the position of its last change
            items.push(json!({"seq": first_kept - 1, "kind": "gap", "from": *sent + 1, "to": first_kept - 1, "chats": []}));
        }
        items.extend(w.journal.iter().filter(|c| c["seq"].as_u64().unwrap() > *sent).cloned());
        *sent = w.head;
        let frame = |chunk: &[Value]| json!({"jsonrpc": "2.0", "method": "changes.batch", "params": {"items": chunk}}).to_string();
        items.chunks(w.batch_max.max(1)).map(frame).collect()
    }
}

#[derive(Default)]
struct ConnState {
    hello: bool,
    changes: bool,
    /// The last position sent, once subscribed.
    sent: Option<u64>,
}

fn bearer_ok(headers: &HeaderMap, fake: &Fake) -> bool {
    let w = fake.w();
    let given = headers.get("authorization").and_then(|v| v.to_str().ok());
    !w.revoked && given == Some(format!("Bearer {}", w.token).as_str())
}

async fn agent(State(fake): State<Arc<Fake>>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    if !bearer_ok(&headers, &fake) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    ws.on_upgrade(move |socket| agent_conn(socket, fake))
}

async fn agent_conn(mut socket: WebSocket, fake: Arc<Fake>) {
    let mut head = fake.head.subscribe();
    let mut kick = fake.kick.subscribe();
    let mut conn = ConnState::default();
    loop {
        tokio::select! {
            _ = kick.changed() => {
                let code = if fake.w().revoked { 4401 } else { 1001 };
                let _ = socket.send(AxMessage::Close(Some(CloseFrame { code, reason: "".into() }))).await;
                return;
            }
            _ = head.changed(), if conn.sent.is_some() => {}
            frame = socket.recv() => {
                let text = match frame {
                    Some(Ok(AxMessage::Text(text))) => text,
                    Some(Ok(AxMessage::Ping(_) | AxMessage::Pong(_))) => continue,
                    _ => return,
                };
                let v: Value = serde_json::from_str(text.as_str()).expect("the bridge sends JSON");
                let method = v["method"].as_str().expect("the bridge sends requests").to_string();
                let answer = match fake.handle(&mut conn, &method, &v["params"]) {
                    Ok(result) => json!({"jsonrpc": "2.0", "id": v["id"], "result": result}),
                    Err((code, retry_after_ms)) => {
                        let mut data = json!({"code": code});
                        if let Some(ms) = retry_after_ms {
                            data["retry_after_ms"] = ms.into();
                        }
                        json!({"jsonrpc": "2.0", "id": v["id"], "error": {"code": -32000, "message": "refused", "data": data}})
                    }
                };
                if socket.send(AxMessage::Text(answer.to_string().into())).await.is_err() {
                    return;
                }
            }
        }
        if let Some(sent) = conn.sent.as_mut() {
            for frame in fake.pending(sent) {
                if socket.send(AxMessage::Text(frame.into())).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn media(State(fake): State<Arc<Fake>>, Path(key): Path<String>, headers: HeaderMap) -> Response {
    if !bearer_ok(&headers, &fake) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let w = fake.w();
    let Some(m) = w.media.get(&key) else { return StatusCode::NOT_FOUND.into_response() };
    let mut response = Response::builder().status(m.status);
    if let Some(content_type) = m.content_type {
        response = response.header("content-type", content_type);
    }
    if let Some(location) = m.location {
        response = response.header("location", location);
    }
    let body = if m.chunked {
        let chunks: Vec<Result<Vec<u8>, std::io::Error>> = m.body.chunks(7).map(|c| Ok(c.to_vec())).collect();
        Body::from_stream(futures::stream::iter(chunks))
    } else {
        Body::from(m.body.clone())
    };
    response.body(body).unwrap()
}

/// The fake gateway on a loopback port. It can be stopped and started again on the same port.
pub struct Gateway {
    pub fake: Arc<Fake>,
    pub addr: SocketAddr,
    server: Option<JoinHandle<()>>,
}

impl Gateway {
    pub async fn start(edit: impl FnOnce(&mut World)) -> Gateway {
        let mut world = World::new();
        edit(&mut world);
        let fake = Arc::new(Fake { world: Mutex::new(world), head: watch::channel(0).0, kick: watch::channel(0).0 });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut gateway = Gateway { fake, addr, server: None };
        gateway.serve(listener);
        gateway
    }

    fn serve(&mut self, listener: TcpListener) {
        let app = Router::new()
            .route("/v0/agent", get(agent))
            .route("/v0/media/{key}", get(media))
            .with_state(self.fake.clone());
        self.server = Some(tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        }));
    }

    /// Takes the gateway away: nothing listens and every connection ends.
    pub async fn stop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
            let _ = server.await;
        }
        self.fake.kick();
    }

    /// Brings it back on the same port, with the same world.
    pub async fn restart(&mut self) {
        let listener = loop {
            match TcpListener::bind(self.addr).await {
                Ok(listener) => break listener,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        self.serve(listener);
    }
}

// ---------------------------------------------------------------------------------------------
// The bridge under test
// ---------------------------------------------------------------------------------------------

/// The protocol's waits, shortened so that a test of a timeout takes a moment.
pub fn fast() -> Timing {
    Timing {
        hello_deadline: Duration::from_millis(400),
        idle: Duration::from_secs(5),
        ack_timeout: Duration::from_millis(300),
        health_tick: Duration::from_millis(100),
        call_timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(50),
        reconnect_max: Duration::from_millis(200),
        hello_wait: Duration::from_millis(600),
        gateway_idle: Duration::from_secs(5),
        oc_retry: Duration::from_millis(20),
    }
}

/// A fake gateway and a bridge connected to it.
pub struct Rig {
    pub gw: Gateway,
    pub addr: SocketAddr,
    pub data: tempfile::TempDir,
    pub timing: Timing,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl Rig {
    pub async fn start() -> Rig {
        Rig::with(|_| {}).await
    }

    /// A rig whose gateway world was edited before the bridge first connects.
    pub async fn with(edit: impl FnOnce(&mut World)) -> Rig {
        Rig::custom(edit, fast()).await
    }

    pub async fn custom(edit: impl FnOnce(&mut World), timing: Timing) -> Rig {
        let mut rig = Rig::parked(edit, timing).await;
        rig.start_bridge().await;
        rig
    }

    /// A rig whose bridge is not started yet.
    pub async fn parked(edit: impl FnOnce(&mut World), timing: Timing) -> Rig {
        let gw = Gateway::start(edit).await;
        let data = tempfile::tempdir().unwrap();
        Rig { gw, addr: "127.0.0.1:0".parse().unwrap(), data, timing, stop: None, task: None }
    }

    /// Starts the bridge: the first time on a free port, later on the same one, always on the
    /// same data directory.
    pub async fn start_bridge(&mut self) {
        let listener = loop {
            match TcpListener::bind(self.addr).await {
                Ok(listener) => break listener,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        self.addr = listener.local_addr().unwrap();
        let settings = Settings {
            token: HUB_TOKEN.to_string(),
            gateway: endpoints(&format!("ws://{}", self.gw.addr)).unwrap(),
            gateway_token: GW_TOKEN.to_string(),
            data: self.data.path().to_path_buf(),
            timing: self.timing.clone(),
        };
        let (stop, stopped) = oneshot::channel::<()>();
        self.stop = Some(stop);
        self.task = Some(tokio::spawn(steve_plugin_openlark::run(listener, settings, async move {
            let _ = stopped.await;
        })));
    }

    pub async fn stop_bridge(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.unwrap().unwrap();
        }
    }

    pub fn fake(&self) -> &Fake {
        &self.gw.fake
    }

    /// A Hub connection that has said hello.
    pub async fn hub(&self) -> Hub {
        let mut hub = Hub::connect(self.addr, "/ws", Some(HUB_TOKEN)).await.expect("the upgrade is accepted");
        hub.hello().await.expect("hello is answered");
        hub
    }

    pub fn media_url(&self, key: &str) -> String {
        format!("http://{}/v1/media/{key}", self.addr)
    }
}

// ---------------------------------------------------------------------------------------------
// The Hub's side
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum Frame {
    Json(Value),
    /// The socket ended, with the close code when one was sent.
    Closed(Option<u16>),
}

/// A client of the external channel protocol, as the Hub is one.
pub struct Hub {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_id: i64,
    /// Notifications that arrived while something else was awaited.
    stash: VecDeque<Value>,
}

/// The reason of an error answer.
pub fn reason(error: &Value) -> &str {
    error["data"]["reason"].as_str().expect("an error carries data.reason")
}

pub fn id_handle(id: &str) -> Value {
    json!({"kind": "id", "value": id})
}

impl Hub {
    /// Upgrades, or returns the HTTP status that refused it.
    pub async fn connect(addr: SocketAddr, path: &str, token: Option<&str>) -> Result<Hub, u16> {
        let mut request = format!("ws://{addr}{path}").into_client_request().unwrap();
        if let Some(token) = token {
            request.headers_mut().insert("Authorization", format!("Bearer {token}").parse().unwrap());
        }
        match tokio_tungstenite::connect_async(request).await {
            Ok((ws, _)) => Ok(Hub { ws, next_id: 0, stash: VecDeque::new() }),
            Err(tungstenite::Error::Http(response)) => Err(response.status().as_u16()),
            Err(e) => panic!("the bridge is not reachable: {e}"),
        }
    }

    pub async fn send_text(&mut self, text: &str) {
        self.ws.send(Message::Text(text.into())).await.unwrap();
    }

    pub async fn send_binary(&mut self, bytes: &[u8]) {
        self.ws.send(Message::Binary(bytes.to_vec().into())).await.unwrap();
    }

    async fn frame_within(&mut self, wait: Duration) -> Option<Frame> {
        loop {
            let next = tokio::time::timeout(wait, self.ws.next()).await.ok()?;
            match next {
                None | Some(Err(_)) => return Some(Frame::Closed(None)),
                Some(Ok(Message::Text(text))) => return Some(Frame::Json(serde_json::from_str(text.as_str()).unwrap())),
                Some(Ok(Message::Close(frame))) => return Some(Frame::Closed(frame.map(|f| u16::from(f.code)))),
                Some(Ok(_)) => {}
            }
        }
    }

    /// The next frame, whatever it is.
    pub async fn frame(&mut self) -> Frame {
        self.frame_within(Duration::from_secs(5)).await.expect("a frame within 5 s")
    }

    /// Sends a request and returns its id.
    pub async fn ask(&mut self, method: &str, params: Value) -> i64 {
        self.next_id += 1;
        let frame = json!({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params});
        self.send_text(&frame.to_string()).await;
        self.next_id
    }

    /// The answer to a request: its result, or its error object.
    pub async fn answer(&mut self, id: i64) -> Result<Value, Value> {
        loop {
            match self.frame().await {
                Frame::Json(v) if v.get("id") == Some(&json!(id)) => {
                    assert_eq!(v["jsonrpc"], "2.0");
                    return match v.get("error") {
                        Some(e) => Err(e.clone()),
                        None => Ok(v["result"].clone()),
                    };
                }
                Frame::Json(v) => self.stash.push_back(v),
                Frame::Closed(code) => panic!("the socket closed ({code:?}) before request {id} was answered"),
            }
        }
    }

    pub async fn call(&mut self, method: &str, params: Value) -> Result<Value, Value> {
        let id = self.ask(method, params).await;
        self.answer(id).await
    }

    pub async fn hello(&mut self) -> Result<Value, Value> {
        self.call(
            "hello",
            json!({"protocol": {"min": 1, "max": 1}, "client": {"name": "steve-hub", "version": "0.0.0"},
                "time_ms": now_ms(), "capabilities": ["changes", "history", "media", "send", "a-later-thing"],
                "a_later_field": true}),
        )
        .await
    }

    /// The params of the next notification of this method that `want` accepts. Others are kept.
    pub async fn note_where(&mut self, method: &str, want: impl Fn(&Value) -> bool) -> Value {
        if let Some(i) = self.stash.iter().position(|v| v["method"] == method && want(&v["params"])) {
            return self.stash.remove(i).unwrap()["params"].clone();
        }
        loop {
            match self.frame().await {
                Frame::Json(v) if v["method"] == method && want(&v["params"]) => return v["params"].clone(),
                Frame::Json(v) => self.stash.push_back(v),
                Frame::Closed(code) => panic!("the socket closed ({code:?}) while waiting for {method}"),
            }
        }
    }

    pub async fn note(&mut self, method: &str) -> Value {
        self.note_where(method, |_| true).await
    }

    /// The next batch of the feed.
    pub async fn batch(&mut self) -> Value {
        self.note("changes.batch").await
    }

    /// Batches until one carries an item, acking each. Returns that batch.
    pub async fn batch_with_items(&mut self) -> Value {
        loop {
            let batch = self.batch().await;
            self.ack(&batch).await.unwrap();
            if !batch["items"].as_array().unwrap().is_empty() {
                return batch;
            }
        }
    }

    /// The next `n` items of the feed, across batches, acking each batch.
    pub async fn items(&mut self, n: usize) -> Vec<Value> {
        let mut items = Vec::new();
        while items.len() < n {
            let batch = self.batch_with_items().await;
            items.extend(batch["items"].as_array().unwrap().iter().cloned());
        }
        assert_eq!(items.len(), n, "more items than expected: {items:?}");
        items
    }

    /// Every batch that arrives within `wait`, unacked.
    pub async fn batches_for(&mut self, wait: Duration) -> Vec<Value> {
        let until = tokio::time::Instant::now() + wait;
        let mut batches = Vec::new();
        loop {
            let at = self.stash.iter().position(|v| v["method"] == "changes.batch");
            if let Some(i) = at {
                batches.push(self.stash.remove(i).unwrap()["params"].clone());
                continue;
            }
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            match self.frame_within(left).await {
                None => return batches,
                Some(Frame::Json(v)) => self.stash.push_back(v),
                Some(Frame::Closed(code)) => panic!("the socket closed ({code:?})"),
            }
        }
    }

    pub async fn ack(&mut self, batch: &Value) -> Result<Value, Value> {
        self.call("changes.ack", json!({"subscription": batch["subscription"], "seq": batch["seq"]})).await
    }

    pub async fn subscribe_now(&mut self) -> Value {
        self.call("changes.subscribe", json!({"from": "now"})).await.expect("the subscription is accepted")
    }

    /// Whether a notification of this method arrives within `wait`. One that does stays kept.
    pub async fn gets(&mut self, method: &str, wait: Duration) -> bool {
        if self.stash.iter().any(|v| v["method"] == method) {
            return true;
        }
        let until = tokio::time::Instant::now() + wait;
        loop {
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            match self.frame_within(left).await {
                None => return false,
                Some(Frame::Json(v)) => {
                    let hit = v["method"] == method;
                    self.stash.push_back(v);
                    if hit {
                        return true;
                    }
                }
                Some(Frame::Closed(code)) => panic!("the socket closed ({code:?})"),
            }
        }
    }

    /// Reads until the socket ends. Returns the close code and every frame before it.
    pub async fn until_closed(&mut self) -> (Option<u16>, Vec<Value>) {
        let mut before: Vec<Value> = self.stash.drain(..).collect();
        loop {
            match self.frame().await {
                Frame::Json(v) => before.push(v),
                Frame::Closed(code) => return (code, before),
            }
        }
    }
}
