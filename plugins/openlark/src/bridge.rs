//! What every connection of the Hub shares: the gateway link, the chats seen so far and the
//! record of writes.

use std::collections::{BTreeMap, HashMap};
use std::future::{Future, IntoFuture};
use std::path::PathBuf;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::time::Instant;

use crate::config::Settings;
use crate::ext::proto::{Reason, Rpc, RpcError};
use crate::gateway::client::GwError;
use crate::gateway::link::{Identity, Link, LinkState, LinkStatus, Session};
use crate::gateway::wire::Chat;
use crate::map::{self, ChatInfo};
use crate::store;
use crate::writes::IdemStore;

/// How long a chat's name and kind are used before the gateway is asked again.
const CHAT_FRESH: std::time::Duration = std::time::Duration::from_secs(600);
/// The chats remembered at once.
const CHAT_CACHE: usize = 4096;
/// How often a group without a shared chat id is looked up again before its own id is used.
const OC_RETRIES: usize = 2;

struct Chats {
    by_internal: HashMap<String, (Arc<ChatInfo>, Instant)>,
    by_shared: HashMap<String, String>,
    /// The id every chat was first announced to the Hub under, by gateway id. A chat id never
    /// changes once announced: a group seen while its shared id was not known keeps the
    /// gateway's id, and one announced under its shared id keeps that through a later miss.
    announced: BTreeMap<String, String>,
}

pub struct Bridge {
    pub settings: Settings,
    pub link: Arc<Link>,
    pub idem: IdemStore,
    /// Writes go to the gateway one at a time, so that a retry meets the record of its first try.
    pub write_lock: Mutex<()>,
    pub http: reqwest::Client,
    chats: Mutex<Chats>,
    announced_path: PathBuf,
    /// Becomes true when the bridge shuts down.
    pub stop: watch::Receiver<bool>,
    /// Every open Hub connection holds a sender of this; the bridge waits for them on its way out.
    pub alive: mpsc::WeakSender<()>,
}

impl Bridge {
    fn new(settings: Settings, stop: watch::Receiver<bool>, alive: mpsc::WeakSender<()>) -> Arc<Bridge> {
        let link = Link::start(&settings, stop.clone());
        let announced_path = settings.data.join("chats.json");
        let announced: BTreeMap<String, String> = store::read_json(&announced_path).unwrap_or_default();
        let by_shared = announced.iter().filter(|(k, v)| k != v).map(|(k, v)| (v.clone(), k.clone())).collect();
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(10))
            .read_timeout(settings.timing.call_timeout)
            .build()
            .expect("the HTTP client has only static settings");
        Arc::new(Bridge {
            idem: IdemStore::load(settings.data.join("idempotency.json")),
            link,
            write_lock: Mutex::new(()),
            http,
            chats: Mutex::new(Chats { by_internal: HashMap::new(), by_shared, announced }),
            announced_path,
            stop,
            alive,
            settings,
        })
    }

    /// One gateway call, with its failure as the protocol's error.
    pub async fn gw<T: DeserializeOwned>(&self, method: &'static str, params: Value) -> Rpc<T> {
        match self.link.call(method, params).await {
            Ok(v) => Ok(v),
            Err(e) => Err(self.gw_err(e).await),
        }
    }

    /// A gateway failure as the protocol's error (§7). The texts are the bridge's own.
    pub async fn gw_err(&self, e: GwError) -> RpcError {
        let rpc = RpcError::new;
        match e {
            GwError::Down => rpc(Reason::UpstreamUnavailable, "the openlark gateway cannot be reached"),
            GwError::Unauthorized => {
                rpc(Reason::SessionExpired, "the openlark gateway refuses the plugin's credential")
                    .with("detail", "gateway_credential_refused")
            }
            GwError::Timeout => rpc(Reason::Timeout, "the openlark gateway did not answer in time"),
            GwError::Protocol(_) => rpc(Reason::Internal, "the openlark gateway gave an answer the plugin cannot read"),
            GwError::Rpc { code, retry_after_ms } => match code.as_str() {
                "session" => {
                    // only a fresh hello says whether the session is expired or locked
                    let _ = self.link.check().await;
                    match self.link.state().session {
                        Session::Locked => rpc(Reason::SessionLocked, "the Lark session is locked at the gateway"),
                        Session::Expired => rpc(Reason::SessionExpired, "the Lark session has expired at the gateway"),
                        Session::Ok => rpc(Reason::UpstreamUnavailable, "the Lark session was not usable for a moment"),
                    }
                }
                "scope" => rpc(Reason::Forbidden, "the gateway credential has no permission for this"),
                "not_found" => rpc(Reason::NotFound, "not found, or not visible to the signed-in user"),
                "rate_limited" => {
                    let e = rpc(Reason::RateLimited, "the gateway asks to slow down");
                    match retry_after_ms {
                        Some(ms) => e.with("retry_after_ms", ms),
                        None => e,
                    }
                }
                "conflict" => rpc(Reason::IdempotencyConflict, "the idempotency key was used for another message"),
                "sender_changed" => rpc(Reason::AccountChanged, "another user is signed in at the gateway"),
                "unsupported" => rpc(Reason::Unsupported, "the gateway does not support this"),
                "upstream" => rpc(Reason::UpstreamUnavailable, "Lark did not answer the gateway"),
                "invalid" => rpc(Reason::Invalid, "the gateway does not accept these values"),
                "forbidden" => rpc(Reason::TargetForbidden, "the gateway's write policy does not allow this target"),
                "unauthorized" => rpc(Reason::SessionExpired, "the openlark gateway refuses the plugin's credential")
                    .with("detail", "gateway_credential_refused"),
                _ => rpc(Reason::Internal, "the gateway failed"),
            },
        }
    }

    /// The signed-in user, or why nobody is known.
    pub fn identity(&self) -> Rpc<Identity> {
        let st = self.link.state();
        match st.identity {
            Some(identity) => Ok(identity),
            None if st.status == LinkStatus::Refused => {
                Err(RpcError::new(Reason::SessionExpired, "the openlark gateway refuses the plugin's credential")
                    .with("detail", "gateway_credential_refused"))
            }
            None => Err(RpcError::new(Reason::UpstreamUnavailable, "the openlark gateway cannot be reached")),
        }
    }

    /// The chat with this gateway id, from memory or from the gateway.
    pub async fn chat_by_internal(&self, id: &str) -> Result<Arc<ChatInfo>, GwError> {
        let stale = {
            let chats = self.chats.lock().await;
            match chats.by_internal.get(id) {
                Some((info, at)) if at.elapsed() < CHAT_FRESH => return Ok(info.clone()),
                Some((info, _)) => Some(info.clone()),
                None => None,
            }
        };
        let fetched = self.fetch_chat(id, stale.is_some()).await;
        match (fetched, stale) {
            (Ok(chat), _) => Ok(self.remember(&chat).await),
            // the name may be old, the id is not
            (Err(_), Some(info)) => Ok(info),
            (Err(e), None) => Err(e),
        }
    }

    /// Reads a chat. A group can lack its shared id for a short while after the gateway failed
    /// to translate it, so a first sight without one is asked again before it is settled.
    async fn fetch_chat(&self, id: &str, known: bool) -> Result<Chat, GwError> {
        let mut tries = 0;
        loop {
            let chat: Chat = self.link.call("chats.get", json!({"chat": id})).await?;
            let settled = known
                || map::shared_chat_id(&chat).is_some()
                || chat.kind == "p2p"
                || self.chats.lock().await.announced.contains_key(id);
            if settled || tries >= OC_RETRIES {
                return Ok(chat);
            }
            tries += 1;
            tokio::time::sleep(self.settings.timing.oc_retry).await;
        }
    }

    /// Records a chat the gateway returned and settles the id the Hub sees for it.
    pub async fn remember(&self, chat: &Chat) -> Arc<ChatInfo> {
        let mut chats = self.chats.lock().await;
        let ext_id = match chats.announced.get(&chat.id) {
            Some(ext_id) => ext_id.clone(),
            None => {
                let ext_id = map::shared_chat_id(chat).unwrap_or(&chat.id).to_string();
                if ext_id == chat.id && chat.kind != "p2p" {
                    tracing::info!("a group without a shared chat id keeps the gateway's id");
                }
                chats.announced.insert(chat.id.clone(), ext_id.clone());
                if let Err(e) = store::write_json(&self.announced_path, &chats.announced) {
                    tracing::warn!("the chat ids could not be saved: {e}");
                }
                ext_id
            }
        };
        if chats.by_internal.len() >= CHAT_CACHE {
            chats.by_internal.clear();
        }
        let info = Arc::new(ChatInfo::new(chat, ext_id));
        if info.ext_id != info.internal {
            chats.by_shared.insert(info.ext_id.clone(), info.internal.clone());
        }
        chats.by_internal.insert(info.internal.clone(), (info.clone(), Instant::now()));
        info
    }

    /// The chat the Hub means by `chat_id`: a shared id (`oc_…`) or a gateway id.
    pub async fn chat_by_ext(&self, chat_id: &str) -> Rpc<Arc<ChatInfo>> {
        let not_found = || RpcError::new(Reason::NotFound, "no such chat, or not visible to the signed-in user");
        if chat_id.starts_with("oc_") {
            let internal = self.chats.lock().await.by_shared.get(chat_id).cloned();
            if let Some(internal) = internal {
                return match self.chat_by_internal(&internal).await {
                    Ok(info) => Ok(info),
                    Err(e) => Err(self.gw_err(e).await),
                };
            }
            if !chat_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') || chat_id.len() > 128 {
                return Err(not_found());
            }
            let chat: Chat = self.gw("chats.resolve", json!({"oc": chat_id})).await?;
            let info = self.remember(&chat).await;
            // a chat announced under another id is not reachable by this one
            return if info.ext_id == chat_id { Ok(info) } else { Err(not_found()) };
        }
        if !map::gateway_id(chat_id) {
            return Err(not_found());
        }
        match self.chat_by_internal(chat_id).await {
            Ok(info) => Ok(info),
            Err(e) => Err(self.gw_err(e).await),
        }
    }
}

/// The features this server offers now (§3.2): what the gateway has and the credential may use.
pub fn features(st: &LinkState) -> Vec<&'static str> {
    let has = |feature: &str, scope: &str| st.has_feature(feature) && st.has_scope(scope);
    let mut out = vec!["me"];
    if has("im.v1", "chats.read") {
        out.push("chats.get");
    }
    if has("im.v1", "messages.read") {
        out.extend(["messages.get", "messages.history"]);
    }
    if has("changes.v1", "messages.read") {
        out.extend(["changes", "changes.inline_body"]);
    }
    if has("media.v1", "messages.read") {
        out.push("media.download");
    }
    if has("im.v1", "messages.send") {
        out.extend(["messages.send", "messages.reply"]);
    }
    if has("recall.v1", "messages.recall") {
        out.push("messages.recall");
    }
    if has("im.v1", "reactions.write") {
        out.push("reactions");
    }
    if has("org.v1", "people.read") {
        out.push("people.get");
    }
    out
}

/// What the Hub is told about the session: `(status, reason, write_policy)`.
///
/// The gateway does not say which targets its write policy allows, only refuses one by one, so
/// a credential that may write at all is `restricted`.
pub fn session_status(st: &LinkState) -> (&'static str, Option<&'static str>, &'static str) {
    let writes = ["messages.send", "messages.recall", "reactions.write"].iter().any(|s| st.has_scope(s));
    let policy = if writes { "restricted" } else { "forbidden" };
    match (st.status, st.session) {
        (LinkStatus::Refused, _) => ("expired", Some("the openlark gateway refuses the plugin's credential"), policy),
        (_, Session::Expired) => ("expired", Some("the Lark session has expired at the gateway"), policy),
        (_, Session::Locked) => ("locked", Some("the Lark session is locked at the gateway"), policy),
        (_, Session::Ok) => ("ok", None, policy),
    }
}

/// What the Hub is told about the way to Lark: `(status, reason)`.
pub fn upstream_status(st: &LinkState) -> (&'static str, Option<&'static str>) {
    match (st.status, st.push_connected) {
        (LinkStatus::Up, Some(false)) => ("down", Some("the gateway's push connection to Lark is down")),
        (LinkStatus::Up, _) => ("ok", None),
        (LinkStatus::Refused, _) => ("down", Some("the openlark gateway refuses the plugin's credential")),
        _ => ("down", Some("the openlark gateway cannot be reached")),
    }
}

/// Serves the Hub on `listener` until `shutdown` resolves.
pub async fn run(
    listener: TcpListener,
    settings: Settings,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let (stop_tx, stop) = watch::channel(false);
    let (alive, mut all_gone) = mpsc::channel::<()>(1);
    let bridge = Bridge::new(settings, stop, alive.downgrade());
    let app = crate::ext::server::router(bridge);
    let served = tokio::select! {
        r = axum::serve(listener, app).into_future() => r,
        () = shutdown => Ok(()),
    };
    // open connections close themselves; give them a moment to say so
    let _ = stop_tx.send(true);
    drop(alive);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), all_gone.recv()).await;
    served
}
