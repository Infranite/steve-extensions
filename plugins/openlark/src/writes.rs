//! Writes as the signed-in user (§4.4): the sender is checked before anything is sent, and a
//! repeated idempotency key returns the first result.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::bridge::Bridge;
use crate::ext::proto::{MAX_TEXT_BYTES, Reason, Rpc, RpcError, now_ms, str_param};
use crate::gateway::wire::{Me, Message};
use crate::map::{gateway_id, sha256_hex};
use crate::store;

/// Keys are remembered for twice the 24 h the protocol asks for.
const KEEP_MS: i64 = 48 * 3600 * 1000;
/// The gateway's own idempotency keys are at most this long.
const GATEWAY_KEY_MAX: usize = 64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Entry {
    /// A hash of the method and params the key was first used with.
    hash: String,
    result: Value,
    at_ms: i64,
}

/// The record of answered writes, kept in the data directory so that it survives a restart.
pub struct IdemStore {
    path: PathBuf,
    entries: Mutex<HashMap<String, Entry>>,
}

impl IdemStore {
    pub fn load(path: PathBuf) -> Self {
        let mut entries: HashMap<String, Entry> = store::read_json(&path).unwrap_or_default();
        let now = now_ms();
        entries.retain(|_, e| now - e.at_ms < KEEP_MS);
        Self { path, entries: Mutex::new(entries) }
    }

    /// The first result of this key: `Ok(None)` when the key is new, a conflict when it was
    /// used with other params.
    fn seen(&self, key: &str, hash: &str) -> Rpc<Option<Value>> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        match entries.get(key) {
            None => Ok(None),
            Some(e) if e.hash == hash => Ok(Some(e.result.clone())),
            Some(_) => {
                Err(RpcError::new(Reason::IdempotencyConflict, "the idempotency key was used for another request"))
            }
        }
    }

    fn record(&self, key: &str, hash: String, result: &Value) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let now = now_ms();
        entries.retain(|_, e| now - e.at_ms < KEEP_MS);
        entries.insert(key.to_string(), Entry { hash, result: result.clone(), at_ms: now });
        if let Err(e) = store::write_json(&self.path, &*entries) {
            tracing::warn!("the record of writes could not be saved: {e}");
        }
    }
}

/// The key the gateway gets for a Hub key: itself when it fits, else a hash of it.
pub fn gateway_key(key: &str) -> String {
    if key.len() <= GATEWAY_KEY_MAX { key.to_string() } else { format!("h:{}", &sha256_hex(&[key])[..62]) }
}

fn key_ok(k: &str) -> bool {
    (1..=128).contains(&k.len())
        && k.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

/// The Lark reaction for each emoji the Hub sends. These are the ones Steve's own Feishu
/// adapter uses for the same three states.
pub fn reaction_key(emoji: &str) -> Option<&'static str> {
    match emoji.trim_end_matches('\u{fe0f}') {
        "👀" => Some("OnIt"),
        "✅" => Some("DONE"),
        "❌" => Some("CrossMark"),
        _ => None,
    }
}

fn reaction_id_ok(id: &str) -> bool {
    (1..=64).contains(&id.len()) && id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

/// The value of `expected_sender`: a `Handle` of kind `id`.
fn expected_sender(params: &Value) -> Rpc<&str> {
    let e = params.get("expected_sender").ok_or_else(|| RpcError::invalid("a write needs `expected_sender`"))?;
    match (e.get("kind").and_then(Value::as_str), e.get("value").and_then(Value::as_str)) {
        (Some("id"), Some(value)) if !value.is_empty() => Ok(value),
        // any other handle cannot be the primary handle this server gave in me.get
        (Some(_), Some(_)) => Err(account_changed()),
        _ => Err(RpcError::invalid("`expected_sender` is not a handle")),
    }
}

fn account_changed() -> RpcError {
    RpcError::new(Reason::AccountChanged, "another user is signed in at the gateway")
}

/// The gateway message a `Target` names. Only the `source_id` this server gave addresses one.
fn target<'a>(params: &'a Value, key: &str) -> Rpc<&'a str> {
    let t =
        params.get(key).filter(|t| t.is_object()).ok_or_else(|| RpcError::invalid(format!("`{key}` is missing")))?;
    let source = t.get("source_id").and_then(Value::as_str);
    let id = t.get("id").and_then(Value::as_str);
    match (source, id) {
        (Some(s), _) if gateway_id(s) => Ok(s),
        (None, None) => Err(RpcError::invalid(format!("`{key}` names no message"))),
        _ => Err(RpcError::new(Reason::NotFound, "no such message")),
    }
}

impl Bridge {
    /// Refuses a write meant for another user than the one signed in (§4.4), before any side
    /// effect. `fresh` asks the gateway now, for the methods where the gateway itself does not
    /// check the sender.
    async fn check_sender(&self, params: &Value, fresh: bool) -> Rpc<String> {
        let expected = expected_sender(params)?;
        let me = if fresh { self.gw::<Me>("me.get", json!({})).await?.user.id } else { self.identity()?.user.id };
        if expected != me {
            return Err(account_changed());
        }
        Ok(me)
    }

    /// `messages.send` and `messages.reply`.
    pub async fn send(&self, method: &'static str, params: &Value) -> Rpc {
        let me = self.check_sender(params, false).await?;
        let key = match params.get("idempotency_key").and_then(Value::as_str) {
            Some(k) if key_ok(k) => k,
            _ => return Err(RpcError::invalid("`idempotency_key` is missing or malformed")),
        };
        let text = params.get("text").and_then(Value::as_str).ok_or_else(|| RpcError::invalid("`text` is missing"))?;
        if text.len() > MAX_TEXT_BYTES {
            return Err(RpcError::new(Reason::TooLarge, "the text is longer than max_text_bytes"));
        }
        if text.trim().is_empty() {
            return Err(RpcError::invalid("`text` is empty"));
        }
        let mut mentions = Vec::new();
        for m in params.get("mentions").and_then(Value::as_array).into_iter().flatten() {
            match (m.get("kind").and_then(Value::as_str), m.get("value").and_then(Value::as_str)) {
                (Some("id"), Some(id)) if gateway_id(id) => mentions.push(json!({"user": id})),
                _ => return Err(RpcError::invalid("a mention must be the `id` handle of a user")),
            }
        }
        let mut call = json!({
            "content": {"type": "text", "text": text},
            "idempotency_key": gateway_key(key),
            "expected_sender": me,
        });
        if !mentions.is_empty() {
            call["mentions"] = mentions.into();
        }
        let chat_id = str_param(params, "chat_id")?;
        let reply = method == "messages.reply";
        if reply {
            call["message"] = target(params, "to")?.into();
            call["in_thread"] = params.get("in_thread").and_then(Value::as_bool).unwrap_or(false).into();
        }

        let hash = sha256_hex(&[method, &params.to_string()]);
        let _one_at_a_time = self.write_lock.lock().await;
        if let Some(first) = self.idem.seen(key, &hash)? {
            return Ok(first);
        }
        if !reply {
            call["chat"] = self.chat_by_ext(chat_id).await?.internal.clone().into();
        }
        let sent: Message = self.gw(method, call).await?;
        let result = json!({
            "source_id": sent.id,
            "id": null,
            "created_ms": (sent.created_ms > 0).then_some(sent.created_ms),
            "thread_id": sent.thread,
        });
        self.idem.record(key, hash, &result);
        Ok(result)
    }

    /// `messages.recall`.
    pub async fn recall(&self, params: &Value) -> Rpc {
        str_param(params, "chat_id")?;
        let message = target(params, "target")?;
        self.check_sender(params, true).await?;
        let _one_at_a_time = self.write_lock.lock().await;
        let _: Value = self.gw("messages.recall", json!({"message": message})).await?;
        Ok(json!({}))
    }

    /// `reactions.add` and `reactions.remove`. The gateway names a reaction by its emoji key, so
    /// that key is the `reaction_id`.
    pub async fn react(&self, method: &'static str, params: &Value) -> Rpc {
        str_param(params, "chat_id")?;
        let message = target(params, "target")?;
        let add = method == "reactions.add";
        let key = if add {
            let emoji =
                params.get("emoji").and_then(Value::as_str).ok_or_else(|| RpcError::invalid("`emoji` is missing"))?;
            reaction_key(emoji)
                .ok_or_else(|| RpcError::new(Reason::Unsupported, "no Lark reaction is mapped to this emoji"))?
        } else {
            match params.get("reaction_id").and_then(Value::as_str) {
                Some(id) if reaction_id_ok(id) => id,
                _ => return Err(RpcError::invalid("`reaction_id` is missing or malformed")),
            }
        };
        self.check_sender(params, true).await?;
        let _one_at_a_time = self.write_lock.lock().await;
        let _: Value = self.gw(method, json!({"message": message, "emoji": key})).await?;
        Ok(if add { json!({"reaction_id": key}) } else { json!({}) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_remembered_with_its_params() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idempotency.json");
        let store = IdemStore::load(path.clone());
        assert_eq!(store.seen("k1", "h1"), Ok(None));
        store.record("k1", "h1".into(), &json!({"source_id": "5"}));
        assert_eq!(store.seen("k1", "h1"), Ok(Some(json!({"source_id": "5"}))));
        assert_eq!(store.seen("k1", "h2").unwrap_err().reason, Reason::IdempotencyConflict);
        // a restart reads the same record
        let again = IdemStore::load(path);
        assert_eq!(again.seen("k1", "h1"), Ok(Some(json!({"source_id": "5"}))));
    }

    #[test]
    fn an_old_key_is_forgotten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idempotency.json");
        let old = Entry { hash: "h".into(), result: json!({}), at_ms: now_ms() - KEEP_MS - 1 };
        let new = Entry { hash: "h".into(), result: json!({}), at_ms: now_ms() };
        store::write_json(&path, &HashMap::from([("old".to_string(), old), ("new".to_string(), new)])).unwrap();
        let store = IdemStore::load(path);
        assert_eq!(store.seen("old", "h"), Ok(None));
        assert!(store.seen("new", "h").unwrap().is_some());
    }

    #[test]
    fn a_long_key_is_hashed_for_the_gateway() {
        assert_eq!(gateway_key("r:42:ack"), "r:42:ack");
        let long = "k".repeat(65);
        let short = gateway_key(&long);
        assert_eq!(short.len(), 64);
        assert!(short.starts_with("h:"));
        assert_eq!(short, gateway_key(&long));
        assert_ne!(short, gateway_key(&"k".repeat(66)));
        assert!(
            key_ok("h:0123abcd") && key_ok("a.b_c-d:e") && !key_ok("") && !key_ok("a b") && !key_ok(&"k".repeat(129))
        );
    }

    #[test]
    fn the_hubs_three_emoji_have_a_reaction() {
        assert_eq!(reaction_key("👀"), Some("OnIt"));
        assert_eq!(reaction_key("✅"), Some("DONE"));
        assert_eq!(reaction_key("❌"), Some("CrossMark"));
        assert_eq!(reaction_key("❌\u{fe0f}"), Some("CrossMark"));
        assert_eq!(reaction_key("🎉"), None);
        assert!(reaction_id_ok("OnIt") && !reaction_id_ok("on it") && !reaction_id_ok(""));
    }

    #[test]
    fn the_sender_and_the_target_are_read_strictly() {
        assert_eq!(expected_sender(&json!({"expected_sender": {"kind": "id", "value": "100"}})), Ok("100"));
        assert_eq!(expected_sender(&json!({})).unwrap_err().reason, Reason::Invalid);
        assert_eq!(expected_sender(&json!({"expected_sender": "100"})).unwrap_err().reason, Reason::Invalid);
        let other = json!({"expected_sender": {"kind": "email", "value": "a@b.example"}});
        assert_eq!(expected_sender(&other).unwrap_err().reason, Reason::AccountChanged);
        assert_eq!(target(&json!({"to": {"source_id": "55"}}), "to"), Ok("55"));
        assert_eq!(target(&json!({"to": {"id": "om_1"}}), "to").unwrap_err().reason, Reason::NotFound);
        assert_eq!(target(&json!({"to": {"source_id": "om_1"}}), "to").unwrap_err().reason, Reason::NotFound);
        assert_eq!(target(&json!({"to": {}}), "to").unwrap_err().reason, Reason::Invalid);
        assert_eq!(target(&json!({}), "to").unwrap_err().reason, Reason::Invalid);
    }
}
