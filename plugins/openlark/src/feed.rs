//! The change feed (§5): the gateway's journal as batches for the Hub.
//!
//! Every subscription has its own gateway connection. A batch's `seq` is the gateway's journal
//! position of its last change, and the cursor carries that position, so a resume asks the
//! gateway for everything after it: nothing is kept in the bridge between connections. The
//! window and redelivery are the session's part (`ext::session`); this module only produces
//! batches, and it is held back when the session does not take them.

use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::bridge::Bridge;
use crate::ext::proto::{MAX_BATCH_ITEMS, Reason, Rpc, RpcError, notification, now_ms};
use crate::gateway::client::{self, GwConn, GwError};
use crate::gateway::link::Identity;
use crate::gateway::wire::{Change, Message, Subscribed};
use crate::map::{self, Ctx, sha256_hex};

/// The bytes of items one batch may carry, well inside the 1 MiB frame.
const BATCH_BYTES: usize = 400 * 1024;
/// An inlined message above this is left for the Hub to fetch with `messages.get`.
const INLINE_BYTES: usize = 200 * 1024;
/// A gap's start is put this far before the last moment the feed was known to be whole,
/// because the gateway says which positions were lost, not which times.
const GAP_MARGIN_MS: i64 = 120_000;

/// One batch as it goes on the wire, the same bytes on every delivery.
#[derive(Debug, Clone)]
pub struct Batch {
    pub seq: u64,
    pub frame: String,
}

pub enum Event {
    Batch(Batch),
    /// The gateway no longer knows the feed's position: the socket has to start over.
    Reset,
}

/// Where a subscription starts.
pub enum From {
    Now,
    Cursor(String),
}

/// A running subscription. Dropping it ends it.
pub struct Feed {
    pub events: mpsc::Receiver<Event>,
    conn: Arc<Mutex<Arc<GwConn>>>,
    task: JoinHandle<()>,
}

impl Feed {
    /// Tells the gateway how far the Hub has processed. Nothing depends on it arriving.
    pub fn ack(&self, seq: u64) {
        self.conn.lock().unwrap_or_else(|e| e.into_inner()).post("changes.ack", json!({"seq": seq}));
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The answer to `changes.subscribe`, before the subscription's name is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Started {
    pub watermark_ms: i64,
    pub resumed: bool,
    /// The gateway position the subscription continues after; every batch has a higher `seq`.
    pub after: u64,
}

/// A cursor names the account it belongs to, a journal position, and when the bridge made it.
fn cursor_tag(me: &Identity) -> String {
    sha256_hex(&[&map::realm(&me.tenant_id), &me.user.id])[..8].to_string()
}

fn encode_cursor(tag: &str, seq: u64, at_ms: i64) -> String {
    format!("g1:{tag}:{seq}:{at_ms}")
}

fn parse_cursor(cursor: &str, tag: &str) -> Option<(u64, i64)> {
    let mut parts = cursor.split(':');
    if parts.next()? != "g1" || parts.next()? != tag {
        return None;
    }
    let seq = parts.next()?.parse().ok()?;
    let at_ms = parts.next()?.parse().ok().filter(|t| *t >= 0)?;
    parts.next().is_none().then_some((seq, at_ms))
}

/// Whether positions after `after` were discarded before this subscription could read them.
fn pruned(after: u64, sub: &Subscribed) -> bool {
    let first_kept = if sub.earliest == 0 { sub.head + 1 } else { sub.earliest };
    after < sub.head && after + 1 < first_kept
}

const KINDS: [&str; 1] = ["message"];

/// Opens a subscription: its own gateway connection, subscribed at the journal's head or after
/// the cursor's position.
pub async fn open(bridge: &Arc<Bridge>, from: From, subscription: String) -> Rpc<(Feed, Started)> {
    let me = bridge.identity()?;
    let tag = cursor_tag(&me);
    let resume = match &from {
        From::Now => None,
        From::Cursor(c) => Some(
            parse_cursor(c, &tag)
                .ok_or_else(|| RpcError::new(Reason::UnknownCursor, "this server did not issue that cursor for this account"))?,
        ),
    };
    let settings = &bridge.settings;
    let (changes_tx, changes) = mpsc::channel(4);
    let conn = match client::connect(&settings.gateway, &settings.gateway_token, &settings.timing, Some(changes_tx)).await {
        Ok(conn) => conn,
        Err(e) => return Err(bridge.gw_err(e).await),
    };
    // taken before the subscription exists, so nothing created from here on can be missed
    let watermark_ms = now_ms();
    let params = match resume {
        None => json!({"from_head": true, "kinds": KINDS}),
        Some((seq, _)) => json!({"after": seq, "kinds": KINDS}),
    };
    let sub: Subscribed = match conn.call("changes.subscribe", params).await {
        Ok(sub) => sub,
        // a position beyond the journal's head was never issued by this gateway
        Err(GwError::Rpc { code, .. }) if resume.is_some() && code == "invalid" => {
            return Err(RpcError::new(Reason::UnknownCursor, "the gateway does not know that position"));
        }
        Err(e) => return Err(bridge.gw_err(e).await),
    };
    if let Some((seq, _)) = resume
        && pruned(seq, &sub)
    {
        return Err(RpcError::new(Reason::CursorTooOld, "the gateway no longer keeps the changes after that cursor"));
    }
    let after = resume.map_or(sub.after, |(seq, _)| seq);
    let conn = Arc::new(Mutex::new(Arc::new(conn)));
    let (events_tx, events) = mpsc::channel(1);
    let pump = Pump {
        bridge: bridge.clone(),
        me,
        tag,
        subscription,
        last_seq: after,
        whole_at_ms: resume.map_or(watermark_ms, |(_, at_ms)| at_ms.min(watermark_ms)),
        watermark_ms: resume.is_none().then_some(watermark_ms),
        conn: conn.clone(),
    };
    let task = tokio::spawn(pump.run(changes, events_tx));
    Ok((Feed { events, conn, task }, Started { watermark_ms, resumed: resume.is_some(), after }))
}

struct Pump {
    bridge: Arc<Bridge>,
    me: Identity,
    tag: String,
    subscription: String,
    /// The journal position of the last change turned into a batch.
    last_seq: u64,
    /// The last time the feed was known to have missed nothing.
    whole_at_ms: i64,
    /// Set for a subscription from "now": older messages are history, not news (§5.1).
    watermark_ms: Option<i64>,
    conn: Arc<Mutex<Arc<GwConn>>>,
}

impl Pump {
    async fn run(mut self, mut changes: mpsc::Receiver<Vec<Change>>, events: mpsc::Sender<Event>) {
        loop {
            match changes.recv().await {
                Some(batch) => {
                    for batch in self.convert(batch).await {
                        if events.send(Event::Batch(batch)).await.is_err() {
                            return;
                        }
                    }
                }
                // the gateway connection ended: continue after the last position on a new one
                None => match self.reconnect().await {
                    Some(next) => changes = next,
                    None => {
                        let _ = events.send(Event::Reset).await;
                        return;
                    }
                },
            }
        }
    }

    /// `None` when the gateway refuses the position.
    async fn reconnect(&mut self) -> Option<mpsc::Receiver<Vec<Change>>> {
        let settings = &self.bridge.settings;
        let idle_ms = i64::try_from(settings.timing.gateway_idle.as_millis()).unwrap_or(0);
        self.whole_at_ms = self.whole_at_ms.max(now_ms() - idle_ms);
        let mut backoff = settings.timing.reconnect_min;
        loop {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(settings.timing.reconnect_max);
            let (changes_tx, changes) = mpsc::channel(4);
            let Ok(conn) = client::connect(&settings.gateway, &settings.gateway_token, &settings.timing, Some(changes_tx)).await
            else {
                continue;
            };
            match conn.call::<Subscribed>("changes.subscribe", json!({"after": self.last_seq, "kinds": KINDS})).await {
                // what the gateway discarded meanwhile arrives as its own gap change
                Ok(_) => {
                    *self.conn.lock().unwrap_or_else(|e| e.into_inner()) = Arc::new(conn);
                    return Some(changes);
                }
                Err(GwError::Rpc { code, .. }) if code == "invalid" => return None,
                Err(_) => {}
            }
        }
    }

    /// Turns one gateway batch into one or more of the Hub's, in order.
    async fn convert(&mut self, changes: Vec<Change>) -> Vec<Batch> {
        let mut out = Vec::new();
        let mut items: Vec<Value> = Vec::new();
        let mut bytes = 0;
        let mut moved = false;
        for change in changes {
            if change.seq <= self.last_seq {
                continue;
            }
            if let Some(item) = self.item(&change).await {
                let size = item.to_string().len();
                if !items.is_empty() && (items.len() >= MAX_BATCH_ITEMS || bytes + size > BATCH_BYTES) {
                    out.push(self.batch(std::mem::take(&mut items)));
                    bytes = 0;
                }
                bytes += size;
                items.push(item);
            }
            self.last_seq = change.seq;
            moved = true;
        }
        // a batch whose changes were all left out still moves the cursor
        if moved {
            self.whole_at_ms = now_ms();
            out.push(self.batch(items));
        }
        out
    }

    /// A batch of everything up to `last_seq`.
    fn batch(&self, items: Vec<Value>) -> Batch {
        let cursor = encode_cursor(&self.tag, self.last_seq, now_ms());
        let params = json!({"subscription": self.subscription, "seq": self.last_seq, "cursor": cursor, "items": items});
        Batch { seq: self.last_seq, frame: notification("changes.batch", params) }
    }

    fn gap(&self, chats: Vec<String>, reason: &str) -> Value {
        let to_ms = now_ms();
        let from_ms = (self.whole_at_ms - GAP_MARGIN_MS).clamp(0, to_ms);
        let chats = if chats.is_empty() { vec!["*".to_string()] } else { chats };
        json!({"type": "gap", "chats": chats, "from_ms": from_ms, "to_ms": to_ms, "reason": reason})
    }

    /// A gap for one chat by its gateway id, or for every chat when it cannot be named.
    async fn chat_gap(&self, chat: Option<&str>, reason: &str) -> Value {
        let known = match chat {
            Some(chat) => self.bridge.chat_by_internal(chat).await.ok().map(|c| c.ext_id.clone()),
            None => None,
        };
        self.gap(known.into_iter().collect(), reason)
    }

    async fn item(&self, change: &Change) -> Option<Value> {
        match change.kind.as_str() {
            "gap" => {
                let mut chats = Vec::new();
                for chat in &change.chats {
                    match self.bridge.chat_by_internal(chat).await {
                        Ok(info) => chats.push(info.ext_id.clone()),
                        // one chat that cannot be named makes it a gap of all
                        Err(_) => {
                            chats.clear();
                            break;
                        }
                    }
                }
                Some(self.gap(chats, "the gateway discarded changes"))
            }
            "message" => self.message_item(change).await,
            _ => None,
        }
    }

    async fn message_item(&self, change: &Change) -> Option<Value> {
        let attached: Option<Message> = change.message.clone().and_then(|m| serde_json::from_value(m).ok());
        let message_id = attached.as_ref().map(|m| m.id.as_str()).or(change.message_id.as_deref());
        let chat_id = attached.as_ref().map(|m| m.chat.as_str()).or(change.chat.as_ref().map(|c| c.id()));
        let op = change.op.as_deref().unwrap_or("upsert");
        match op {
            "delete" => return None,
            "recall" => {
                let chat = self.bridge.chat_by_internal(chat_id?).await.ok()?;
                return Some(json!({"type": "message_recalled", "chat_id": chat.ext_id, "source_id": message_id?}));
            }
            "update" => {
                let m = attached.as_ref()?;
                let edited_ms = m.edited_ms?;
                let chat = self.bridge.chat_by_internal(&m.chat).await.ok()?;
                return Some(json!({"type": "message_edited", "chat_id": chat.ext_id, "source_id": m.id, "edited_ms": edited_ms}));
            }
            _ => {}
        }
        // a new message: attached, or to be read, or lost
        let message = match attached {
            Some(m) => m,
            None => {
                let read = match message_id {
                    Some(id) => self.bridge.link.call::<Message>("messages.get", json!({"message": id})).await.ok(),
                    None => None,
                };
                match read {
                    Some(m) => m,
                    None => return Some(self.chat_gap(chat_id, "a message the gateway could not hand over").await),
                }
            }
        };
        if self.watermark_ms.is_some_and(|w| message.created_ms < w) {
            return None;
        }
        let Ok(chat) = self.bridge.chat_by_internal(&message.chat).await else {
            return Some(self.gap(Vec::new(), "a message in a chat the gateway could not describe"));
        };
        if message.recalled {
            return Some(json!({"type": "message_recalled", "chat_id": chat.ext_id, "source_id": message.id}));
        }
        let cx = Ctx { me: Some(&self.me), chat: &chat };
        let mut item = map::meta(&message, &cx, change.mine);
        item["type"] = "message".into();
        let full = map::message(&message, &cx, change.mine);
        if full.to_string().len() <= INLINE_BYTES {
            item["message"] = full;
        }
        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::wire::User;

    fn me(user: &str, tenant: &str) -> Identity {
        Identity { user: User { id: user.into(), ..User::default() }, tenant_id: tenant.into() }
    }

    #[test]
    fn a_cursor_belongs_to_one_account() {
        let tag = cursor_tag(&me("100", "t1"));
        assert_eq!(tag.len(), 8);
        let c = encode_cursor(&tag, 17, 1_762_430_400_000);
        assert!(c.len() <= 1024);
        assert_eq!(parse_cursor(&c, &tag), Some((17, 1_762_430_400_000)));
        assert_eq!(parse_cursor(&c, &cursor_tag(&me("101", "t1"))), None);
        assert_eq!(parse_cursor(&c, &cursor_tag(&me("100", "t2"))), None);
        for bad in ["", "now", "c:17", "g1:zz:17:5", &format!("g1:{tag}:x:5"), &format!("g1:{tag}:17"), &format!("g1:{tag}:17:5:6"), &format!("g1:{tag}:17:-5")] {
            assert_eq!(parse_cursor(bad, &tag), None, "{bad}");
        }
    }

    #[test]
    fn a_position_is_too_old_when_the_gateway_dropped_what_follows() {
        let sub = |head, earliest| Subscribed { after: 0, head, earliest };
        // everything after 10 is still kept
        assert!(!pruned(10, &sub(20, 5)));
        assert!(!pruned(10, &sub(20, 11)));
        // 11 is gone
        assert!(pruned(10, &sub(20, 12)));
        // nothing is kept at all, but changes came after 10
        assert!(pruned(10, &sub(20, 0)));
        // at the head nothing can have been lost
        assert!(!pruned(20, &sub(20, 0)));
        assert!(!pruned(20, &sub(20, 30)));
    }
}
