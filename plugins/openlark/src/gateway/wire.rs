//! The part of the gateway's protocol (openlark-protocol v0) this bridge uses, as lenient serde
//! types: only the methods it calls and only the fields it reads. A field the gateway adds later
//! is ignored.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The protocol name the gateway must answer in hello.
pub const PROTOCOL: &str = "openlark/0";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// The gateway's own id of the user.
    #[serde(default)]
    pub id: String,
    /// Known only for the signed-in user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub union_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enterprise_email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub en_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external: Option<bool>,
    /// `user`, `bot` or `app`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Tenant {
    #[serde(default)]
    pub id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Account {
    #[serde(default)]
    pub user: Option<User>,
    #[serde(default)]
    pub tenant: Option<Tenant>,
    /// `ok`, `expired` or `locked`.
    #[serde(default)]
    pub session: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct HelloResult {
    #[serde(default)]
    pub protocol: String,
    #[serde(default)]
    pub account: Account,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub features: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Me {
    pub user: User,
    pub tenant: Tenant,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Chat {
    pub id: String,
    /// The chat id the platform's open API uses, when the gateway knows it.
    #[serde(default)]
    pub oc: Option<String>,
    /// `p2p`, `group` or `topic`.
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub external: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Mention {
    /// A user id, or `all`.
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub name: Option<String>,
}

/// A message body. `type` says which of the other fields are set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Content {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub markdown: Option<String>,
    /// The media reference of an image or file.
    #[serde(rename = "ref", default)]
    pub media: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub mime: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Message {
    pub id: String,
    /// The gateway's id of the chat.
    pub chat: String,
    /// The root message of the thread the message is in.
    #[serde(default)]
    pub thread: Option<String>,
    /// The message this one answers.
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub sender: User,
    pub created_ms: i64,
    #[serde(default)]
    pub edited_ms: Option<i64>,
    #[serde(default)]
    pub recalled: bool,
    #[serde(default)]
    pub content: Content,
    #[serde(default)]
    pub mentions: Vec<Mention>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct HistoryPage {
    /// Oldest first.
    #[serde(default)]
    pub items: Vec<Message>,
    /// Where the page before this one ends (`before`).
    #[serde(default)]
    pub prev: Option<String>,
    /// Where the page after this one starts (`after`).
    #[serde(default)]
    pub next: Option<String>,
}

/// The chat of a change: its id, or the whole object.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ChatRef {
    Id(String),
    Chat(Chat),
}

impl ChatRef {
    pub fn id(&self) -> &str {
        match self {
            Self::Id(id) => id,
            Self::Chat(c) => &c.id,
        }
    }
}

/// One journal entry. `message` stays raw so that an entry the bridge cannot read still moves
/// the position.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Change {
    pub seq: u64,
    #[serde(default)]
    pub kind: String,
    /// `upsert`, `update`, `recall` or `delete`.
    #[serde(default)]
    pub op: Option<String>,
    #[serde(default)]
    pub message: Option<Value>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub chat: Option<ChatRef>,
    /// `unavailable` when the gateway could not attach the message.
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub mine: bool,
    #[serde(default)]
    pub at_ms: Option<i64>,
    /// A `gap` covers the positions `from..=to` of `chats` (all chats when empty).
    #[serde(default)]
    pub chats: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct Subscribed {
    /// The position the subscription continues after.
    #[serde(default)]
    pub after: u64,
    /// The newest position in the journal.
    #[serde(default)]
    pub head: u64,
    /// The oldest position still kept, 0 when nothing is.
    #[serde(default)]
    pub earliest: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PushStatus {
    #[serde(default)]
    pub connected: bool,
}

/// A user's profile. The user's own fields are flattened into it.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Person {
    #[serde(flatten)]
    pub user: User,
    #[serde(default)]
    pub avatar_url: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct People {
    #[serde(default)]
    pub people: Vec<Person>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_change_keeps_its_position_whatever_it_carries() {
        let c: Change = serde_json::from_value(json!({"seq": 9, "kind": "calendar", "event": {"x": 1}})).unwrap();
        assert_eq!((c.seq, c.kind.as_str()), (9, "calendar"));
        let c: Change =
            serde_json::from_value(json!({"seq": 10, "kind": "message", "op": "upsert", "chat": "7", "mine": true}))
                .unwrap();
        assert_eq!(c.chat.unwrap().id(), "7");
        assert!(c.mine);
        let c: Change =
            serde_json::from_value(json!({"seq": 11, "kind": "message", "chat": {"id": "8", "kind": "group"}}))
                .unwrap();
        assert_eq!(c.chat.unwrap().id(), "8");
    }

    #[test]
    fn a_message_reads_every_content_type() {
        let m: Message = serde_json::from_value(json!({"id": "5", "chat": "7", "sender": {"id": "1"},
            "created_ms": 1, "content": {"type": "file", "ref": "m.7.5.a", "name": "a.pdf", "size": 3}}))
        .unwrap();
        assert_eq!(m.content.media.as_deref(), Some("m.7.5.a"));
        let p: Person = serde_json::from_value(json!({"id": "1", "name": "Ada", "avatar_url": "https://x"})).unwrap();
        assert_eq!((p.user.id.as_str(), p.avatar_url.as_deref()), ("1", Some("https://x")));
    }
}
