//! Gateway objects as the external channel protocol's objects (§6). Nothing here calls the
//! gateway; what the gateway does not say stays null.

use std::collections::HashSet;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::ext::proto::{MAX_TEXT_BYTES, opaque_ok};
use crate::gateway::link::Identity;
use crate::gateway::wire::{Chat, Content, Message, Person, User};

/// The handle system Steve's built-in Feishu adapter stores users under: a `union_id`.
pub const FEISHU_SYSTEM: &str = "feishu";

/// The sender id of a message the gateway gives no sender for.
const NO_SENDER: &str = "unknown";

pub fn sha256_hex(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p.len().to_le_bytes());
        h.update(p.as_bytes());
    }
    hex::encode(h.finalize())
}

/// The realm of an account: its tenant. `[a-z0-9._-]`, at most 64 bytes.
pub fn realm(tenant_id: &str) -> String {
    let plain = !tenant_id.is_empty()
        && tenant_id.len() <= 48
        && tenant_id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-'));
    if plain { format!("lark-{tenant_id}") } else { format!("lark-h{}", &sha256_hex(&[tenant_id])[..32]) }
}

pub fn id_handle(id: &str) -> Value {
    json!({"kind": "id", "value": id})
}

/// Whether the gateway could mean a user, chat or message by this: its ids are decimal numbers.
pub fn gateway_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= 32 && s.bytes().all(|b| b.is_ascii_digit())
}

fn email(raw: &str) -> Option<String> {
    let e = raw.trim().to_lowercase();
    let ok = e.len() <= 256 && e.matches('@').count() == 1 && !e.starts_with('@') && !e.ends_with('@');
    (ok && !e.chars().any(|c| c.is_control() || c.is_whitespace())).then_some(e)
}

/// The other handles of a user: mail addresses, and the `union_id` under the system Steve's
/// Feishu bot adapter uses, so that the same person matches across both.
pub fn aliases(u: &User) -> Vec<Value> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for e in [&u.email, &u.enterprise_email].into_iter().flatten().filter_map(|e| email(e)) {
        if seen.insert(e.clone()) {
            out.push(json!({"kind": "email", "value": e}));
        }
    }
    if let Some(union) = u.union_id.as_deref().filter(|v| opaque_ok(v, 256)) {
        out.push(json!({"kind": "platform", "system": FEISHU_SYSTEM, "value": union}));
    }
    out
}

fn clip(s: &str, max_chars: usize) -> String {
    s.chars().take(max_chars).collect()
}

pub fn display(u: &User) -> Option<String> {
    [&u.name, &u.en_name].into_iter().flatten().map(|n| n.trim()).find(|n| !n.is_empty()).map(|n| clip(n, 256))
}

/// A `Sender`. The signed-in user's own messages carry the aliases `me.get` gave, which the
/// gateway does not repeat on every message.
pub fn sender(u: &User, me: Option<&Identity>) -> Value {
    let own = me.map(|i| &i.user).filter(|own| own.id == u.id);
    let id = if opaque_ok(&u.id, 256) { u.id.as_str() } else { NO_SENDER };
    let mut v = json!({
        "id": id_handle(id),
        "is_bot": matches!(u.kind.as_deref(), Some("bot" | "app")),
        "external": u.external == Some(true),
        "aliases": aliases(own.unwrap_or(u)),
    });
    if let Some(name) = display(u).or_else(|| own.and_then(display)) {
        v["display"] = name.into();
    }
    v
}

/// The `user` of `me.get`.
pub fn me_user(me: &Identity) -> Value {
    let mut v = json!({"id": id_handle(&me.user.id), "aliases": aliases(&me.user)});
    if let Some(name) = display(&me.user) {
        v["display"] = name.into();
    }
    v
}

/// A chat as the bridge knows it: the gateway's id and the id the Hub sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatInfo {
    pub internal: String,
    /// The platform's shared chat id (`oc_…`) when the gateway knows it, else `internal`.
    pub ext_id: String,
    pub kind: String,
    pub name: Option<String>,
    pub external: Option<bool>,
}

impl ChatInfo {
    pub fn new(chat: &Chat, ext_id: String) -> Self {
        Self {
            internal: chat.id.clone(),
            ext_id,
            kind: chat.kind.clone(),
            name: chat.name.as_deref().map(|n| clip(n, 256)).filter(|n| !n.is_empty()),
            external: chat.external,
        }
    }

    pub fn ext_kind(&self) -> Option<&'static str> {
        match self.kind.as_str() {
            "p2p" => Some("dm"),
            "group" => Some("group"),
            "topic" => Some("channel"),
            _ => None,
        }
    }
}

/// The shared chat id of a gateway chat, when it has a usable one.
pub fn shared_chat_id(chat: &Chat) -> Option<&str> {
    chat.oc.as_deref().filter(|oc| oc.starts_with("oc_") && opaque_ok(oc, 256))
}

/// A `Chat`. The gateway gives no member count.
pub fn chat(info: &ChatInfo) -> Value {
    json!({
        "chat_id": info.ext_id,
        "kind": info.ext_kind(),
        "name": info.name,
        "member_count": null,
        "has_external": info.external,
    })
}

struct Body {
    kind: &'static str,
    text: String,
    attachment: Option<Value>,
}

fn attachment(kind: &str, c: &Content) -> Option<Value> {
    let key = c.media.as_deref().filter(|k| !k.is_empty() && k.len() <= 512)?;
    let mut a = json!({"type": kind, "key": key});
    if let Some(name) = c.name.as_deref().filter(|n| !n.is_empty()) {
        a["name"] = clip(name, 256).into();
    }
    if let Some(size) = c.size {
        a["size"] = size.into();
    }
    if let Some(mime) = c.mime.as_deref().filter(|m| !m.is_empty() && m.len() <= 128) {
        a["mime"] = mime.into();
    }
    Some(a)
}

fn body(c: &Content) -> Body {
    let own = |s: &Option<String>| s.clone().unwrap_or_default();
    match c.kind.as_str() {
        "text" => Body { kind: "text", text: own(&c.text), attachment: None },
        "post" => {
            let markdown = own(&c.markdown);
            let text = match c.title.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
                Some(title) => format!("{title}\n{markdown}"),
                None => markdown,
            };
            Body { kind: "rich", text, attachment: None }
        }
        "image" => Body { kind: "rich", text: "[image]".into(), attachment: attachment("image", c) },
        "file" => {
            let text = match c.name.as_deref().filter(|n| !n.is_empty()) {
                Some(name) => format!("[file] {name}"),
                None => "[file]".into(),
            };
            Body { kind: "rich", text, attachment: attachment("file", c) }
        }
        "card" => Body { kind: "rich", text: own(&c.summary), attachment: None },
        _ => Body { kind: "unknown", text: own(&c.summary), attachment: None },
    }
}

/// Cuts a text at a character boundary so that it fits `max` bytes. `true` when it was cut.
fn fit(text: &mut String, max: usize) -> bool {
    if text.len() <= max {
        return false;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    true
}

/// What a message is mapped with: the signed-in user and the message's chat.
pub struct Ctx<'a> {
    pub me: Option<&'a Identity>,
    pub chat: &'a ChatInfo,
}

pub fn is_mine(m: &Message, me: Option<&Identity>, mine_hint: bool) -> bool {
    mine_hint || me.is_some_and(|i| !m.sender.id.is_empty() && i.user.id == m.sender.id)
}

/// The metadata of a message: what a feed item carries (§5.2).
pub fn meta(m: &Message, cx: &Ctx, mine_hint: bool) -> Value {
    json!({
        "chat_id": cx.chat.ext_id,
        "source_id": m.id,
        "id": null,
        "created_ms": m.created_ms,
        "sender": sender(&m.sender, cx.me),
        "is_mine": is_mine(m, cx.me, mine_hint),
        "chat_kind": cx.chat.ext_kind(),
        "thread_id": m.thread.as_deref().filter(|t| opaque_ok(t, 256)),
    })
}

/// A full `Message`. `source_id` is the gateway's message id everywhere: feed, reads and
/// `SendResult`. The platform's own message id is not known to the gateway, so `id` is null.
pub fn message(m: &Message, cx: &Ctx, mine_hint: bool) -> Value {
    let mut v = meta(m, cx, mine_hint);
    let mut b =
        if m.recalled { Body { kind: "text", text: String::new(), attachment: None } } else { body(&m.content) };
    let truncated = fit(&mut b.text, MAX_TEXT_BYTES);
    let me_id = cx.me.map(|i| i.user.id.as_str());
    let mentions: Vec<Value> = m
        .mentions
        .iter()
        .take(256)
        .map(|x| {
            let user = gateway_id(&x.user).then(|| id_handle(&x.user));
            let name = x
                .name
                .as_deref()
                .map(|n| clip(n, 256))
                .unwrap_or_else(|| if x.user == "all" { "all".to_string() } else { String::new() });
            json!({"user": user, "name": name, "is_me": me_id == Some(x.user.as_str())})
        })
        .collect();
    let o = v.as_object_mut().expect("meta is an object");
    o.insert("edited_ms".into(), json!(m.edited_ms));
    o.insert("chat_name".into(), json!(cx.chat.name));
    let reply_to = m.parent.as_deref().filter(|p| opaque_ok(p, 256)).map(|p| json!({"source_id": p, "id": null}));
    o.insert("reply_to".into(), json!(reply_to));
    o.insert("root_id".into(), Value::Null);
    o.insert("kind".into(), b.kind.into());
    o.insert("text".into(), b.text.into());
    o.insert("mentions".into(), mentions.into());
    o.insert("attachments".into(), b.attachment.into_iter().collect::<Vec<_>>().into());
    o.insert("recalled".into(), m.recalled.into());
    o.insert("raw".into(), if truncated { json!({"text_truncated": true}) } else { json!({}) });
    v
}

/// One entry of `people.get`. The gateway has no title or organisation, and it does not say
/// whether a person is in the directory; someone of the signed-in user's own tenant is.
pub fn person(p: &Person, me: Option<&Identity>) -> Value {
    let same_tenant = me.is_some_and(|i| p.user.tenant_id.as_deref() == Some(i.tenant_id.as_str()));
    let mail = [&p.user.email, &p.user.enterprise_email].into_iter().flatten().find_map(|e| email(e));
    json!({
        "user": id_handle(&p.user.id),
        "display": display(&p.user),
        "email": mail,
        "avatar_url": p.avatar_url.as_deref().filter(|u| u.starts_with("https://") && u.len() <= 2048),
        "title": null,
        "org": null,
        "aliases": aliases(&p.user),
        "directory": same_tenant || p.user.external == Some(false),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(v: Value) -> User {
        serde_json::from_value(v).unwrap()
    }

    fn me() -> Identity {
        Identity {
            user: user(json!({"id": "100", "union_id": "on_me", "email": "Ada@Example.com", "name": "Ada"})),
            tenant_id: "t1".into(),
        }
    }

    fn group() -> ChatInfo {
        ChatInfo {
            internal: "7".into(),
            ext_id: "oc_7".into(),
            kind: "group".into(),
            name: Some("Design".into()),
            external: Some(false),
        }
    }

    fn msg(v: Value) -> Message {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn the_realm_is_the_tenant() {
        assert_eq!(realm("t1"), "lark-t1");
        assert_eq!(realm("7300000000000000001"), "lark-7300000000000000001");
        let hashed = realm("Tenant One");
        assert!(hashed.starts_with("lark-h") && hashed.len() == 38);
        assert_ne!(realm(""), realm("Tenant One"));
        for r in [realm("t1"), hashed, realm("")] {
            assert!(
                r.len() <= 64 && r.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
            );
        }
    }

    #[test]
    fn a_sender_has_an_id_handle_and_typed_aliases() {
        let s = sender(&user(json!({"id": "9", "name": " Grace ", "kind": "bot", "external": true})), Some(&me()));
        assert_eq!(
            s,
            json!({"id": {"kind": "id", "value": "9"}, "display": "Grace", "is_bot": true, "external": true, "aliases": []})
        );
        // the own account brings the aliases me.get gave
        let s = sender(&user(json!({"id": "100"})), Some(&me()));
        assert_eq!(
            s["aliases"],
            json!([{"kind": "email", "value": "ada@example.com"}, {"kind": "platform", "system": "feishu", "value": "on_me"}])
        );
        assert_eq!(s["display"], "Ada");
        // a second address is kept once, a broken one is left out
        let u = user(json!({"id": "3", "email": "a@b.example", "enterprise_email": "A@B.example"}));
        assert_eq!(aliases(&u).len(), 1);
        assert!(aliases(&user(json!({"id": "3", "email": "a@b@c"}))).is_empty());
        assert_eq!(sender(&user(json!({})), None)["id"]["value"], "unknown");
    }

    #[test]
    fn a_text_message_maps_field_by_field() {
        let m = msg(
            json!({"id": "501", "chat": "7", "thread": "400", "parent": "450", "sender": {"id": "9", "name": "Grace"},
            "created_ms": 1_762_430_401_234_i64, "content": {"type": "text", "text": "hello @Ada"},
            "mentions": [{"user": "100", "name": "Ada"}, {"user": "all"}]}),
        );
        let own = me();
        let chat = group();
        let v = message(&m, &Ctx { me: Some(&own), chat: &chat }, false);
        assert_eq!(v["chat_id"], "oc_7");
        assert_eq!(v["source_id"], "501");
        assert_eq!(v["id"], Value::Null);
        assert_eq!(v["created_ms"], 1_762_430_401_234_i64);
        assert_eq!(v["is_mine"], false);
        assert_eq!(v["chat_kind"], "group");
        assert_eq!(v["chat_name"], "Design");
        assert_eq!(v["thread_id"], "400");
        assert_eq!(v["reply_to"], json!({"source_id": "450", "id": null}));
        assert_eq!(v["kind"], "text");
        assert_eq!(v["text"], "hello @Ada");
        assert_eq!(v["mentions"][0], json!({"user": {"kind": "id", "value": "100"}, "name": "Ada", "is_me": true}));
        assert_eq!(v["mentions"][1], json!({"user": null, "name": "all", "is_me": false}));
        assert_eq!(v["attachments"], json!([]));
        assert_eq!(v["recalled"], false);
    }

    #[test]
    fn mine_is_the_gateways_word_or_the_own_sender() {
        let own = me();
        let chat = group();
        let cx = Ctx { me: Some(&own), chat: &chat };
        let theirs = msg(
            json!({"id": "1", "chat": "7", "sender": {"id": "9"}, "created_ms": 1, "content": {"type": "text", "text": "x"}}),
        );
        let mine = msg(
            json!({"id": "2", "chat": "7", "sender": {"id": "100"}, "created_ms": 1, "content": {"type": "text", "text": "x"}}),
        );
        assert_eq!(meta(&theirs, &cx, false)["is_mine"], false);
        assert_eq!(meta(&theirs, &cx, true)["is_mine"], true);
        assert_eq!(meta(&mine, &cx, false)["is_mine"], true);
    }

    #[test]
    fn other_contents_become_a_rendering_and_attachments() {
        let own = me();
        let chat = group();
        let cx = Ctx { me: Some(&own), chat: &chat };
        let base = |content: Value| {
            msg(json!({"id": "1", "chat": "7", "sender": {"id": "9"}, "created_ms": 1, "content": content}))
        };
        let v = message(&base(json!({"type": "post", "title": "Plan", "markdown": "**a**"})), &cx, false);
        assert_eq!((v["kind"].as_str(), v["text"].as_str()), (Some("rich"), Some("Plan\n**a**")));
        let v = message(&base(json!({"type": "image", "ref": "m.7.1.abc", "width": 2, "height": 3})), &cx, false);
        assert_eq!(v["attachments"], json!([{"type": "image", "key": "m.7.1.abc"}]));
        let v = message(
            &base(json!({"type": "file", "ref": "m.7.1.abc", "name": "a.pdf", "size": 9, "mime": "application/pdf"})),
            &cx,
            false,
        );
        assert_eq!(
            v["attachments"],
            json!([{"type": "file", "key": "m.7.1.abc", "name": "a.pdf", "size": 9, "mime": "application/pdf"}])
        );
        assert_eq!(v["text"], "[file] a.pdf");
        let v = message(&base(json!({"type": "card", "summary": "Approval", "raw": {}})), &cx, false);
        assert_eq!((v["kind"].as_str(), v["text"].as_str()), (Some("rich"), Some("Approval")));
        let v = message(&base(json!({"type": "other", "summary": "a sticker", "lark_type": "sticker"})), &cx, false);
        assert_eq!((v["kind"].as_str(), v["text"].as_str()), (Some("unknown"), Some("a sticker")));
        let v = message(&base(json!({"type": "hologram"})), &cx, false);
        assert_eq!((v["kind"].as_str(), v["text"].as_str()), (Some("unknown"), Some("")));
    }

    #[test]
    fn a_recalled_message_has_no_body_and_a_long_text_is_cut() {
        let own = me();
        let chat = group();
        let cx = Ctx { me: Some(&own), chat: &chat };
        let m = msg(json!({"id": "1", "chat": "7", "sender": {"id": "9"}, "created_ms": 1, "recalled": true,
            "content": {"type": "image", "ref": "m.7.1.abc"}}));
        let v = message(&m, &cx, false);
        assert_eq!((v["recalled"].as_bool(), v["text"].as_str()), (Some(true), Some("")));
        assert_eq!(v["attachments"], json!([]));
        let long = "é".repeat(MAX_TEXT_BYTES);
        let m = msg(
            json!({"id": "1", "chat": "7", "sender": {"id": "9"}, "created_ms": 1, "content": {"type": "text", "text": long}}),
        );
        let v = message(&m, &cx, false);
        assert_eq!(v["text"].as_str().unwrap().len(), MAX_TEXT_BYTES);
        assert_eq!(v["raw"], json!({"text_truncated": true}));
    }

    #[test]
    fn a_chat_uses_the_shared_id_when_there_is_one() {
        let c: Chat =
            serde_json::from_value(json!({"id": "7", "oc": "oc_7", "kind": "group", "name": "Design"})).unwrap();
        assert_eq!(shared_chat_id(&c), Some("oc_7"));
        let info = ChatInfo::new(&c, "oc_7".into());
        assert_eq!(
            chat(&info),
            json!({"chat_id": "oc_7", "kind": "group", "name": "Design", "member_count": null, "has_external": null})
        );
        let p: Chat = serde_json::from_value(json!({"id": "8", "kind": "p2p", "external": true})).unwrap();
        assert_eq!(shared_chat_id(&p), None);
        assert_eq!(
            chat(&ChatInfo::new(&p, "8".into())),
            json!({"chat_id": "8", "kind": "dm", "name": null, "member_count": null, "has_external": true})
        );
        let odd: Chat = serde_json::from_value(json!({"id": "9", "oc": "9", "kind": "topic"})).unwrap();
        assert_eq!(shared_chat_id(&odd), None);
        assert_eq!(ChatInfo::new(&odd, "9".into()).ext_kind(), Some("channel"));
    }

    #[test]
    fn a_person_says_only_what_the_gateway_knows() {
        let p: Person = serde_json::from_value(json!({"id": "9", "name": "Grace", "email": "Grace@Example.com",
            "tenant_id": "t1", "avatar_url": "https://cdn.example.com/a.png", "description": "x"}))
        .unwrap();
        let v = person(&p, Some(&me()));
        assert_eq!(v["user"], json!({"kind": "id", "value": "9"}));
        assert_eq!(v["email"], "grace@example.com");
        assert_eq!(v["directory"], true);
        assert_eq!((v["title"].clone(), v["org"].clone()), (Value::Null, Value::Null));
        let p: Person = serde_json::from_value(json!({"id": "9", "tenant_id": "t2", "external": true})).unwrap();
        assert_eq!(person(&p, Some(&me()))["directory"], false);
    }
}
