//! Writes as the signed-in user (§4.4, §4.5; vectors V10 and V11).

mod common;

use common::*;
use serde_json::{Value, json};

fn send(chat: &str, text: &str, key: &str) -> Value {
    json!({"chat_id": chat, "text": text, "idempotency_key": key, "expected_sender": id_handle(ME)})
}

#[tokio::test]
async fn a_send_goes_out_as_the_signed_in_user() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    let before = now_ms();
    let sent = hub.call("messages.send", send(GROUP_OC, "hello", "send-1")).await.unwrap();
    let stored = rig.fake().w().messages.last().unwrap().clone();
    assert_eq!(
        sent,
        json!({"source_id": stored["id"], "id": null, "created_ms": stored["created_ms"], "thread_id": null})
    );
    assert!(sent["created_ms"].as_i64().unwrap() >= before);
    assert_eq!((stored["chat"].as_str(), stored["content"]["text"].as_str()), (Some(GROUP), Some("hello")));
    assert_eq!(
        rig.fake().w().called("messages.send"),
        [json!({
            "chat": GROUP,
            "content": {"type": "text", "text": "hello"},
            "idempotency_key": "send-1",
            "expected_sender": ME,
        })]
    );

    // into a direct chat, which is known by the gateway's id only; with a mention
    let mut to_grace = send(DM, "see this", "send-2");
    to_grace["mentions"] = json!([id_handle(GRACE)]);
    hub.call("messages.send", to_grace).await.unwrap();
    let call = rig.fake().w().called("messages.send")[1].clone();
    assert_eq!((call["chat"].as_str(), &call["mentions"]), (Some(DM), &json!([{"user": GRACE}])));
    let read = hub.call("messages.get", json!({"items": [{"chat_id": DM, "source_id": "502"}]})).await.unwrap();
    assert_eq!(read["messages"][0]["mentions"], json!([{"user": id_handle(GRACE), "name": "", "is_me": false}]));

    // a key longer than the gateway takes reaches it as a hash, the same one each time
    let long = "k".repeat(100);
    let first = hub.call("messages.send", send(GROUP_OC, "long key", &long)).await.unwrap();
    let key = rig.fake().w().called("messages.send")[2]["idempotency_key"].as_str().unwrap().to_string();
    assert!(key.starts_with("h:") && key.len() == 64, "{key}");
    assert_eq!(hub.call("messages.send", send(GROUP_OC, "long key", &long)).await.unwrap(), first);
    assert_eq!(rig.fake().w().called("messages.send").len(), 3);
}

/// V11.
#[tokio::test]
async fn a_repeated_key_sends_once() {
    let mut rig = Rig::start().await;
    let mut hub = rig.hub().await;
    let first = hub.call("messages.send", send(GROUP_OC, "once", "key-1")).await.unwrap();
    assert_eq!(hub.call("messages.send", send(GROUP_OC, "once", "key-1")).await.unwrap(), first);
    let other = hub.call("messages.send", send(GROUP_OC, "something else", "key-1")).await.unwrap_err();
    assert_eq!(reason(&other), "idempotency_conflict");
    let elsewhere = hub.call("messages.send", send(DM, "once", "key-1")).await.unwrap_err();
    assert_eq!(reason(&elsewhere), "idempotency_conflict");
    // the key belongs to the method too
    let mut reply = send(GROUP_OC, "once", "key-1");
    reply["to"] = json!({"source_id": first["source_id"]});
    assert_eq!(reason(&hub.call("messages.reply", reply).await.unwrap_err()), "idempotency_conflict");
    drop(hub);

    // the plugin restarts and the Hub retries: still once
    rig.stop_bridge().await;
    rig.start_bridge().await;
    let mut hub = rig.hub().await;
    assert_eq!(hub.call("messages.send", send(GROUP_OC, "once", "key-1")).await.unwrap(), first);
    let other = hub.call("messages.send", send(GROUP_OC, "something else", "key-1")).await.unwrap_err();
    assert_eq!(reason(&other), "idempotency_conflict");
    assert_eq!(rig.fake().w().called("messages.send").len(), 1);
    assert_eq!(rig.fake().w().messages.len(), 1);
    // another key is another message
    let second = hub.call("messages.send", send(GROUP_OC, "once", "key-2")).await.unwrap();
    assert_ne!(second["source_id"], first["source_id"]);
}

/// V10.
#[tokio::test]
async fn a_write_for_another_user_is_refused_before_anything_happens() {
    let rig = Rig::start().await;
    let target = rig.fake().w().store(GROUP, GRACE, "a", 1_000);
    let mut hub = rig.hub().await;
    let target = json!({"source_id": target["id"]});
    let writes = |sender: Value| {
        [
            ("messages.send", json!({"chat_id": GROUP_OC, "text": "x", "idempotency_key": "k1", "expected_sender": sender})),
            (
                "messages.reply",
                json!({"chat_id": GROUP_OC, "to": target, "text": "x", "idempotency_key": "k2", "expected_sender": sender}),
            ),
            ("messages.recall", json!({"chat_id": GROUP_OC, "target": target, "expected_sender": sender})),
            ("reactions.add", json!({"chat_id": GROUP_OC, "target": target, "emoji": "👀", "expected_sender": sender})),
            (
                "reactions.remove",
                json!({"chat_id": GROUP_OC, "target": target, "reaction_id": "OnIt", "expected_sender": sender}),
            ),
        ]
    };
    let others = [id_handle("999"), id_handle(GRACE), json!({"kind": "email", "value": "ada@example.com"})];
    for other in others {
        for (method, params) in writes(other.clone()) {
            let refused = hub.call(method, params).await.unwrap_err();
            assert_eq!(reason(&refused), "account_changed", "{method} as {other}");
        }
    }
    for (method, mut params) in writes(Value::Null) {
        params.as_object_mut().unwrap().remove("expected_sender");
        assert_eq!(reason(&hub.call(method, params).await.unwrap_err()), "invalid", "{method}");
    }
    {
        let w = rig.fake().w();
        for method in ["messages.send", "messages.reply", "messages.recall", "reactions.add", "reactions.remove"] {
            assert!(w.called(method).is_empty(), "{method} reached the gateway");
        }
        assert_eq!(w.messages.len(), 1);
    }
    // the refused keys were not used up
    for (method, params) in writes(id_handle(ME)) {
        hub.call(method, params).await.unwrap_or_else(|e| panic!("{method}: {e}"));
    }

    // the account changes under a Hub that has not noticed yet: the gateway itself refuses
    rig.fake().w().fail.insert("messages.send".into(), ("sender_changed".into(), None));
    let refused = hub.call("messages.send", send(GROUP_OC, "x", "k9")).await.unwrap_err();
    assert_eq!(reason(&refused), "account_changed");
}

#[tokio::test]
async fn a_reply_names_its_parent_and_may_open_a_thread() {
    let rig = Rig::start().await;
    let parent = rig.fake().w().store(GROUP, GRACE, "question", 1_000);
    let mut hub = rig.hub().await;
    let mut reply = send(GROUP_OC, "answer", "reply-1");
    reply["to"] = json!({"source_id": parent["id"]});
    let plain = hub.call("messages.reply", reply.clone()).await.unwrap();
    assert_eq!(plain["thread_id"], Value::Null);
    assert_eq!(
        rig.fake().w().called("messages.reply"),
        [json!({
            "message": parent["id"],
            "in_thread": false,
            "content": {"type": "text", "text": "answer"},
            "idempotency_key": "reply-1",
            "expected_sender": ME,
        })]
    );
    reply["in_thread"] = true.into();
    reply["idempotency_key"] = "reply-2".into();
    let threaded = hub.call("messages.reply", reply.clone()).await.unwrap();
    assert_eq!(threaded["thread_id"], parent["id"]);
    let read = hub
        .call("messages.get", json!({"items": [{"chat_id": GROUP_OC, "source_id": threaded["source_id"]}]}))
        .await
        .unwrap();
    let read = &read["messages"][0];
    assert_eq!(read["reply_to"], json!({"source_id": parent["id"], "id": null}));
    assert_eq!((&read["thread_id"], read["is_mine"].as_bool()), (&parent["id"], Some(true)));

    // a parent that is not there, or one named in a way this server cannot follow
    reply["idempotency_key"] = "reply-3".into();
    reply["to"] = json!({"source_id": "404404"});
    assert_eq!(reason(&hub.call("messages.reply", reply.clone()).await.unwrap_err()), "not_found");
    reply["to"] = json!({"id": "om_abc"});
    assert_eq!(reason(&hub.call("messages.reply", reply.clone()).await.unwrap_err()), "not_found");
    reply["to"] = json!({});
    assert_eq!(reason(&hub.call("messages.reply", reply.clone()).await.unwrap_err()), "invalid");
    reply.as_object_mut().unwrap().remove("to");
    assert_eq!(reason(&hub.call("messages.reply", reply).await.unwrap_err()), "invalid");
    assert_eq!(rig.fake().w().called("messages.reply").len(), 3);
}

#[tokio::test]
async fn a_send_that_cannot_go_out_says_why() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    let invalid = [
        send(GROUP_OC, "", "k1"),
        send(GROUP_OC, "  \n", "k1"),
        send(GROUP_OC, "x", ""),
        send(GROUP_OC, "x", "a key with spaces"),
        send(GROUP_OC, "x", &"k".repeat(129)),
        json!({"chat_id": GROUP_OC, "idempotency_key": "k1", "expected_sender": id_handle(ME)}),
        json!({"text": "x", "idempotency_key": "k1", "expected_sender": id_handle(ME)}),
        json!({"chat_id": GROUP_OC, "text": "x", "expected_sender": id_handle(ME)}),
        json!({"chat_id": GROUP_OC, "text": "x", "idempotency_key": "k1", "expected_sender": id_handle(ME),
            "mentions": [{"kind": "email", "value": "grace@example.com"}]}),
    ];
    for params in invalid {
        let refused = hub.call("messages.send", params.clone()).await.unwrap_err();
        assert_eq!(reason(&refused), "invalid", "{params}");
    }
    // one byte more than max_text_bytes
    let big = "a".repeat(256 * 1024 + 1);
    assert_eq!(reason(&hub.call("messages.send", send(GROUP_OC, &big, "k1")).await.unwrap_err()), "too_large");
    assert_eq!(reason(&hub.call("messages.send", send("oc_nope", "x", "k1")).await.unwrap_err()), "not_found");
    assert!(rig.fake().w().called("messages.send").is_empty());

    // the gateway's write policy does not allow this chat
    rig.fake().w().forbidden_chats.insert(DM.into());
    assert_eq!(reason(&hub.call("messages.send", send(DM, "x", "k1")).await.unwrap_err()), "target_forbidden");
    // the gateway paces writes, and says for how long
    rig.fake().w().fail.insert("messages.send".into(), ("rate_limited".into(), Some(1500)));
    let paced = hub.call("messages.send", send(GROUP_OC, "x", "k1")).await.unwrap_err();
    assert_eq!((reason(&paced), &paced["data"]["retry_after_ms"]), ("rate_limited", &json!(1500)));
    rig.fake().w().fail.insert("messages.send".into(), ("upstream".into(), None));
    let down = hub.call("messages.send", send(GROUP_OC, "x", "k1")).await.unwrap_err();
    assert_eq!(reason(&down), "upstream_unavailable");
    // none of these used the key up
    rig.fake().w().fail.clear();
    hub.call("messages.send", send(GROUP_OC, "x", "k1")).await.unwrap();
    assert_eq!(rig.fake().w().messages.len(), 1);
}

#[tokio::test]
async fn a_recall_takes_the_message_back() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    let sent = hub.call("messages.send", send(GROUP_OC, "oops", "k1")).await.unwrap();
    let target = json!({"source_id": sent["source_id"]});
    let recall = json!({"chat_id": GROUP_OC, "target": target, "expected_sender": id_handle(ME)});
    assert_eq!(hub.call("messages.recall", recall.clone()).await.unwrap(), json!({}));
    assert_eq!(rig.fake().w().called("messages.recall"), [json!({"message": sent["source_id"]})]);
    let read = hub.call("messages.get", json!({"items": [{"chat_id": GROUP_OC, "source_id": sent["source_id"]}]})).await;
    let read = read.unwrap()["messages"][0].clone();
    assert_eq!((read["recalled"].as_bool(), read["text"].as_str()), (Some(true), Some("")));

    let mut missing = recall.clone();
    missing["target"] = json!({"source_id": "404404"});
    assert_eq!(reason(&hub.call("messages.recall", missing).await.unwrap_err()), "not_found");
    let mut unnamed = recall.clone();
    unnamed.as_object_mut().unwrap().remove("target");
    assert_eq!(reason(&hub.call("messages.recall", unnamed).await.unwrap_err()), "invalid");
    // the gateway refuses (somebody else's message, or too late)
    rig.fake().w().fail.insert("messages.recall".into(), ("forbidden".into(), None));
    assert_eq!(reason(&hub.call("messages.recall", recall).await.unwrap_err()), "target_forbidden");
}

#[tokio::test]
async fn the_three_reactions_steve_uses_are_lark_reactions() {
    let rig = Rig::start().await;
    let message = rig.fake().w().store(GROUP, GRACE, "please look", 1_000);
    let id = message["id"].as_str().unwrap();
    let mut hub = rig.hub().await;
    let react = |emoji: &str| {
        json!({"chat_id": GROUP_OC, "target": {"source_id": id}, "emoji": emoji, "expected_sender": id_handle(ME)})
    };
    for (emoji, key) in [("👀", "OnIt"), ("✅", "DONE"), ("❌", "CrossMark"), ("✅\u{fe0f}", "DONE")] {
        assert_eq!(hub.call("reactions.add", react(emoji)).await.unwrap(), json!({"reaction_id": key}), "{emoji}");
    }
    assert_eq!(rig.fake().w().called("reactions.add")[0], json!({"message": id, "emoji": "OnIt"}));
    // any other emoji has no Lark reaction this server knows
    assert_eq!(reason(&hub.call("reactions.add", react("🎉")).await.unwrap_err()), "unsupported");
    assert_eq!(rig.fake().w().called("reactions.add").len(), 4);

    let remove = |reaction_id: Value| {
        json!({"chat_id": GROUP_OC, "target": {"source_id": id}, "reaction_id": reaction_id,
            "expected_sender": id_handle(ME)})
    };
    assert_eq!(hub.call("reactions.remove", remove("OnIt".into())).await.unwrap(), json!({}));
    assert_eq!(rig.fake().w().called("reactions.remove"), [json!({"message": id, "emoji": "OnIt"})]);
    assert!(!rig.fake().w().reactions.contains(&(id.to_string(), "OnIt".to_string())));
    assert!(rig.fake().w().reactions.contains(&(id.to_string(), "CrossMark".to_string())));
    assert_eq!(reason(&hub.call("reactions.remove", remove(Value::Null)).await.unwrap_err()), "invalid");
    assert_eq!(reason(&hub.call("reactions.remove", remove("not a key".into())).await.unwrap_err()), "invalid");
    let mut elsewhere = react("👀");
    elsewhere["target"] = json!({"source_id": "404404"});
    assert_eq!(reason(&hub.call("reactions.add", elsewhere).await.unwrap_err()), "not_found");
}
