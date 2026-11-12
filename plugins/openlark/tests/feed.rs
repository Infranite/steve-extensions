//! The change feed (§5; vectors V4 to V7): what is news, the window, redelivery, cursors, and
//! the gateway's journal as the Hub's items.

mod common;

use std::time::Duration;

use common::*;
use serde_json::{Value, json};
use steve_plugin_openlark::config::Timing;

fn texts(items: &[Value]) -> Vec<&str> {
    items.iter().map(|i| i["message"]["text"].as_str().unwrap()).collect()
}

async fn resume(hub: &mut Hub, cursor: &Value) -> Result<Value, Value> {
    hub.call("changes.subscribe", json!({"from": {"cursor": cursor}})).await
}

/// V4.
#[tokio::test]
async fn from_now_only_what_happens_after_is_news() {
    let rig = Rig::start().await;
    rig.fake().post(GROUP, GRACE, "said before anybody listened");
    let mut hub = rig.hub().await;
    let before = now_ms();
    let sub = hub.subscribe_now().await;
    assert_eq!(sub["subscription"], "s1");
    assert_eq!(sub["resumed"], false);
    assert_eq!(sub["next_seq"], 2);
    let watermark = sub["watermark_ms"].as_i64().unwrap();
    assert!((before..=now_ms()).contains(&watermark));

    // a change that arrives now about a message from before the watermark is history
    rig.fake().post_at(GROUP, GRACE, "written a minute ago", watermark - 60_000);
    let posted = rig.fake().post(GROUP, GRACE, "hello there");
    let batch = hub.batch_with_items().await;
    assert_eq!(batch["subscription"], "s1");
    assert_eq!(batch["seq"], 3);
    let cursor = batch["cursor"].as_str().unwrap();
    assert!(!cursor.is_empty() && cursor.len() <= 1024);
    let items = batch["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let message = json!({
        "chat_id": GROUP_OC,
        "source_id": posted["id"],
        "id": null,
        "created_ms": posted["created_ms"],
        "sender": {"id": id_handle(GRACE), "display": "Grace", "is_bot": false, "external": false, "aliases": []},
        "is_mine": false,
        "chat_kind": "group",
        "thread_id": null,
        "edited_ms": null,
        "chat_name": "Design",
        "reply_to": null,
        "root_id": null,
        "kind": "text",
        "text": "hello there",
        "mentions": [],
        "attachments": [],
        "recalled": false,
        "raw": {},
    });
    let mut item = message.clone();
    for field in
        ["edited_ms", "chat_name", "reply_to", "root_id", "kind", "text", "mentions", "attachments", "recalled", "raw"]
    {
        item.as_object_mut().unwrap().remove(field);
    }
    item["type"] = "message".into();
    item["message"] = message.clone();
    assert_eq!(items[0], item);
    // the body read on its own is the body the feed carried
    let read = hub.call("messages.get", json!({"items": [{"chat_id": GROUP_OC, "source_id": posted["id"]}]})).await;
    assert_eq!(read.unwrap()["messages"], json!([message]));
    assert!(!hub.gets("changes.batch", Duration::from_millis(500)).await);
}

#[tokio::test]
async fn my_own_message_comes_back_under_the_id_the_send_returned() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    let send =
        json!({"chat_id": GROUP_OC, "text": "on it", "idempotency_key": "k-1", "expected_sender": id_handle(ME)});
    let sent = hub.call("messages.send", send).await.unwrap();
    let source_id = sent["source_id"].as_str().unwrap();
    let item = hub.items(1).await.remove(0);
    assert_eq!(item["type"], "message");
    assert_eq!(item["source_id"], source_id);
    assert_eq!(item["chat_id"], GROUP_OC);
    assert_eq!(item["is_mine"], true);
    assert_eq!(item["created_ms"], sent["created_ms"]);
    // my own messages carry the handles me.get gives
    let me = hub.call("me.get", json!({})).await.unwrap();
    assert_eq!(item["sender"]["id"], me["user"]["id"]);
    assert_eq!(item["sender"]["aliases"], me["user"]["aliases"]);
    let read = hub.call("messages.get", json!({"items": [{"chat_id": GROUP_OC, "source_id": source_id}]})).await;
    let read = read.unwrap()["messages"][0].clone();
    assert_eq!((read["source_id"].as_str(), read["is_mine"].as_bool()), (Some(source_id), Some(true)));
    assert_eq!(read, item["message"]);
    let now = now_ms();
    let page = hub
        .call("messages.history", json!({"chat_id": GROUP_OC, "from_ms": now - 60_000, "to_ms": now + 60_000}))
        .await
        .unwrap();
    assert_eq!(page["messages"], json!([read]));
}

/// §5.4: no more than `max_unacked_batches` are on their way at once.
#[tokio::test]
async fn the_window_holds_the_feed_until_the_hub_acks() {
    let timing = Timing { ack_timeout: Duration::from_secs(4), ..fast() };
    let rig = Rig::custom(|w| w.batch_max = 1, timing).await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    for i in 0..10 {
        rig.fake().post(GROUP, GRACE, &format!("m{i}"));
    }
    let mut seen: Vec<Value> = Vec::new();
    for expected in [4, 4, 2] {
        let batches = hub.batches_for(Duration::from_millis(500)).await;
        assert_eq!(batches.len(), expected, "after {} batches", seen.len());
        // one ack for the last of them takes them all
        hub.ack(batches.last().unwrap()).await.unwrap();
        seen.extend(batches);
    }
    let seqs: Vec<u64> = seen.iter().map(|b| b["seq"].as_u64().unwrap()).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");
    let items: Vec<Value> = seen.iter().flat_map(|b| b["items"].as_array().unwrap().clone()).collect();
    assert_eq!(texts(&items), ["m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"]);
    assert!(hub.batches_for(Duration::from_millis(300)).await.is_empty());
}

/// V5.
#[tokio::test]
async fn an_unacked_batch_is_sent_again_unchanged() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().post(GROUP, GRACE, "a");
    let sent = tokio::time::Instant::now();
    let first = hub.batch().await;
    let again = hub.batch().await;
    assert!(sent.elapsed() >= Duration::from_millis(250));
    assert_eq!(again, first);
    let seq = first["seq"].as_u64().unwrap();
    assert_eq!(hub.ack(&first).await.unwrap(), json!({}));
    assert!(hub.batches_for(Duration::from_millis(800)).await.is_empty());
    // the gateway learns how far the Hub is
    assert_eq!(rig.fake().w().acks, [seq]);

    // acks are cumulative and repeatable; one for a batch never sent is a mistake
    let sub = &first["subscription"];
    assert_eq!(hub.call("changes.ack", json!({"subscription": sub, "seq": seq})).await.unwrap(), json!({}));
    assert_eq!(hub.call("changes.ack", json!({"subscription": sub, "seq": 0})).await.unwrap(), json!({}));
    let above = hub.call("changes.ack", json!({"subscription": sub, "seq": seq + 5})).await.unwrap_err();
    assert_eq!(reason(&above), "invalid");
    assert_eq!(hub.call("changes.ack", json!({"subscription": "zz", "seq": 99})).await.unwrap(), json!({}));
    assert_eq!(reason(&hub.call("changes.ack", json!({"subscription": sub})).await.unwrap_err()), "invalid");
    assert_eq!(reason(&hub.call("changes.ack", json!({"seq": seq})).await.unwrap_err()), "invalid");
    // and the feed goes on
    rig.fake().post(GROUP, GRACE, "b");
    assert_eq!(texts(&hub.items(1).await), ["b"]);
    hub.call("ping", json!({})).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(rig.fake().w().acks.len(), 2);
}

/// V6.
#[tokio::test]
async fn a_cursor_resumes_without_loss_or_repeat() {
    let mut rig = Rig::start().await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().post(GROUP, GRACE, "a");
    let batch = hub.batch_with_items().await;
    let (seq, cursor) = (batch["seq"].as_u64().unwrap(), batch["cursor"].clone());
    drop(hub);

    // the Hub is away while two messages arrive, one of them older than its return
    rig.fake().post(GROUP, GRACE, "b");
    rig.fake().post(DM, GRACE, "c");
    let mut hub = rig.hub().await;
    let sub = resume(&mut hub, &cursor).await.unwrap();
    assert_eq!(sub["resumed"], true);
    assert_eq!(sub["next_seq"], seq + 1);
    assert!(sub["watermark_ms"].is_i64());
    let batch = hub.batch_with_items().await;
    assert!(batch["seq"].as_u64().unwrap() > seq);
    let items = batch["items"].as_array().unwrap();
    assert_eq!(texts(items), ["b", "c"]);
    assert_eq!((items[0]["chat_id"].as_str(), items[1]["chat_id"].as_str()), (Some(GROUP_OC), Some(DM)));
    assert_eq!(items[1]["chat_kind"], "dm");
    let later = batch["cursor"].clone();
    drop(hub);

    // the same cursor again gives the same again: a cursor is a position, not a receipt
    let mut hub = rig.hub().await;
    resume(&mut hub, &cursor).await.unwrap();
    assert_eq!(texts(&hub.items(2).await), ["b", "c"]);
    drop(hub);

    // the plugin itself restarts: the cursor still stands
    rig.stop_bridge().await;
    rig.fake().post(GROUP, GRACE, "d");
    rig.start_bridge().await;
    let mut hub = rig.hub().await;
    let sub = resume(&mut hub, &later).await.unwrap();
    assert_eq!(sub["resumed"], true);
    assert_eq!(texts(&hub.items(1).await), ["d"]);
    assert!(hub.batches_for(Duration::from_millis(300)).await.is_empty());
}

/// V7.
#[tokio::test]
async fn a_cursor_the_gateway_cannot_serve_is_refused_by_name() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().post(GROUP, GRACE, "a");
    let batch = hub.batch_with_items().await;
    let cursor = batch["cursor"].as_str().unwrap().to_string();
    rig.fake().post(GROUP, GRACE, "b");
    rig.fake().post(GROUP, GRACE, "c");
    hub.items(2).await;

    // the gateway discarded what follows the cursor
    rig.fake().w().prune_through(2);
    assert_eq!(reason(&resume(&mut hub, &json!(cursor)).await.unwrap_err()), "cursor_too_old");
    rig.fake().w().prune_through(3);
    assert_eq!(reason(&resume(&mut hub, &json!(cursor)).await.unwrap_err()), "cursor_too_old");
    // a cursor at the head has lost nothing, whatever was discarded
    let head = cursor.replacen(":1:", ":3:", 1);
    assert_eq!(resume(&mut hub, &json!(head)).await.unwrap()["next_seq"], 4);

    // a cursor this server did not issue, or a position its gateway never reached
    let beyond = cursor.replacen(":1:", ":999:", 1);
    for unknown in ["c2:whatever", "g1:00000000:1:5", beyond.as_str()] {
        assert_eq!(reason(&resume(&mut hub, &json!(unknown)).await.unwrap_err()), "unknown_cursor", "{unknown}");
    }
    for invalid in [json!(5), json!({"cursor": ""}), json!({"cursor": 7}), json!("then"), json!({})] {
        let refused = hub.call("changes.subscribe", json!({"from": invalid})).await.unwrap_err();
        assert_eq!(reason(&refused), "invalid", "{invalid}");
    }
    assert_eq!(reason(&hub.call("changes.subscribe", json!({})).await.unwrap_err()), "invalid");

    // the Hub's repair is to subscribe from now
    let sub = hub.subscribe_now().await;
    assert_eq!(sub["resumed"], false);
    rig.fake().post(GROUP, GRACE, "d");
    assert_eq!(texts(&hub.items(1).await), ["d"]);
}

#[tokio::test]
async fn every_kind_of_change_is_the_item_it_should_be() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    let first = rig.fake().post(GROUP, GRACE, "a");
    let id = first["id"].as_str().unwrap();
    assert_eq!(hub.items(1).await[0]["source_id"], id);
    let started = now_ms();

    let fake = rig.fake();
    fake.append(json!({"kind": "message", "op": "recall", "message_id": id, "chat": GROUP}));
    let mut edited = first.clone();
    edited["edited_ms"] = (started + 5).into();
    edited["content"]["text"] = "a, corrected".into();
    fake.append(json!({"kind": "message", "op": "update", "message": edited}));
    // an update that is not an edit (a reaction, say) is nothing the Hub is told
    fake.append(json!({"kind": "message", "op": "update", "message": first}));
    fake.append(json!({"kind": "message", "op": "delete", "message_id": id, "chat": GROUP}));
    // a change of a kind this bridge has never heard of
    fake.append(json!({"kind": "calendar", "op": "upsert", "event": {"id": "1"}}));
    // the gateway could not attach the body, but it can still be read
    let late = fake.w().store(DM, GRACE, "too big to attach", now_ms());
    fake.append(
        json!({"kind": "message", "op": "upsert", "message_id": late["id"], "chat": DM, "body": "unavailable"}),
    );
    // or it cannot: that chat has to be read again
    fake.append(
        json!({"kind": "message", "op": "upsert", "message_id": "99999", "chat": GROUP, "body": "unavailable"}),
    );
    // the gateway's own gaps, of one chat and of all
    fake.append(json!({"kind": "gap", "from": 3, "to": 4, "chats": [DM]}));
    fake.append(json!({"kind": "gap", "from": 3, "to": 4, "chats": []}));
    // a message that arrives already recalled
    let mut gone = fake.w().store(GROUP, GRACE, "", now_ms());
    gone["recalled"] = true.into();
    fake.append(json!({"kind": "message", "op": "upsert", "message": gone}));

    let items = hub.items(7).await;
    assert_eq!(items[0], json!({"type": "message_recalled", "chat_id": GROUP_OC, "source_id": id}));
    assert_eq!(
        items[1],
        json!({"type": "message_edited", "chat_id": GROUP_OC, "source_id": id, "edited_ms": started + 5})
    );
    assert_eq!(items[2]["type"], "message");
    assert_eq!(items[2]["message"]["text"], "too big to attach");
    assert_eq!(items[2]["chat_id"], DM);
    let gap = |item: &Value, chats: Value| {
        assert_eq!(item["type"], "gap", "{item}");
        assert_eq!(item["chats"], chats);
        let (from_ms, to_ms) = (item["from_ms"].as_i64().unwrap(), item["to_ms"].as_i64().unwrap());
        assert!(from_ms <= started && started <= to_ms && to_ms <= now_ms(), "{item}");
        assert!(item["reason"].is_string());
    };
    gap(&items[3], json!([GROUP_OC]));
    gap(&items[4], json!([DM]));
    gap(&items[5], json!(["*"]));
    assert_eq!(items[6], json!({"type": "message_recalled", "chat_id": GROUP_OC, "source_id": gone["id"]}));
    assert!(hub.batches_for(Duration::from_millis(300)).await.is_empty());
}

#[tokio::test]
async fn the_feed_survives_the_gateway_connection() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().post(GROUP, GRACE, "a");
    assert_eq!(texts(&hub.items(1).await), ["a"]);
    // every connection to the gateway ends; what is posted meanwhile is not lost
    rig.fake().kick();
    rig.fake().post(GROUP, GRACE, "b");
    assert_eq!(texts(&hub.items(1).await), ["b"]);
    rig.fake().kick();
    tokio::time::sleep(Duration::from_millis(400)).await;
    rig.fake().post(GROUP, GRACE, "c");
    assert_eq!(texts(&hub.items(1).await), ["c"]);
    // and nothing came twice
    assert!(hub.batches_for(Duration::from_millis(500)).await.is_empty());
}

#[tokio::test]
async fn a_gateway_that_lost_its_journal_makes_the_socket_start_over() {
    let mut rig = Rig::start().await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().post(GROUP, GRACE, "a");
    rig.fake().post(GROUP, GRACE, "b");
    let cursor = hub.batch_with_items().await["cursor"].clone();

    rig.gw.stop().await;
    {
        let mut w = rig.fake().w();
        w.journal.clear();
        w.head = 0;
    }
    rig.gw.restart().await;
    assert_eq!(hub.until_closed().await.0, Some(1012));
    let mut hub = rig.hub().await;
    assert_eq!(reason(&resume(&mut hub, &cursor).await.unwrap_err()), "unknown_cursor");
    assert_eq!(hub.subscribe_now().await["next_seq"], 1);
    rig.fake().post(GROUP, GRACE, "c");
    assert_eq!(texts(&hub.items(1).await), ["c"]);
}

#[tokio::test]
async fn a_new_subscription_replaces_the_old_one() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    assert_eq!(hub.subscribe_now().await["subscription"], "s1");
    rig.fake().post(GROUP, GRACE, "a");
    let old = hub.batch().await;
    assert_eq!(hub.subscribe_now().await["subscription"], "s2");
    // an ack that was on its way for the old one does no harm
    assert_eq!(hub.ack(&old).await.unwrap(), json!({}));
    rig.fake().post(GROUP, GRACE, "b");
    let batch = hub.note_where("changes.batch", |b| b["subscription"] == "s2").await;
    assert_eq!(texts(batch["items"].as_array().unwrap()), ["b"]);
    hub.ack(&batch).await.unwrap();
    // nothing of the old subscription is sent again
    assert!(hub.batches_for(Duration::from_millis(700)).await.is_empty());
}

#[tokio::test]
async fn a_cursor_of_another_account_is_unknown() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().post(GROUP, GRACE, "a");
    let cursor = hub.batch_with_items().await["cursor"].clone();
    rig.fake().w().me = GRACE.into();
    assert_eq!(hub.until_closed().await.0, Some(1012));
    let mut hub = rig.hub().await;
    assert_eq!(reason(&resume(&mut hub, &cursor).await.unwrap_err()), "unknown_cursor");
}

/// §6.1: a chat keeps the id it was first announced under.
#[tokio::test]
async fn a_chat_keeps_the_id_it_was_announced_under() {
    // the gateway does not know the group's shared id yet
    let mut rig = Rig::with(|w| {
        w.oc_missing.insert(GROUP.into(), usize::MAX);
    })
    .await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().post(GROUP, GRACE, "a");
    assert_eq!(hub.items(1).await[0]["chat_id"], GROUP);
    // it learns it later; the Hub keeps hearing the id it was given
    rig.fake().w().oc_missing.clear();
    rig.fake().post(GROUP, GRACE, "b");
    assert_eq!(hub.items(1).await[0]["chat_id"], GROUP);
    assert_eq!(hub.call("chats.get", json!({"chat_id": GROUP})).await.unwrap()["chat_id"], GROUP);
    assert_eq!(reason(&hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap_err()), "not_found");
    drop(hub);

    rig.stop_bridge().await;
    rig.start_bridge().await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().post(GROUP, GRACE, "c");
    let item = hub.items(1).await.remove(0);
    assert_eq!(item["chat_id"], GROUP);
    assert_eq!(item["message"]["chat_name"], "Design");
    assert_eq!(reason(&hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap_err()), "not_found");
    let send = json!({"chat_id": GROUP, "text": "x", "idempotency_key": "k", "expected_sender": id_handle(ME)});
    hub.call("messages.send", send).await.unwrap();
}

#[tokio::test]
async fn a_shared_id_that_is_a_moment_late_is_waited_for() {
    let rig = Rig::with(|w| {
        w.oc_missing.insert(GROUP.into(), 1);
    })
    .await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().post(GROUP, GRACE, "a");
    assert_eq!(hub.items(1).await[0]["chat_id"], GROUP_OC);
    assert_eq!(rig.fake().w().called("chats.get").len(), 2);
}
