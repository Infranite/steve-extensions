//! The optional gateway reaction contract through the running bridge's real WebSocket endpoints.
mod common;

use std::time::Duration;

use common::*;
use serde_json::{Value, json};

fn enable(world: &mut World) {
    world.features.push("reaction_changes.v1".into());
}

fn change(target: &str, actor: &str, action: &str, origin: Option<&str>, at: i64, time_source: &str) -> Value {
    let mut action = json!({"actor":{"id":actor},"emoji":"THUMBSUP","change":action,
        "action_ms":at,"time_source":time_source});
    if let Some(origin) = origin {
        action["origin"] = origin.into();
    }
    json!({"kind":"reaction","message_id":target,"chat":GROUP,"reaction":action})
}

#[tokio::test]
async fn a_new_action_on_an_old_message_keeps_its_target_identity_and_time() {
    let rig = Rig::with(enable).await;
    let target = rig.fake().w().store(GROUP, GRACE, "old question", now_ms() - 86_400_000);
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    let at = now_ms() - 120_000;
    let seq = rig.fake().append(change(target["id"].as_str().unwrap(), ME, "created", Some("manual"), at, "provider"));
    let item = hub.items(1).await.remove(0);
    assert_eq!(item["type"], "reaction");
    assert_eq!(item["chat_id"], GROUP_OC);
    assert_eq!(item["source_id"], target["id"]);
    assert_eq!(item["actor"]["id"], id_handle(ME));
    assert_eq!(
        item["actor"]["aliases"],
        json!([
            {"kind":"email","value":"ada@example.com"},
            {"kind":"platform","system":"feishu","value":"on_ada"}
        ])
    );
    assert_eq!(item["origin"], "manual");
    assert_eq!(item["action_ms"], at);
    assert_eq!(item["time_source"], "provider");
    assert_eq!(item["revision"], seq);
    assert!(item.get("id").is_none());
    let calls = rig.fake().w();
    assert_eq!(calls.called("changes.subscribe").last().unwrap()["kinds"], json!(["message", "reaction"]));
    assert!(calls.called("reactions.list").is_empty());
    assert!(calls.called("messages.get").is_empty(), "target age does not require hydration");
}

#[tokio::test]
async fn origins_and_same_millisecond_remove_readd_revisions_survive_the_bridge() {
    let rig = Rig::with(enable).await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    let at = now_ms();
    for (action, origin) in [("created", Some("manual")), ("removed", Some("automated")), ("created", None)] {
        rig.fake().append(change("501", ME, action, origin, at, "observed"));
    }
    let items = hub.items(3).await;
    assert_eq!(
        items.iter().map(|i| i["change"].clone()).collect::<Vec<_>>(),
        json!(["created", "removed", "created"]).as_array().unwrap().clone()
    );
    assert_eq!(
        items.iter().map(|i| i["origin"].clone()).collect::<Vec<_>>(),
        json!(["manual", "automated", "unknown"]).as_array().unwrap().clone()
    );
    assert_eq!(items.iter().map(|i| i["revision"].as_u64().unwrap()).collect::<Vec<_>>(), [1, 2, 3]);
    assert!(items.iter().all(|i| i["action_ms"] == at && i["time_source"] == "observed"));
    assert!(
        items.iter().all(|i| i["actor"]["is_bot"] == false),
        "automation on a human account does not change its identity"
    );
}

#[tokio::test]
async fn an_unacked_reaction_is_redelivered_without_changing_its_revision_or_observation_time() {
    let rig = Rig::with(enable).await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().append(change("501", ME, "created", Some("unknown"), now_ms(), "observed"));
    let first = hub.batch().await;
    let again = hub.batch().await;
    assert_eq!(again, first);
    hub.ack(&again).await.unwrap();
    assert!(hub.batches_for(Duration::from_millis(350)).await.is_empty());
}

#[tokio::test]
async fn reaction_cursor_survives_plugin_restart_and_gateway_reconnect() {
    let mut rig = Rig::with(enable).await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    let at = now_ms();
    rig.fake().append(change("501", ME, "created", Some("manual"), at, "provider"));
    let first = hub.batch_with_items().await;
    hub.ack(&first).await.unwrap();
    drop(hub);
    rig.stop_bridge().await;
    rig.fake().append(change("501", ME, "removed", Some("manual"), at + 1, "provider"));
    rig.start_bridge().await;
    let mut hub = rig.hub().await;
    let sub = hub.call("changes.subscribe", json!({"from":{"cursor":first["cursor"]}})).await.unwrap();
    assert_eq!(sub["resumed"], true);
    let removed = hub.batch_with_items().await;
    assert_eq!(removed["items"][0]["change"], "removed");
    assert_eq!(removed["items"][0]["revision"], 2);
    hub.ack(&removed).await.unwrap();
    rig.fake().kick();
    rig.fake().append(change("501", GRACE, "created", Some("unknown"), at + 2, "provider"));
    let next = hub.items(1).await.remove(0);
    assert_eq!(next["revision"], 3);
    assert_eq!(next["actor"]["id"], id_handle(GRACE));
    assert_eq!(next["origin"], "unknown");
}

#[tokio::test]
async fn an_old_gateway_keeps_message_delivery_without_claiming_reaction_coverage() {
    let rig = Rig::start().await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let hello = hub.hello().await.unwrap();
    assert!(!hello["features"].as_array().unwrap().contains(&json!("changes.reactions")));
    hub.subscribe_now().await;
    rig.fake().append(change("501", ME, "created", Some("manual"), now_ms(), "provider"));
    let skipped = hub.batch().await;
    assert!(skipped["items"].as_array().unwrap().is_empty());
    assert_eq!(rig.fake().w().called("changes.subscribe").last().unwrap()["kinds"], json!(["message"]));
    rig.fake().post(GROUP, GRACE, "still works");
    assert_eq!(hub.items(1).await[0]["message"]["text"], "still works");
}

#[tokio::test]
async fn old_metadata_is_not_hydrated_into_fictional_history() {
    let rig = Rig::with(enable).await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().append(json!({"kind":"reaction","message_id":"501","chat":GROUP,"op":"update"}));
    let skipped = hub.batch().await;
    assert_eq!(skipped["seq"], 1);
    assert!(skipped["items"].as_array().unwrap().is_empty());
    assert!(rig.fake().w().called("reactions.list").is_empty());
    rig.fake().append(change("501", ME, "removed", Some("unknown"), now_ms(), "observed"));
    assert_eq!(hub.items(1).await[0]["revision"], 2);
}

#[tokio::test]
async fn malformed_declared_actions_close_the_feed_without_acknowledging_or_inventing_them() {
    let rig = Rig::with(enable).await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    let mut invalid = change("501", ME, "created", Some("manual"), now_ms(), "provider");
    invalid["reaction"].as_object_mut().unwrap().remove("action_ms");
    rig.fake().append(invalid);
    let (code, frames) = hub.until_closed().await;
    assert_eq!(code, Some(1012));
    assert!(frames.iter().all(|f| f["method"] != "changes.batch"));
    assert!(rig.fake().w().acks.is_empty());
    assert!(rig.fake().w().called("reactions.list").is_empty());
}

#[tokio::test]
async fn a_changed_reaction_capability_forces_a_new_handshake() {
    let rig = Rig::with(enable).await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().w().features.retain(|f| f != "reaction_changes.v1");
    rig.fake().kick();
    assert_eq!(hub.until_closed().await.0, Some(1012));
}

#[tokio::test]
async fn incoming_reactions_need_message_read_scope_not_reaction_write_scope() {
    let rig = Rig::with(|w| {
        enable(w);
        w.scopes = vec!["messages.read".into(), "chats.read".into()];
    })
    .await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let hello = hub.hello().await.unwrap();
    let features = hello["features"].as_array().unwrap();
    assert!(features.contains(&json!("changes.reactions")));
    assert!(!features.contains(&json!("reactions")));
    assert_eq!(hello["write_policy"], "forbidden");
    hub.subscribe_now().await;
    rig.fake().append(change("501", GRACE, "created", Some("manual"), now_ms(), "provider"));
    assert_eq!(hub.items(1).await[0]["origin"], "manual");
}

#[tokio::test]
async fn the_independent_feed_socket_cannot_subscribe_as_another_account() {
    let rig = Rig::with(|w| {
        enable(w);
        w.feed_me = Some(GRACE.into());
    })
    .await;
    let mut hub = rig.hub().await;
    let error = hub.call("changes.subscribe", json!({"from":"now"})).await.unwrap_err();
    assert_eq!(reason(&error), "account_changed");
    assert!(rig.fake().w().called("changes.subscribe").is_empty());
}

#[tokio::test]
async fn a_reaction_revision_that_the_hub_cannot_store_is_not_truncated_or_acknowledged() {
    let rig = Rig::with(enable).await;
    let mut hub = rig.hub().await;
    hub.subscribe_now().await;
    rig.fake().w().head = i64::MAX as u64;
    rig.fake().append(change("501", ME, "created", Some("manual"), now_ms(), "provider"));
    let (code, frames) = hub.until_closed().await;
    assert_eq!(code, Some(1012));
    assert!(frames.iter().all(|f| f["method"] != "changes.batch"));
    assert!(rig.fake().w().acks.is_empty());
}

#[tokio::test]
async fn an_independent_feed_cannot_silently_lose_the_coverage_announced_to_the_hub() {
    let rig = Rig::with(|w| {
        enable(w);
        w.feed_reactions = Some(false);
    })
    .await;
    let mut hub = rig.hub().await;
    let error = hub.call("changes.subscribe", json!({"from":"now"})).await.unwrap_err();
    assert_eq!(reason(&error), "upstream_unavailable");
    assert!(rig.fake().w().called("changes.subscribe").is_empty());
}
