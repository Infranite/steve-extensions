//! The connection itself: the upgrade, hello, framing, and the session and upstream state the
//! Hub is told about (§2, §3, §9; vectors V1, V3, V13).

mod common;

use std::time::Duration;

use common::*;
use serde_json::{Value, json};
use steve_plugin_openlark::config::Timing;

fn features(hello: &Value) -> Vec<&str> {
    hello["features"].as_array().unwrap().iter().map(|f| f.as_str().unwrap()).collect()
}

#[tokio::test]
async fn the_upgrade_needs_the_bearer_token() {
    let rig = Rig::start().await;
    assert_eq!(Hub::connect(rig.addr, "/ws", None).await.err(), Some(401));
    assert_eq!(Hub::connect(rig.addr, "/ws", Some("hub-token-012345678X")).await.err(), Some(401));
    // the gateway's credential is not the Hub's token
    assert_eq!(Hub::connect(rig.addr, "/ws", Some(GW_TOKEN)).await.err(), Some(401));
    assert_eq!(Hub::connect(rig.addr, "/", None).await.err(), Some(401));
    for path in ["/ws", "/"] {
        let mut hub = Hub::connect(rig.addr, path, Some(HUB_TOKEN)).await.unwrap();
        hub.hello().await.unwrap();
    }
}

#[tokio::test]
async fn hello_says_what_this_server_is_and_can_do() {
    let rig = Rig::start().await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let before = now_ms();
    // the Hub's hello carries a field and a capability this server does not know
    let hello = hub.hello().await.unwrap();
    assert_eq!(hello["protocol"], 1);
    assert_eq!(hello["server"]["name"], "steve-plugin-openlark");
    assert!(hello["server"]["version"].is_string());
    assert_eq!(hello["realm"], "lark-t1");
    assert_eq!(hello["time_precision_ms"], 1);
    assert!((before..=now_ms()).contains(&hello["time_ms"].as_i64().unwrap()));
    let features = features(&hello);
    for required in ["me", "chats.get", "messages.get", "messages.history", "changes", "media.download"] {
        assert!(features.contains(&required), "{required} is required");
    }
    for offered in
        ["changes.inline_body", "messages.send", "messages.reply", "messages.recall", "reactions", "people.get"]
    {
        assert!(features.contains(&offered), "{offered} is offered");
    }
    assert_eq!(features.len(), 12, "nothing the gateway cannot serve is announced: {features:?}");
    let limits = hello["limits"].as_object().unwrap();
    let mut keys: Vec<&str> = limits.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "history_max_limit",
            "max_attachment_bytes",
            "max_batch_items",
            "max_frame_bytes",
            "max_ids_per_call",
            "max_inflight_requests",
            "max_text_bytes",
            "max_unacked_batches",
            "write_burst",
            "write_per_sec",
        ]
    );
    assert!(limits.values().all(|v| v.as_u64().is_some_and(|n| n > 0)));
    assert_eq!(hello["limits"]["max_frame_bytes"], 1 << 20);
    assert_eq!(hello["limits"]["max_unacked_batches"], 4);
    assert_eq!(hello["session"], json!({"status": "ok"}));
    assert_eq!(hello["upstream"], json!({"status": "ok"}));
    assert_eq!(hello["write_policy"], "restricted");

    // hello is said once
    assert_eq!(reason(&hub.hello().await.unwrap_err()), "invalid");
    let pong = hub.call("ping", json!({})).await.unwrap();
    assert!(pong["time_ms"].as_i64().unwrap() >= before);
    // a method this server does not have
    assert_eq!(reason(&hub.call("chats.list", json!({})).await.unwrap_err()), "unsupported");
    assert_eq!(reason(&hub.call("x.office.calendar", json!({})).await.unwrap_err()), "unsupported");
    // an answer or a notification from the Hub is nothing to this server
    hub.send_text(r#"{"jsonrpc":"2.0","id":77,"result":{}}"#).await;
    hub.send_text(r#"{"jsonrpc":"2.0","method":"something.happened","params":{}}"#).await;
    hub.call("ping", json!({})).await.unwrap();
}

#[tokio::test]
async fn features_follow_the_gateway_and_the_credential() {
    let rig = Rig::with(|w| {
        w.scopes = vec!["chats.read".into(), "messages.read".into()];
        w.features.retain(|f| f != "changes.v1");
    })
    .await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let hello = hub.hello().await.unwrap();
    assert_eq!(features(&hello), ["me", "chats.get", "messages.get", "messages.history", "media.download"]);
    // a credential that cannot write at all
    assert_eq!(hello["write_policy"], "forbidden");
    let send = json!({"chat_id": GROUP_OC, "text": "x", "idempotency_key": "k", "expected_sender": id_handle(ME)});
    assert_eq!(reason(&hub.call("messages.send", send).await.unwrap_err()), "unsupported");
    assert_eq!(reason(&hub.call("people.get", json!({"users": []})).await.unwrap_err()), "unsupported");
    assert_eq!(reason(&hub.call("changes.subscribe", json!({"from": "now"})).await.unwrap_err()), "unsupported");
    assert!(rig.fake().w().called("messages.send").is_empty());
    hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap();
}

/// V3.
#[tokio::test]
async fn a_request_before_hello_is_refused_and_the_socket_closed() {
    let rig = Rig::start().await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    hub.ask("me.get", json!({})).await;
    let (code, frames) = hub.until_closed().await;
    assert_eq!(code, Some(1008));
    assert_eq!(frames.len(), 1);
    assert_eq!(reason(&frames[0]["error"]), "not_ready");
    assert_eq!(frames[0]["error"]["code"], -32000);
}

/// V3.
#[tokio::test]
async fn a_socket_without_hello_is_closed_at_the_deadline() {
    let rig = Rig::start().await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let opened = tokio::time::Instant::now();
    let (code, frames) = hub.until_closed().await;
    assert_eq!(code, Some(1008));
    assert!(frames.is_empty());
    assert!(opened.elapsed() >= rig.timing.hello_deadline);
}

/// V1.
#[tokio::test]
async fn a_hub_of_another_version_is_told_which_one_is_spoken() {
    let rig = Rig::start().await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    // a hello that cannot be read leaves the socket waiting for a good one
    assert_eq!(reason(&hub.call("hello", json!({"client": {"name": "steve-hub"}})).await.unwrap_err()), "invalid");
    hub.ask("hello", json!({"protocol": {"min": 2, "max": 3}, "client": {"name": "steve-hub", "version": "9"}})).await;
    let (code, frames) = hub.until_closed().await;
    assert_eq!(code, Some(1008));
    assert_eq!(reason(&frames[0]["error"]), "unsupported_version");
    assert_eq!(frames[0]["error"]["data"]["supported"], json!([1]));

    // a range that includes version 1 is version 1
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let hello = hub.call("hello", json!({"protocol": {"min": 1, "max": 4}, "client": {"name": "steve-hub"}})).await;
    assert_eq!(hello.unwrap()["protocol"], 1);
}

#[tokio::test]
async fn frames_outside_the_protocol_close_the_socket() {
    let rig = Rig::start().await;

    let mut hub = rig.hub().await;
    hub.send_binary(b"\x00\x01").await;
    assert_eq!(hub.until_closed().await.0, Some(1003));

    for not_a_request in ["this is not JSON", r#"[{"jsonrpc":"2.0","id":1,"method":"ping","params":{}}]"#, "17"] {
        let mut hub = rig.hub().await;
        hub.send_text(not_a_request).await;
        let (code, frames) = hub.until_closed().await;
        assert_eq!(code, Some(1003), "{not_a_request}");
        assert_eq!(reason(&frames[0]["error"]), "parse_error");
        assert_eq!(frames[0]["id"], Value::Null);
    }

    // one byte more than max_frame_bytes
    let mut hub = rig.hub().await;
    let frame = json!({"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {"pad": ""}}).to_string();
    let pad = "a".repeat((1 << 20) + 1 - frame.len());
    let frame = frame.replace(r#""pad":"""#, &format!(r#""pad":"{pad}""#));
    assert_eq!(frame.len(), (1 << 20) + 1);
    hub.send_text(&frame).await;
    assert_eq!(hub.until_closed().await.0, Some(1009));

    // a frame of exactly the limit is a frame like any other
    let mut hub = rig.hub().await;
    hub.send_text(&frame.replacen('a', "", 1)).await;
    match hub.frame().await {
        Frame::Json(v) => assert!(v["result"]["time_ms"].is_i64()),
        Frame::Closed(code) => panic!("closed with {code:?}"),
    }
}

#[tokio::test]
async fn a_silent_hub_is_dropped_and_a_pinging_one_is_kept() {
    let timing = Timing { idle: Duration::from_millis(400), ..fast() };
    let rig = Rig::custom(|_| {}, timing).await;
    let mut hub = rig.hub().await;
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        hub.call("ping", json!({})).await.unwrap();
    }
    let quiet_since = tokio::time::Instant::now();
    assert_eq!(hub.until_closed().await.0, Some(1001));
    assert!(quiet_since.elapsed() >= Duration::from_millis(350));
}

/// V13: the session's state arrives as it changes, and requests fail by it meanwhile.
#[tokio::test]
async fn an_expired_or_locked_session_is_told_and_then_its_return() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    let send = |key: &str| {
        json!({"chat_id": GROUP_OC, "text": "hello", "idempotency_key": key, "expected_sender": id_handle(ME)})
    };

    rig.fake().w().session = "expired".into();
    let told = hub.note("session.status").await;
    assert_eq!(told["status"], "expired");
    assert!(told["reason"].is_string());
    assert_eq!(told["write_policy"], "restricted");
    assert_eq!(reason(&hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap_err()), "session_expired");
    assert_eq!(reason(&hub.call("messages.send", send("k1")).await.unwrap_err()), "session_expired");
    // who is signed in is still known, and the socket stays
    assert_eq!(hub.call("me.get", json!({})).await.unwrap()["user"]["id"], id_handle(ME));
    // a Hub that connects now learns it in hello
    let mut second = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let hello = second.hello().await.unwrap();
    assert_eq!(hello["session"]["status"], "expired");
    assert!(hello["session"]["reason"].is_string());

    rig.fake().w().session = "locked".into();
    assert_eq!(hub.note("session.status").await["status"], "locked");
    assert_eq!(reason(&hub.call("messages.send", send("k1")).await.unwrap_err()), "session_locked");

    rig.fake().w().session = "ok".into();
    let told = hub.note("session.status").await;
    assert_eq!(told, json!({"status": "ok", "write_policy": "restricted"}));
    assert_eq!(second.note("session.status").await["status"], "locked");
    assert_eq!(second.note("session.status").await["status"], "ok");
    hub.call("messages.send", send("k1")).await.unwrap();
    // the three changes were told once each, and the way to Lark was never said to be down
    assert!(!hub.gets("session.status", Duration::from_millis(400)).await);
    assert!(!hub.gets("push.status", Duration::from_millis(10)).await);
    assert_eq!(rig.fake().w().called("messages.send").len(), 1);
}

#[tokio::test]
async fn a_session_that_is_broken_from_the_start_is_in_hello() {
    // the gateway still names the user
    let rig = Rig::with(|w| w.session = "locked".into()).await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let hello = hub.hello().await.unwrap();
    assert_eq!(hello["session"]["status"], "locked");
    assert_eq!(hello["realm"], "lark-t1");
    assert_eq!(hub.call("me.get", json!({})).await.unwrap()["user"]["id"], id_handle(ME));
    // me.get gives the whole user once the session is back
    rig.fake().w().session = "ok".into();
    assert_eq!(hub.note("session.status").await["status"], "ok");
    let me = hub.call("me.get", json!({})).await.unwrap();
    assert_eq!(me["user"]["aliases"].as_array().unwrap().len(), 2);

    // the gateway names nobody: there is no realm to answer hello with
    for (session, refused_as) in [("expired", "session_expired"), ("locked", "session_locked")] {
        let rig = Rig::with(|w| {
            w.session = session.into();
            w.hello_user = false;
        })
        .await;
        let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
        hub.ask("hello", json!({"protocol": {"min": 1, "max": 1}, "client": {"name": "steve-hub"}})).await;
        let (code, frames) = hub.until_closed().await;
        assert_eq!(reason(&frames[0]["error"]), refused_as);
        assert_eq!(code, Some(1013));
    }
}

/// V13: a gateway that is away is the upstream being down, not a reason to exit.
#[tokio::test]
async fn a_gateway_that_goes_away_is_reported_and_so_is_its_return() {
    let mut rig = Rig::start().await;
    let mut hub = rig.hub().await;
    hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap();

    rig.gw.stop().await;
    let told = hub.note("push.status").await;
    assert_eq!(told["status"], "down");
    assert!(told["since_ms"].is_i64() && told["reason"].is_string());
    assert_eq!(
        reason(&hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap_err()),
        "upstream_unavailable"
    );
    assert_eq!(reason(&hub.call("changes.subscribe", json!({"from": "now"})).await.unwrap_err()), "upstream_unavailable");
    hub.call("me.get", json!({})).await.unwrap();
    let mut second = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let hello = second.hello().await.unwrap();
    assert_eq!((hello["upstream"]["status"].as_str(), hello["session"]["status"].as_str()), (Some("down"), Some("ok")));

    rig.gw.restart().await;
    assert_eq!(hub.note("push.status").await["status"], "ok");
    hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap();
    // down and up were told once each; the session was never said to be broken
    assert!(!hub.gets("push.status", Duration::from_millis(400)).await);
    assert!(!hub.gets("session.status", Duration::from_millis(10)).await);
}

#[tokio::test]
async fn the_gateways_own_push_connection_is_the_upstream_too() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    rig.fake().w().push_connected = false;
    let told = hub.note("push.status").await;
    assert_eq!(told["status"], "down");
    assert!(told["reason"].is_string());
    // requests still go through: only the news is late
    hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap();
    rig.fake().w().push_connected = true;
    let told = hub.note("push.status").await;
    assert_eq!(told["status"], "ok");
    assert!(told.get("reason").is_none());
}

#[tokio::test]
async fn hello_waits_for_the_account_and_gives_up_without_one() {
    let mut rig = Rig::parked(|_| {}, fast()).await;
    rig.gw.stop().await;
    rig.start_bridge().await;
    // nothing is known about the account: no realm, so no hello
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    hub.ask("hello", json!({"protocol": {"min": 1, "max": 1}, "client": {"name": "steve-hub"}})).await;
    let (code, frames) = hub.until_closed().await;
    assert_eq!(reason(&frames[0]["error"]), "upstream_unavailable");
    assert_eq!(code, Some(1013));

    // a hello that arrives while the gateway is on its way is answered once it is there
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let id = hub.ask("hello", json!({"protocol": {"min": 1, "max": 1}, "client": {"name": "steve-hub"}})).await;
    rig.gw.restart().await;
    let hello = hub.answer(id).await.unwrap();
    assert_eq!(hello["upstream"]["status"], "ok");
    drop(hub);

    // after a restart beside a gateway that is away, the account of the last run is still known
    rig.stop_bridge().await;
    rig.gw.stop().await;
    rig.start_bridge().await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    let hello = hub.hello().await.unwrap();
    assert_eq!(hello["realm"], "lark-t1");
    assert_eq!(hello["upstream"]["status"], "down");
    assert!(features(&hello).contains(&"messages.history"));
    assert_eq!(hub.call("me.get", json!({})).await.unwrap()["user"]["id"], id_handle(ME));
    rig.gw.restart().await;
    assert_eq!(hub.note("push.status").await["status"], "ok");
}

/// V13: a credential the gateway refuses is a state the Hub is told, and it can be mended
/// without restarting anything.
#[tokio::test]
async fn a_refused_credential_is_reported_as_the_session() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    rig.fake().revoke();
    let told = hub.note("session.status").await;
    assert_eq!(told["status"], "expired");
    assert!(told["reason"].as_str().unwrap().contains("credential"));
    assert_eq!(hub.note("push.status").await["status"], "down");
    let refused = hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap_err();
    assert_eq!(reason(&refused), "session_expired");
    assert_eq!(refused["data"]["detail"], "gateway_credential_refused");
    let mut second = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    assert_eq!(second.hello().await.unwrap()["session"]["status"], "expired");

    rig.fake().w().revoked = false;
    assert_eq!(hub.note("session.status").await["status"], "ok");
    assert_eq!(hub.note("push.status").await["status"], "ok");
    hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap();
}

#[tokio::test]
async fn a_credential_refused_from_the_start_is_said_in_hello() {
    let rig = Rig::with(|w| w.token = "olk_another_credential_02".into()).await;
    let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    hub.ask("hello", json!({"protocol": {"min": 1, "max": 1}, "client": {"name": "steve-hub"}})).await;
    let (code, frames) = hub.until_closed().await;
    assert_eq!(reason(&frames[0]["error"]), "session_expired");
    assert_eq!(frames[0]["error"]["data"]["detail"], "gateway_credential_refused");
    assert_eq!(code, Some(1013));
    // the bridge is still there once the credential is accepted
    rig.fake().w().token = GW_TOKEN.into();
    let accepted = tokio::time::Instant::now();
    let hello = loop {
        let mut hub = Hub::connect(rig.addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
        match hub.hello().await {
            Ok(hello) => break hello,
            Err(_) if accepted.elapsed() < Duration::from_secs(3) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("the credential is accepted, and hello still says {e}"),
        }
    };
    assert_eq!(hello["session"]["status"], "ok");
}

#[tokio::test]
async fn another_account_at_the_gateway_ends_the_socket() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    assert_eq!(hub.call("me.get", json!({})).await.unwrap()["user"]["id"], id_handle(ME));
    rig.fake().w().me = GRACE.into();
    assert_eq!(hub.until_closed().await.0, Some(1012));
    // the next hello is the new account's
    let mut hub = rig.hub().await;
    let me = hub.call("me.get", json!({})).await.unwrap();
    assert_eq!(me["user"]["id"], id_handle(GRACE));
    assert_eq!(me["user"]["display"], "Grace");
}
