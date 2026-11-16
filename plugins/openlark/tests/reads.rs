//! The reads (§4.1 to §4.3, §4.6): identity, chats, messages by id, history by time, people.

mod common;

use common::*;
use serde_json::{Value, json};

fn texts(page: &Value) -> Vec<String> {
    page["messages"].as_array().unwrap().iter().map(|m| m["text"].as_str().unwrap().to_string()).collect()
}

/// `count` messages in the group, one a second, the first at `base`.
fn fill(rig: &Rig, count: i64, base: i64) {
    let mut w = rig.fake().w();
    for i in 0..count {
        w.store(GROUP, GRACE, &format!("m{i}"), base + i * 1000);
    }
}

/// Reads a range page by page, the way the Hub does. Returns the pages.
async fn read_all(hub: &mut Hub, mut params: Value) -> Vec<Value> {
    let mut pages = Vec::new();
    loop {
        let page = hub.call("messages.history", params.clone()).await.unwrap();
        let token = page["next_page_token"].clone();
        pages.push(page);
        assert!(pages.len() <= 20, "the read does not end");
        if token.is_null() {
            return pages;
        }
        assert!(token.as_str().unwrap().len() <= 1024);
        params["page_token"] = token;
    }
}

#[tokio::test]
async fn me_get_is_the_signed_in_user() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    let me = hub.call("me.get", json!({})).await.unwrap();
    assert_eq!(
        me,
        json!({
            "realm": "lark-t1",
            "user": {
                "id": {"kind": "id", "value": ME},
                "display": "Ada",
                "aliases": [
                    {"kind": "email", "value": "ada@example.com"},
                    {"kind": "platform", "system": "feishu", "value": "on_ada"},
                ],
            },
        })
    );
}

#[tokio::test]
async fn chats_are_read_by_the_id_they_were_given_under() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    let group =
        json!({"chat_id": GROUP_OC, "kind": "group", "name": "Design", "member_count": null, "has_external": false});
    assert_eq!(hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap(), group);
    // the gateway's own id names the same chat, and the answer says which id is the chat's
    assert_eq!(hub.call("chats.get", json!({"chat_id": GROUP})).await.unwrap(), group);
    // a direct chat has no shared id at the gateway
    assert_eq!(
        hub.call("chats.get", json!({"chat_id": DM})).await.unwrap(),
        json!({"chat_id": DM, "kind": "dm", "name": "Grace", "member_count": null, "has_external": null})
    );
    for unknown in ["oc_nope", "abc", "12345", "om_1"] {
        let refused = hub.call("chats.get", json!({"chat_id": unknown})).await.unwrap_err();
        assert_eq!(reason(&refused), "not_found", "{unknown}");
    }
    assert_eq!(reason(&hub.call("chats.get", json!({})).await.unwrap_err()), "invalid");
    // a renamed chat shows its new name
    rig.fake().w().chats.get_mut(GROUP).unwrap()["name"] = "Design review".into();
    assert_eq!(hub.call("chats.get", json!({"chat_id": GROUP_OC})).await.unwrap()["name"], "Design review");
}

#[tokio::test]
async fn messages_are_read_by_source_id_in_the_order_asked() {
    let rig = Rig::start().await;
    let (a, b, c) = {
        let mut w = rig.fake().w();
        let a = w.store(GROUP, GRACE, "a", 1_000);
        let b = w.store(DM, "50", "b", 2_000);
        let mut c = w.store(GROUP, ME, "c @Grace", 3_000);
        c["parent"] = a["id"].clone();
        c["thread"] = a["id"].clone();
        c["mentions"] = json!([{"user": GRACE, "name": "Grace"}, {"user": ME, "name": "Ada"}, {"user": "all"}]);
        *w.messages.last_mut().unwrap() = c.clone();
        (a, b, c)
    };
    let mut hub = rig.hub().await;
    let items = json!([
        {"chat_id": DM, "source_id": b["id"]},
        {"chat_id": GROUP_OC, "source_id": "404404"},
        {"chat_id": GROUP_OC, "source_id": c["id"]},
        // the platform's own message ids are not known to the gateway
        {"chat_id": GROUP_OC, "id": "om_abc"},
        // in another chat than asked
        {"chat_id": DM, "source_id": a["id"]},
        {"chat_id": "oc_nope", "source_id": a["id"]},
        {"chat_id": GROUP, "source_id": a["id"]},
    ]);
    let got = hub.call("messages.get", json!({"items": items})).await.unwrap();
    let got = got["messages"].as_array().unwrap();
    assert_eq!(got.len(), 7);
    for null in [1, 3, 4, 5] {
        assert_eq!(got[null], Value::Null, "item {null}");
    }
    assert_eq!((got[0]["text"].as_str(), got[0]["chat_id"].as_str()), (Some("b"), Some(DM)));
    assert_eq!(got[0]["sender"]["is_bot"], true);
    assert_eq!(got[0]["chat_kind"], "dm");
    let reply = &got[2];
    assert_eq!(reply["is_mine"], true);
    assert_eq!(reply["reply_to"], json!({"source_id": a["id"], "id": null}));
    assert_eq!(reply["thread_id"], a["id"]);
    assert_eq!(reply["root_id"], Value::Null);
    assert_eq!(
        reply["mentions"],
        json!([
            {"user": id_handle(GRACE), "name": "Grace", "is_me": false},
            {"user": id_handle(ME), "name": "Ada", "is_me": true},
            {"user": null, "name": "all", "is_me": false},
        ])
    );
    assert_eq!((got[6]["text"].as_str(), got[6]["chat_id"].as_str()), (Some("a"), Some(GROUP_OC)));

    let one = json!({"chat_id": GROUP_OC, "source_id": a["id"]});
    let many = hub.call("messages.get", json!({"items": vec![one.clone(); 50]})).await.unwrap();
    assert_eq!(many["messages"].as_array().unwrap().len(), 50);
    let too_many = hub.call("messages.get", json!({"items": vec![one; 51]})).await.unwrap_err();
    assert_eq!(reason(&too_many), "invalid");
    assert_eq!(reason(&hub.call("messages.get", json!({})).await.unwrap_err()), "invalid");
    assert_eq!(hub.call("messages.get", json!({"items": []})).await.unwrap(), json!({"messages": []}));
}

#[tokio::test]
async fn a_recalled_message_and_every_content_type_have_a_shape() {
    let rig = Rig::start().await;
    let ids: Vec<Value> = {
        let mut w = rig.fake().w();
        let contents = [
            json!({"type": "post", "title": "Plan", "markdown": "**one**"}),
            json!({"type": "image", "ref": "m.7.900.img"}),
            json!({"type": "file", "ref": "m.7.901.f", "name": "notes.pdf", "size": 1234, "mime": "application/pdf"}),
            json!({"type": "card", "summary": "Build passed"}),
            json!({"type": "sticker", "summary": "[sticker]"}),
            json!({"type": "text", "text": "gone"}),
        ];
        let mut ids = Vec::new();
        for (i, content) in contents.into_iter().enumerate() {
            let mut m = w.store(GROUP, GRACE, "", 1_000 + i64::try_from(i).unwrap());
            m["content"] = content;
            m["recalled"] = (i == 5).into();
            ids.push(m["id"].clone());
            *w.messages.last_mut().unwrap() = m;
        }
        ids
    };
    let mut hub = rig.hub().await;
    let items: Vec<Value> = ids.iter().map(|id| json!({"chat_id": GROUP_OC, "source_id": id})).collect();
    let got = hub.call("messages.get", json!({"items": items})).await.unwrap();
    let got = got["messages"].as_array().unwrap();
    let shape = |m: &Value| (m["kind"].as_str().unwrap().to_string(), m["text"].as_str().unwrap().to_string());
    assert_eq!(shape(&got[0]), ("rich".into(), "Plan\n**one**".into()));
    assert_eq!(shape(&got[1]), ("rich".into(), "[image]".into()));
    assert_eq!(got[1]["attachments"], json!([{"type": "image", "key": "m.7.900.img"}]));
    assert_eq!(shape(&got[2]), ("rich".into(), "[file] notes.pdf".into()));
    assert_eq!(
        got[2]["attachments"],
        json!([{"type": "file", "key": "m.7.901.f", "name": "notes.pdf", "size": 1234, "mime": "application/pdf"}])
    );
    assert_eq!(shape(&got[3]), ("rich".into(), "Build passed".into()));
    assert_eq!(shape(&got[4]), ("unknown".into(), "[sticker]".into()));
    assert_eq!(shape(&got[5]), ("text".into(), String::new()));
    assert_eq!(got[5]["recalled"], true);
    assert!(got[..5].iter().all(|m| m["recalled"] == false));
}

/// §4.3: `[from_ms, to_ms)`, every message once, in the order asked.
#[tokio::test]
async fn history_reads_a_time_range_page_by_page() {
    let rig = Rig::start().await;
    let base = now_ms() - 3_600_000;
    fill(&rig, 25, base);
    let mut hub = rig.hub().await;
    let range = json!({"chat_id": GROUP_OC, "from_ms": base + 5_000, "to_ms": base + 20_000, "limit": 10});
    let expected: Vec<String> = (5..20).map(|i| format!("m{i}")).collect();

    let mut asc = range.clone();
    asc["order"] = "asc".into();
    let pages = read_all(&mut hub, asc.clone()).await;
    let read: Vec<String> = pages.iter().flat_map(texts).collect();
    assert_eq!(read, expected);
    assert!(pages.iter().all(|p| p["complete"] == true && p["messages"].as_array().unwrap().len() <= 10));
    let first = &pages[pages.iter().position(|p| !texts(p).is_empty()).unwrap()]["messages"][0];
    assert_eq!(first["created_ms"], base + 5_000);
    assert_eq!((first["chat_id"].as_str(), first["chat_name"].as_str()), (Some(GROUP_OC), Some("Design")));
    // ascending is the default
    assert_eq!(read_all(&mut hub, range.clone()).await, pages);

    let mut desc = range.clone();
    desc["order"] = "desc".into();
    let pages = read_all(&mut hub, desc.clone()).await;
    let read: Vec<String> = pages.iter().flat_map(texts).collect();
    assert_eq!(read, expected.iter().rev().cloned().collect::<Vec<_>>());

    // the same range by the gateway's own chat id
    let mut by_id = asc.clone();
    by_id["chat_id"] = GROUP.into();
    assert_eq!(read_all(&mut hub, by_id).await.iter().flat_map(texts).collect::<Vec<_>>(), expected);

    // a range that ends where the chat does, and one with nothing in it
    let tail = json!({"chat_id": GROUP_OC, "from_ms": base + 23_000, "to_ms": base + 600_000});
    assert_eq!(read_all(&mut hub, tail).await.iter().flat_map(texts).collect::<Vec<_>>(), ["m23", "m24"]);
    for nothing in [(base + 500, base + 900), (base - 9_000, base), (base + 50_000, base + 60_000), (base, base)] {
        for order in ["asc", "desc"] {
            let params =
                json!({"chat_id": GROUP_OC, "from_ms": nothing.0, "to_ms": nothing.1, "order": order, "limit": 10});
            let pages = read_all(&mut hub, params).await;
            assert!(pages.iter().all(|p| texts(p).is_empty()), "{nothing:?} {order}");
        }
    }
    let empty = json!({"chat_id": DM, "from_ms": base, "to_ms": base + 20_000});
    assert_eq!(
        hub.call("messages.history", empty).await.unwrap(),
        json!({"messages": [], "next_page_token": null, "complete": true})
    );
}

#[tokio::test]
async fn a_page_token_belongs_to_its_read() {
    let rig = Rig::start().await;
    let base = now_ms() - 3_600_000;
    fill(&rig, 25, base);
    let mut hub = rig.hub().await;
    let range = json!({"chat_id": GROUP_OC, "from_ms": base + 5_000, "to_ms": base + 20_000, "limit": 10});
    let token = hub.call("messages.history", range.clone()).await.unwrap()["next_page_token"].clone();
    assert!(token.is_string());

    let with = |edit: &dyn Fn(&mut Value)| {
        let mut params = range.clone();
        params["page_token"] = token.clone();
        edit(&mut params);
        params
    };
    let stale = [
        with(&|p| p["to_ms"] = (base + 21_000).into()),
        with(&|p| p["from_ms"] = (base + 4_000).into()),
        with(&|p| p["order"] = "desc".into()),
        with(&|p| p["chat_id"] = DM.into()),
        with(&|p| p["page_token"] = "eyJub3QiOiJvdXJzIn0".into()),
        with(&|p| p["page_token"] = 7.into()),
        // a token of the right read whose place the gateway does not know
        with(&|p| p["page_token"] = format!("h1:a:{}:{}:{GROUP}:0:p999", base + 5_000, base + 20_000).into()),
    ];
    for params in stale {
        let refused = hub.call("messages.history", params.clone()).await.unwrap_err();
        assert_eq!(reason(&refused), "unknown_cursor", "{params}");
    }
    // the token itself is still good
    hub.call("messages.history", with(&|_| {})).await.unwrap();

    let invalid = [
        json!({"chat_id": GROUP_OC, "from_ms": base + 9, "to_ms": base}),
        json!({"chat_id": GROUP_OC, "from_ms": base - 32 * 86_400_000, "to_ms": base}),
        json!({"chat_id": GROUP_OC, "from_ms": base, "to_ms": base + 9, "limit": 0}),
        json!({"chat_id": GROUP_OC, "from_ms": base, "to_ms": base + 9, "order": "newest"}),
        json!({"chat_id": GROUP_OC, "from_ms": base}),
        json!({"from_ms": base, "to_ms": base + 9}),
    ];
    for params in invalid {
        let refused = hub.call("messages.history", params.clone()).await.unwrap_err();
        assert_eq!(reason(&refused), "invalid", "{params}");
    }
    let nowhere = json!({"chat_id": "oc_nope", "from_ms": base, "to_ms": base + 9});
    assert_eq!(reason(&hub.call("messages.history", nowhere).await.unwrap_err()), "not_found");
    // a limit above history_max_limit is the limit
    let wide = json!({"chat_id": GROUP_OC, "from_ms": base, "to_ms": base + 60_000, "limit": 5000});
    assert_eq!(texts(&hub.call("messages.history", wide).await.unwrap()).len(), 25);
    assert_eq!(rig.fake().w().called("messages.history").last().unwrap()["limit"], 100);
}

/// §4.3: a range whose start cannot be reached is said to be incomplete, never passed off as
/// whole.
#[tokio::test]
async fn a_range_too_deep_to_reach_is_incomplete() {
    let rig = Rig::start().await;
    let base = now_ms() - 3_600_000;
    fill(&rig, 45, base);
    let mut hub = rig.hub().await;
    let params = json!({"chat_id": GROUP_OC, "from_ms": base - 1_000, "to_ms": base + 100_000, "limit": 1});
    let page = hub.call("messages.history", params.clone()).await.unwrap();
    assert_eq!(page["complete"], false);
    // what it did reach is given, and every later page says the same
    assert_eq!(texts(&page), ["m5"]);
    let mut next = params.clone();
    next["page_token"] = page["next_page_token"].clone();
    let page = hub.call("messages.history", next).await.unwrap();
    assert_eq!((texts(&page), page["complete"].as_bool()), (vec!["m6".to_string()], Some(false)));
    // with pages of the Hub's usual size the same range is whole
    let mut wide = params;
    wide["limit"] = 50.into();
    let pages = read_all(&mut hub, wide).await;
    assert_eq!(pages.iter().flat_map(texts).count(), 45);
    assert!(pages.iter().all(|p| p["complete"] == true));
}

#[tokio::test]
async fn people_are_looked_up_by_the_id_handle() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    let users = json!([
        id_handle(GRACE),
        {"kind": "email", "value": "grace@example.com"},
        id_handle("424242"),
        id_handle("50"),
        {"kind": "platform", "system": "feishu", "value": "on_ada"},
        id_handle(GRACE),
    ]);
    let got = hub.call("people.get", json!({"users": users})).await.unwrap();
    assert_eq!(got["missing_permissions"], json!([]));
    let people = got["people"].as_array().unwrap();
    let grace = json!({
        "user": id_handle(GRACE),
        "display": "Grace",
        "email": "grace@example.com",
        "avatar_url": "https://cdn.example.com/9.png",
        "title": null,
        "org": null,
        "aliases": [{"kind": "email", "value": "grace@example.com"}],
        "directory": true,
    });
    assert_eq!(people.len(), 6);
    assert_eq!(people[0], grace);
    assert_eq!(people[5], grace);
    for null in [1, 2, 4] {
        assert_eq!(people[null], Value::Null, "user {null}");
    }
    assert_eq!((people[3]["display"].as_str(), &people[3]["email"]), (Some("Helper"), &Value::Null));
    // one gateway call, each id once
    assert_eq!(rig.fake().w().called("people.batch"), [json!({"users": ["424242", "50", "9"]})]);
    // nothing the gateway can look up: it is not asked
    let got = hub.call("people.get", json!({"users": [{"kind": "email", "value": "x@example.com"}]})).await.unwrap();
    assert_eq!(got["people"], json!([null]));
    assert_eq!(rig.fake().w().called("people.batch").len(), 1);
    let too_many = hub.call("people.get", json!({"users": vec![id_handle(GRACE); 51]})).await.unwrap_err();
    assert_eq!(reason(&too_many), "invalid");
}

/// §9.2: the gateway's refusals under the protocol's names.
#[tokio::test]
async fn a_gateway_error_keeps_its_meaning() {
    let rig = Rig::start().await;
    let mut hub = rig.hub().await;
    let read = json!({"chat_id": GROUP_OC});
    hub.call("chats.get", read.clone()).await.unwrap();
    let cases = [
        (("rate_limited", Some(700)), "rate_limited"),
        (("upstream", None), "upstream_unavailable"),
        (("scope", None), "forbidden"),
        (("forbidden", None), "target_forbidden"),
        (("not_found", None), "not_found"),
        (("internal", None), "internal"),
        (("unsupported", None), "unsupported"),
        (("something_new", None), "internal"),
    ];
    for ((code, retry_after_ms), expected) in cases {
        rig.fake().w().fail.insert("chats.get".into(), (code.to_string(), retry_after_ms));
        let refused = hub.call("chats.get", read.clone()).await.unwrap_err();
        assert_eq!(reason(&refused), expected, "{code}");
        assert!(refused["code"].is_i64() && refused["message"].is_string());
        if let Some(ms) = retry_after_ms {
            assert_eq!(refused["data"]["retry_after_ms"], ms);
        }
    }
    rig.fake().w().fail.clear();
    hub.call("chats.get", read).await.unwrap();
}
