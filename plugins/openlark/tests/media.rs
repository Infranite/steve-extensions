//! Attachments over HTTP (§8), on the origin of the socket.

mod common;

use common::*;
use reqwest::{Client, Response};

const IMAGE: &str = "m.7.501.aW1hZ2U";

fn world(w: &mut World) {
    w.media.insert(IMAGE.into(), Media::file("image/png", b"\x89PNG not really a picture"));
    w.media.insert("m.7.502.big".into(), Media::file("application/pdf", &[7u8; 5000]));
    let mut stream = Media::file("video/mp4", &[9u8; 3000]);
    stream.chunked = true;
    w.media.insert("m.7.503.stream".into(), stream);
    let mut untyped = Media::file("x", b"bytes");
    untyped.content_type = None;
    w.media.insert("m.7.504.plain".into(), untyped);
    let mut moved = Media::status(302);
    moved.location = Some("http://127.0.0.1:9/elsewhere");
    w.media.insert("m.7.505.moved".into(), moved);
    for (key, status) in [("m.7.506.sticker", 501), ("m.7.507.paced", 429), ("m.7.508.denied", 403)] {
        w.media.insert(key.into(), Media::status(status));
    }
    for (key, status) in [("m.7.509.bad", 400), ("m.7.510.broken", 500), ("m.7.511.away", 503)] {
        w.media.insert(key.into(), Media::status(status));
    }
}

async fn get(rig: &Rig, key: &str, max: Option<u64>) -> Response {
    let mut request = Client::new().get(rig.media_url(key)).bearer_auth(HUB_TOKEN);
    if let Some(max) = max {
        request = request.header("X-Max-Bytes", max);
    }
    request.send().await.unwrap()
}

#[tokio::test]
async fn media_needs_the_hubs_token() {
    let rig = Rig::with(world).await;
    let url = rig.media_url(IMAGE);
    let http = Client::new();
    assert_eq!(http.get(&url).send().await.unwrap().status(), 401);
    assert_eq!(http.get(&url).bearer_auth("hub-token-012345678X").send().await.unwrap().status(), 401);
    // the gateway's credential opens nothing here
    assert_eq!(http.get(&url).bearer_auth(GW_TOKEN).send().await.unwrap().status(), 401);
    assert_eq!(http.get(&url).bearer_auth(HUB_TOKEN).send().await.unwrap().status(), 200);
}

#[tokio::test]
async fn a_file_comes_with_its_type_and_length() {
    let rig = Rig::with(world).await;
    let response = get(&rig, &format!("{IMAGE}?chat_id={GROUP_OC}&source_id=501"), Some(1 << 20)).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "image/png");
    assert_eq!(response.headers()["content-length"], "25");
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"\x89PNG not really a picture");

    // a body the gateway sends without a length still arrives whole
    let response = get(&rig, "m.7.503.stream", None).await;
    assert_eq!((response.status().as_u16(), &response.headers()["content-type"]), (200, &"video/mp4".parse().unwrap()));
    assert_eq!(response.bytes().await.unwrap().as_ref(), &[9u8; 3000]);
    let response = get(&rig, "m.7.504.plain", None).await;
    assert_eq!(response.headers()["content-type"], "application/octet-stream");
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"bytes");
}

#[tokio::test]
async fn more_than_the_hub_will_take_is_refused_before_it_is_sent() {
    let rig = Rig::with(world).await;
    for (key, max, status) in [
        ("m.7.502.big", 4999, 413),
        ("m.7.502.big", 5000, 200),
        ("m.7.503.stream", 2999, 413),
        ("m.7.503.stream", 3000, 200),
        (IMAGE, 0, 413),
    ] {
        let response = get(&rig, key, Some(max)).await;
        assert_eq!(response.status(), status, "{key} within {max}");
        let body = response.bytes().await.unwrap();
        assert_eq!(body.is_empty(), status == 413);
    }
}

#[tokio::test]
async fn what_cannot_be_served_says_how() {
    let mut rig = Rig::with(world).await;
    let cases = [
        // not a key of this server, a key of another message, a key the gateway does not know
        ("img_v3_abc", 404),
        ("m.7.999.unknown", 404),
        ("m.7.501.aW1hZ2U?source_id=502", 404),
        ("m.7.509.bad", 404),
        ("m.7.508.denied", 403),
        ("m.7.507.paced", 429),
        ("m.7.510.broken", 502),
        // a redirect is not followed: the credential stays with the gateway
        ("m.7.505.moved", 502),
        ("m.7.511.away", 503),
    ];
    for (key, status) in cases {
        assert_eq!(get(&rig, key, None).await.status(), status, "{key}");
    }
    // a kind of media the gateway cannot fetch at all will not come later either
    let gone = get(&rig, "m.7.506.sticker", None).await;
    assert_eq!(gone.status(), 410);
    assert_eq!(gone.headers()["content-type"], "application/json");
    let said: serde_json::Value = serde_json::from_slice(&gone.bytes().await.unwrap()).unwrap();
    assert_eq!(said, serde_json::json!({"reason": "unsupported"}));

    // a gateway that refuses the plugin's credential is not the Hub's token being wrong
    rig.fake().revoke();
    assert_eq!(get(&rig, IMAGE, None).await.status(), 503);
    rig.fake().w().revoked = false;
    assert_eq!(get(&rig, IMAGE, None).await.status(), 200);
    rig.gw.stop().await;
    assert_eq!(get(&rig, IMAGE, None).await.status(), 502);
}
