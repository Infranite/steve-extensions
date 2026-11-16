//! Attachments over HTTP (§8): the gateway's media route behind the Hub's bearer token, on the
//! same origin as the socket.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;

use crate::bridge::Bridge;
use crate::ext::proto::MAX_ATTACHMENT_BYTES;
use crate::ext::server::{authorized, unauthorized};

/// A key is a media reference as the gateway gives one in a message.
fn key_ok(key: &str) -> bool {
    key.starts_with("m.")
        && key.len() <= 512
        && key.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Whether the reference is of this message: the gateway puts the message id third.
fn of_message(key: &str, source_id: &str) -> bool {
    key.split('.').nth(2) == Some(source_id)
}

/// The most the Hub will take, never above the announced `max_attachment_bytes`.
fn cap(headers: &HeaderMap) -> u64 {
    let asked = headers.get("x-max-bytes").and_then(|v| v.to_str().ok()).and_then(|v| v.trim().parse::<u64>().ok());
    asked.map_or(MAX_ATTACHMENT_BYTES, |max| max.min(MAX_ATTACHMENT_BYTES))
}

/// `GET /v1/media/<key>?chat_id=…&source_id=…`.
pub async fn download(
    State(bridge): State<Arc<Bridge>>,
    Path(key): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&headers, &bridge.settings.token) {
        return unauthorized();
    }
    if !key_ok(&key) || query.get("source_id").is_some_and(|source_id| !of_message(&key, source_id)) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(url) = bridge.settings.gateway.media.join(&key) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let cap = cap(&headers);
    let upstream = match bridge.http.get(url).bearer_auth(&bridge.settings.gateway_token).send().await {
        Ok(upstream) => upstream,
        Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
    };
    match upstream.status().as_u16() {
        200 => {}
        400 | 404 => return StatusCode::NOT_FOUND.into_response(),
        403 => return StatusCode::FORBIDDEN.into_response(),
        429 => {
            return match upstream.headers().get(RETRY_AFTER).cloned() {
                Some(after) => (StatusCode::TOO_MANY_REQUESTS, [(RETRY_AFTER, after)]).into_response(),
                None => StatusCode::TOO_MANY_REQUESTS.into_response(),
            };
        }
        // a kind of media the gateway cannot fetch will not come later either
        501 => {
            let body = r#"{"reason":"unsupported"}"#;
            return (StatusCode::GONE, [(CONTENT_TYPE, "application/json")], body).into_response();
        }
        // the gateway's own trouble is not the Hub's token being wrong: never a 401 here
        401 | 503 => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        // a redirect is not followed: the credential stays on the gateway's origin
        _ => return StatusCode::BAD_GATEWAY.into_response(),
    }
    let content_type = upstream
        .headers()
        .get(CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));
    match upstream.content_length() {
        Some(len) if len > cap => StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Some(len) => {
            let body = Body::from_stream(upstream.bytes_stream());
            (StatusCode::OK, [(CONTENT_TYPE, content_type), (CONTENT_LENGTH, HeaderValue::from(len))], body)
                .into_response()
        }
        // a length must be announced, so a body of unknown length is read first
        None => {
            let mut all: Vec<u8> = Vec::new();
            let mut chunks = upstream.bytes_stream();
            while let Some(chunk) = chunks.next().await {
                let Ok(chunk) = chunk else { return StatusCode::BAD_GATEWAY.into_response() };
                if (all.len() + chunk.len()) as u64 > cap {
                    return StatusCode::PAYLOAD_TOO_LARGE.into_response();
                }
                all.extend_from_slice(&chunk);
            }
            (StatusCode::OK, [(CONTENT_TYPE, content_type)], all).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_a_gateway_reference_of_the_message() {
        assert!(key_ok("m.7.501.aGVsbG8"));
        assert!(!key_ok("x.7.501.a"));
        assert!(!key_ok("m.7.501/../../agent"));
        assert!(!key_ok("m.7.501.a?x=1"));
        assert!(!key_ok(&format!("m.{}", "a".repeat(512))));
        assert!(of_message("m.7.501.a", "501"));
        assert!(!of_message("m.7.501.a", "502"));
        assert!(!of_message("m.7", "7"));
    }

    #[test]
    fn the_cap_is_the_lower_of_the_two() {
        let with = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert("x-max-bytes", v.parse().unwrap());
            h
        };
        assert_eq!(cap(&with("1024")), 1024);
        assert_eq!(cap(&with("999999999999")), MAX_ATTACHMENT_BYTES);
        assert_eq!(cap(&with("many")), MAX_ATTACHMENT_BYTES);
        assert_eq!(cap(&HeaderMap::new()), MAX_ATTACHMENT_BYTES);
    }
}
