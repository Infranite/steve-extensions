//! The HTTP side: the WebSocket upgrade and the media route, both behind the Hub's bearer token
//! (§3.1, §8).

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use super::proto::MAX_FRAME_BYTES;
use super::session;
use crate::bridge::Bridge;
use crate::media;

/// The Hub's plugin store connects to `/ws`; `/` is the same socket for a hand-entered address.
pub fn router(bridge: Arc<Bridge>) -> Router {
    Router::new()
        .route("/ws", get(upgrade))
        .route("/", get(upgrade))
        .route("/v1/media/{key}", get(media::download))
        .with_state(bridge)
}

/// Whether the request carries the token, compared without an early exit.
pub fn authorized(headers: &HeaderMap, token: &str) -> bool {
    let Some(given) = headers.get(AUTHORIZATION).and_then(|v| v.as_bytes().strip_prefix(b"Bearer ")) else {
        return false;
    };
    let token = token.as_bytes();
    given.len() == token.len() && given.iter().zip(token).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

pub fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, [(WWW_AUTHENTICATE, "Bearer")]).into_response()
}

async fn upgrade(
    State(bridge): State<Arc<Bridge>>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    // the token comes first: a caller without it learns nothing else
    if !authorized(&headers, &bridge.settings.token) {
        return unauthorized();
    }
    match ws {
        // the socket accepts more than a frame may hold, so that the session can refuse an
        // oversized frame with the protocol's own close code
        Ok(ws) => ws
            .max_message_size(MAX_FRAME_BYTES * 2)
            .max_frame_size(MAX_FRAME_BYTES * 2)
            .on_upgrade(move |socket| session::run(socket, bridge)),
        Err(rejection) => rejection.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_exact_bearer_passes() {
        let with = |value: &str| {
            let mut h = HeaderMap::new();
            h.insert(AUTHORIZATION, value.parse().unwrap());
            h
        };
        assert!(authorized(&with("Bearer 0123456789abcdef"), "0123456789abcdef"));
        assert!(!authorized(&with("Bearer 0123456789abcdeX"), "0123456789abcdef"));
        assert!(!authorized(&with("Bearer 0123456789abcde"), "0123456789abcdef"));
        assert!(!authorized(&with("bearer 0123456789abcdef"), "0123456789abcdef"));
        assert!(!authorized(&with("0123456789abcdef"), "0123456789abcdef"));
        assert!(!authorized(&HeaderMap::new(), "0123456789abcdef"));
    }
}
