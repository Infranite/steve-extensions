//! The frames, error reasons and limits of Steve's external channel protocol v1, server side.

use serde_json::{Map, Value, json};

/// The protocol versions this server speaks.
pub const VERSIONS: &[u32] = &[1];

// The limits announced in hello. Each is at or below what the Hub accepts.
pub const MAX_FRAME_BYTES: usize = 1 << 20;
pub const MAX_TEXT_BYTES: usize = 256 * 1024;
pub const MAX_BATCH_ITEMS: usize = 50;
pub const MAX_UNACKED_BATCHES: usize = 4;
pub const MAX_IDS_PER_CALL: usize = 50;
/// The gateway's own page limit.
pub const HISTORY_MAX_LIMIT: usize = 100;
pub const MAX_INFLIGHT_REQUESTS: usize = 32;
pub const MAX_ATTACHMENT_BYTES: u64 = 100 * 1024 * 1024;
/// The gateway announces no write pace of its own; this keeps the Hub gentle with it.
pub const WRITE_PER_SEC: u32 = 5;
pub const WRITE_BURST: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    ParseError,
    Invalid,
    Unsupported,
    Internal,
    NotReady,
    UnsupportedVersion,
    Unauthorized,
    SessionExpired,
    SessionLocked,
    AccountChanged,
    WriteForbidden,
    RateLimited,
    NotFound,
    Forbidden,
    UnknownCursor,
    CursorTooOld,
    TooLarge,
    IdempotencyConflict,
    UpstreamUnavailable,
    Timeout,
    TargetForbidden,
}

impl Reason {
    pub fn name(self) -> &'static str {
        match self {
            Self::ParseError => "parse_error",
            Self::Invalid => "invalid",
            Self::Unsupported => "unsupported",
            Self::Internal => "internal",
            Self::NotReady => "not_ready",
            Self::UnsupportedVersion => "unsupported_version",
            Self::Unauthorized => "unauthorized",
            Self::SessionExpired => "session_expired",
            Self::SessionLocked => "session_locked",
            Self::AccountChanged => "account_changed",
            Self::WriteForbidden => "write_forbidden",
            Self::RateLimited => "rate_limited",
            Self::NotFound => "not_found",
            Self::Forbidden => "forbidden",
            Self::UnknownCursor => "unknown_cursor",
            Self::CursorTooOld => "cursor_too_old",
            Self::TooLarge => "too_large",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::UpstreamUnavailable => "upstream_unavailable",
            Self::Timeout => "timeout",
            Self::TargetForbidden => "target_forbidden",
        }
    }

    pub fn code(self) -> i64 {
        match self {
            Self::ParseError => -32700,
            Self::Invalid => -32602,
            Self::Unsupported => -32601,
            Self::Internal => -32603,
            Self::NotReady => -32000,
            Self::UnsupportedVersion => -32001,
            Self::Unauthorized => -32002,
            Self::SessionExpired => -32003,
            Self::SessionLocked => -32004,
            Self::AccountChanged => -32005,
            Self::WriteForbidden => -32006,
            Self::RateLimited => -32007,
            Self::NotFound => -32008,
            Self::Forbidden => -32009,
            Self::UnknownCursor => -32010,
            Self::CursorTooOld => -32011,
            Self::TooLarge => -32012,
            Self::IdempotencyConflict => -32013,
            Self::UpstreamUnavailable => -32014,
            Self::Timeout => -32015,
            Self::TargetForbidden => -32016,
        }
    }
}

/// An error answer (§7). The message is written here, never taken from the gateway: the Hub
/// shows it to the user.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcError {
    pub reason: Reason,
    pub message: String,
    pub data: Map<String, Value>,
}

impl RpcError {
    pub fn new(reason: Reason, message: impl Into<String>) -> Self {
        Self { reason, message: message.into(), data: Map::new() }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(Reason::Invalid, message)
    }

    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.data.insert(key.to_string(), value.into());
        self
    }

    pub fn to_value(&self) -> Value {
        let mut data = self.data.clone();
        data.insert("reason".into(), self.reason.name().into());
        json!({"code": self.reason.code(), "message": self.message, "data": data})
    }
}

pub type Rpc<T = Value> = Result<T, RpcError>;

pub fn response(id: &Value, result: &Rpc) -> String {
    match result {
        Ok(r) => json!({"jsonrpc": "2.0", "id": id, "result": r}),
        Err(e) => json!({"jsonrpc": "2.0", "id": id, "error": e.to_value()}),
    }
    .to_string()
}

pub fn notification(method: &str, params: Value) -> String {
    json!({"jsonrpc": "2.0", "method": method, "params": params}).to_string()
}

/// The highest version both sides speak (§3.3).
pub fn negotiate(client_min: u32, client_max: u32, server: &[u32]) -> Option<u32> {
    server.iter().copied().filter(|v| (client_min..=client_max).contains(v)).max()
}

/// An id as the protocol carries one: 1–`max` bytes, no control characters, not the Hub's own
/// `fp:` prefix.
pub fn opaque_ok(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && !s.starts_with("fp:") && !s.chars().any(char::is_control)
}

pub fn now_ms() -> i64 {
    let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
}

/// A required string of the params.
pub fn str_param<'a>(params: &'a Value, key: &str) -> Rpc<&'a str> {
    match params.get(key).and_then(Value::as_str) {
        Some(s) if opaque_ok(s, 256) => Ok(s),
        _ => Err(RpcError::invalid(format!("`{key}` is missing or not a usable id"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_the_highest_common_version() {
        assert_eq!(negotiate(1, 1, VERSIONS), Some(1));
        assert_eq!(negotiate(1, 3, &[1, 2]), Some(2));
        assert_eq!(negotiate(2, 3, &[1]), None);
        assert_eq!(negotiate(2, 3, VERSIONS), None);
        assert_eq!(negotiate(3, 1, &[1, 2, 3]), None);
    }

    #[test]
    fn an_error_carries_its_reason_and_code() {
        let e = RpcError::new(Reason::RateLimited, "slow down").with("retry_after_ms", 800);
        assert_eq!(
            e.to_value(),
            json!({"code": -32007, "message": "slow down", "data": {"reason": "rate_limited", "retry_after_ms": 800}})
        );
        let frame: Value = serde_json::from_str(&response(&json!(7), &Err(e))).unwrap();
        assert_eq!(frame["id"], 7);
        assert_eq!(frame["error"]["data"]["reason"], "rate_limited");
        assert!(frame.get("result").is_none());
    }

    #[test]
    fn ids_are_opaque_but_bounded() {
        assert!(opaque_ok("oc_1", 256));
        assert!(!opaque_ok("", 256));
        assert!(!opaque_ok("fp:1", 256));
        assert!(!opaque_ok("a\nb", 256));
        assert!(!opaque_ok(&"x".repeat(257), 256));
    }
}
