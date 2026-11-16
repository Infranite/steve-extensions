//! `messages.history` (§4.3): a time range, served from a gateway that pages a chat by position.
//!
//! The gateway reads a chat page by page from its newest message backwards (`before`) or from a
//! known place forwards (`after`); it cannot start at a time. So the first call of a range walks
//! back from the newest page until it reaches the range's start, and every later call follows
//! the gateway's own cursor, carried in the page token. This relies on a chat's messages being
//! in creation order by position. Replies inside a thread are not part of a chat's history at
//! the gateway, so they are not in the result either.

use serde_json::{Value, json};

use crate::bridge::Bridge;
use crate::ext::proto::{HISTORY_MAX_LIMIT, Reason, Rpc, RpcError, str_param};
use crate::gateway::wire::HistoryPage;
use crate::map::{self, Ctx};

/// The longest range of one read.
const MAX_RANGE_MS: i64 = 31 * 24 * 3600 * 1000;
/// How many gateway pages the first call may walk back before it gives up on reaching the
/// start of the range and answers `complete: false`.
pub const MAX_WALK_PAGES: usize = 40;
const TOKEN_MAX: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    Asc,
    Desc,
}

/// What a page token says: the read it belongs to and where the gateway continues.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    order: Order,
    from_ms: i64,
    to_ms: i64,
    chat: String,
    /// The walk back stopped early, so the range's oldest part is missing.
    incomplete: bool,
    cursor: String,
}

impl Token {
    fn encode(&self) -> String {
        let order = if self.order == Order::Asc { 'a' } else { 'd' };
        let incomplete = u8::from(self.incomplete);
        format!("h1:{order}:{}:{}:{}:{incomplete}:{}", self.from_ms, self.to_ms, self.chat, self.cursor)
    }

    fn parse(s: &str) -> Option<Token> {
        let mut parts = s.splitn(7, ':');
        if parts.next()? != "h1" {
            return None;
        }
        let order = match parts.next()? {
            "a" => Order::Asc,
            "d" => Order::Desc,
            _ => return None,
        };
        let from_ms = parts.next()?.parse().ok()?;
        let to_ms = parts.next()?.parse().ok()?;
        let chat = parts.next()?.to_string();
        let incomplete = match parts.next()? {
            "0" => false,
            "1" => true,
            _ => return None,
        };
        let cursor = parts.next().filter(|c| !c.is_empty())?.to_string();
        Some(Token { order, from_ms, to_ms, chat, incomplete, cursor })
    }
}

struct Read<'a> {
    bridge: &'a Bridge,
    chat: &'a str,
    limit: usize,
    /// A page token of this read is in use, so a cursor the gateway refuses is a stale token.
    continued: bool,
}

impl Read<'_> {
    async fn page(&self, before: Option<&str>, after: Option<&str>) -> Rpc<HistoryPage> {
        let mut params = json!({"chat": self.chat, "limit": self.limit});
        if let Some(before) = before {
            params["before"] = before.into();
        }
        if let Some(after) = after {
            params["after"] = after.into();
        }
        match self.bridge.gw("messages.history", params).await {
            Err(e) if self.continued && e.reason == Reason::Invalid => Err(stale()),
            r => r,
        }
    }

    /// Walks back from the newest page to the first page that has a message created before
    /// `until`, or to the oldest page. `None` when the chat is empty; `false` when the walk
    /// was cut short.
    async fn walk_back(&self, until: i64) -> Rpc<Option<(HistoryPage, bool)>> {
        let mut reached: Option<HistoryPage> = None;
        let mut before: Option<String> = None;
        for walked in 1.. {
            let page = self.page(before.as_deref(), None).await?;
            let Some(oldest) = page.items.first().map(|m| m.created_ms) else { break };
            let prev = page.prev.clone();
            reached = Some(page);
            if oldest < until {
                break;
            }
            match prev {
                None => break,
                Some(_) if walked >= MAX_WALK_PAGES => return Ok(reached.map(|p| (p, false))),
                Some(prev) => before = Some(prev),
            }
        }
        Ok(reached.map(|p| (p, true)))
    }
}

fn stale() -> RpcError {
    RpcError::new(Reason::UnknownCursor, "the page token does not belong to this read")
}

impl Bridge {
    pub async fn history(&self, params: &Value) -> Rpc {
        let chat_id = str_param(params, "chat_id")?;
        let time = |key: &str| {
            params
                .get(key)
                .and_then(Value::as_i64)
                .filter(|t| *t >= 0)
                .ok_or_else(|| RpcError::invalid(format!("`{key}` is missing")))
        };
        let (from_ms, to_ms) = (time("from_ms")?, time("to_ms")?);
        if from_ms > to_ms || to_ms - from_ms > MAX_RANGE_MS {
            return Err(RpcError::invalid("the range must run forwards and cover at most 31 days"));
        }
        let limit = match params.get("limit") {
            None | Some(Value::Null) => 50,
            Some(l) => match l.as_u64() {
                Some(0) | None => return Err(RpcError::invalid("`limit` must be at least 1")),
                Some(l) => usize::try_from(l).unwrap_or(usize::MAX).min(HISTORY_MAX_LIMIT),
            },
        };
        let order = match params.get("order").and_then(Value::as_str) {
            None | Some("asc") => Order::Asc,
            Some("desc") => Order::Desc,
            Some(_) => return Err(RpcError::invalid("`order` is asc or desc")),
        };
        let chat = self.chat_by_ext(chat_id).await?;
        let token = match params.get("page_token") {
            None | Some(Value::Null) => None,
            Some(t) => {
                let token = t.as_str().and_then(Token::parse).ok_or_else(stale)?;
                let same = token.order == order
                    && token.from_ms == from_ms
                    && token.to_ms == to_ms
                    && token.chat == chat.internal;
                if !same {
                    return Err(stale());
                }
                Some(token)
            }
        };

        let read = Read { bridge: self, chat: &chat.internal, limit, continued: token.is_some() };
        let mut incomplete = token.as_ref().is_some_and(|t| t.incomplete);
        let page = match (&token, order) {
            (Some(t), Order::Asc) => Some(read.page(None, Some(&t.cursor)).await?),
            (Some(t), Order::Desc) => Some(read.page(Some(&t.cursor), None).await?),
            (None, _) => {
                // ascending starts at the range's oldest page, descending at its newest
                let until = if order == Order::Asc { from_ms } else { to_ms };
                match read.walk_back(until).await? {
                    Some((page, whole)) => {
                        // a cut walk only matters when it was looking for the start
                        incomplete = !whole && order == Order::Asc;
                        Some(page)
                    }
                    None => None,
                }
            }
        };
        let Some(page) = page.filter(|p| !p.items.is_empty()) else {
            return Ok(json!({"messages": [], "next_page_token": null, "complete": !incomplete}));
        };

        let oldest = page.items.first().map_or(i64::MAX, |m| m.created_ms);
        let newest = page.items.last().map_or(i64::MIN, |m| m.created_ms);
        // the gateway's cursor to the next page of this read, unless the range ends here
        let cursor = match order {
            Order::Asc if newest < to_ms => page.next.clone(),
            Order::Desc if oldest >= from_ms => page.prev.clone(),
            _ => None,
        };
        let next_page_token = match cursor {
            Some(cursor) if token.as_ref().is_some_and(|t| t.cursor == cursor) => None,
            Some(cursor) => {
                let token = Token { order, from_ms, to_ms, chat: chat.internal.clone(), incomplete, cursor }.encode();
                if token.len() > TOKEN_MAX {
                    return Err(RpcError::new(Reason::Internal, "the gateway's cursor is too long to hand on"));
                }
                Some(token)
            }
            None => None,
        };

        let identity = self.identity().ok();
        let cx = Ctx { me: identity.as_ref(), chat: &chat };
        let in_range =
            page.items.iter().filter(|m| m.chat == chat.internal && (from_ms..to_ms).contains(&m.created_ms));
        let mut messages: Vec<Value> = in_range.map(|m| map::message(m, &cx, false)).collect();
        if order == Order::Desc {
            messages.reverse();
        }
        Ok(json!({"messages": messages, "next_page_token": next_page_token, "complete": !incomplete}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_names_its_read_and_the_gateways_cursor() {
        let t = Token {
            order: Order::Asc,
            from_ms: 10,
            to_ms: 20,
            chat: "7".into(),
            incomplete: true,
            cursor: "p:12:x".into(),
        };
        let s = t.encode();
        assert_eq!(s, "h1:a:10:20:7:1:p:12:x");
        assert_eq!(Token::parse(&s), Some(t));
        let d = Token::parse("h1:d:10:20:7:0:p3").unwrap();
        assert_eq!((d.order, d.incomplete, d.cursor.as_str()), (Order::Desc, false, "p3"));
        for bad in [
            "",
            "p:3",
            "h2:a:10:20:7:0:p3",
            "h1:x:10:20:7:0:p3",
            "h1:a:ten:20:7:0:p3",
            "h1:a:10:20:7:2:p3",
            "h1:a:10:20:7:0:",
            "h1:a:10:20:7:0",
        ] {
            assert_eq!(Token::parse(bad), None, "{bad}");
        }
    }
}
