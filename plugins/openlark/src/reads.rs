//! The request methods other than the feed's: identity, chats, single messages, people, and
//! the way from a method name to its handler.

use futures::StreamExt;
use serde_json::{Value, json};

use crate::bridge::{self, Bridge};
use crate::ext::proto::{MAX_IDS_PER_CALL, Reason, Rpc, RpcError, str_param};
use crate::gateway::wire::{Chat, Message, People};
use crate::map::{self, Ctx};

/// Gateway reads of one `messages.get` that run at once.
const GET_PARALLEL: usize = 8;

impl Bridge {
    /// Every request except `hello`, `ping` and the feed's two (§4).
    pub async fn dispatch(&self, method: &str, params: &Value) -> Rpc {
        let feature = match method {
            "me.get" => "me",
            "chats.get" | "messages.get" | "messages.history" | "people.get" => method,
            "messages.send" | "messages.reply" | "messages.recall" => method,
            "reactions.add" | "reactions.remove" => "reactions",
            _ => return Err(RpcError::new(Reason::Unsupported, "this server does not have that method")),
        };
        // a method whose feature is not announced is not there (§3.2)
        if !bridge::features(&self.link.state()).contains(&feature) {
            return Err(RpcError::new(Reason::Unsupported, "the gateway or its credential does not offer this"));
        }
        match method {
            "me.get" => self.me_get(),
            "chats.get" => self.chats_get(params).await,
            "messages.get" => self.messages_get(params).await,
            "messages.history" => self.history(params).await,
            "people.get" => self.people_get(params).await,
            "messages.send" => self.send("messages.send", params).await,
            "messages.reply" => self.send("messages.reply", params).await,
            "messages.recall" => self.recall(params).await,
            "reactions.add" => self.react("reactions.add", params).await,
            _ => self.react("reactions.remove", params).await,
        }
    }

    fn me_get(&self) -> Rpc {
        let me = self.identity()?;
        Ok(json!({"realm": map::realm(&me.tenant_id), "user": map::me_user(&me)}))
    }

    /// Always asks the gateway, so that a renamed chat shows its new name.
    async fn chats_get(&self, params: &Value) -> Rpc {
        let known = self.chat_by_ext(str_param(params, "chat_id")?).await?;
        let chat: Chat = self.gw("chats.get", json!({"chat": known.internal})).await?;
        Ok(map::chat(&self.remember(&chat).await))
    }

    /// Bodies by `source_id`, in the order asked; `null` for what cannot be found or seen. The
    /// gateway knows no platform message id, so an item with only `id` is `null` too.
    async fn messages_get(&self, params: &Value) -> Rpc {
        let items = params.get("items").and_then(Value::as_array).ok_or_else(|| RpcError::invalid("`items` is missing"))?;
        if items.len() > MAX_IDS_PER_CALL {
            return Err(RpcError::invalid("more items than max_ids_per_call"));
        }
        let identity = self.identity().ok();
        let me = identity.as_ref();
        let one = |item: &Value| {
            let chat_id = item.get("chat_id").and_then(Value::as_str).map(str::to_string);
            let source_id = item.get("source_id").and_then(Value::as_str).map(str::to_string);
            async move {
                let chat_id = chat_id.ok_or_else(|| RpcError::invalid("an item without `chat_id`"))?;
                let Some(source_id) = source_id.filter(|s| map::gateway_id(s)) else { return Ok(Value::Null) };
                let chat = match self.chat_by_ext(&chat_id).await {
                    Ok(chat) => chat,
                    Err(e) if e.reason == Reason::NotFound => return Ok(Value::Null),
                    Err(e) => return Err(e),
                };
                match self.gw::<Message>("messages.get", json!({"message": source_id})).await {
                    Ok(m) if m.chat == chat.internal => Ok(map::message(&m, &Ctx { me, chat: &chat }, false)),
                    Ok(_) => Ok(Value::Null),
                    Err(e) if matches!(e.reason, Reason::NotFound | Reason::Invalid) => Ok(Value::Null),
                    Err(e) => Err(e),
                }
            }
        };
        let results: Vec<Rpc> = futures::stream::iter(items.iter().map(one)).buffered(GET_PARALLEL).collect().await;
        let messages = results.into_iter().collect::<Rpc<Vec<Value>>>()?;
        Ok(json!({"messages": messages}))
    }

    /// Profiles by the `id` handle. The gateway looks people up by its own id only, so any other
    /// handle is `null`.
    async fn people_get(&self, params: &Value) -> Rpc {
        let users = params.get("users").and_then(Value::as_array).ok_or_else(|| RpcError::invalid("`users` is missing"))?;
        if users.len() > MAX_IDS_PER_CALL {
            return Err(RpcError::invalid("more users than max_ids_per_call"));
        }
        let wanted: Vec<Option<&str>> = users
            .iter()
            .map(|u| match (u.get("kind").and_then(Value::as_str), u.get("value").and_then(Value::as_str)) {
                (Some("id"), Some(id)) if map::gateway_id(id) => Some(id),
                _ => None,
            })
            .collect();
        let mut ids: Vec<&str> = wanted.iter().flatten().copied().collect();
        ids.sort_unstable();
        ids.dedup();
        let found = if ids.is_empty() { People::default() } else { self.gw("people.batch", json!({"users": ids})).await? };
        let identity = self.identity().ok();
        let people: Vec<Value> = wanted
            .iter()
            .map(|id| {
                let person = id.and_then(|id| found.people.iter().find(|p| p.user.id == id));
                person.map_or(Value::Null, |p| map::person(p, identity.as_ref()))
            })
            .collect();
        Ok(json!({"people": people, "missing_permissions": []}))
    }
}
