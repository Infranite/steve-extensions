//! Translation of the gateway's optional immutable reaction actions. The gateway owns upstream
//! parsing, history and provenance; the bridge never reconstructs them from current state.

use serde_json::{Value, json};

use crate::ext::proto::opaque_ok;
use crate::gateway::link::Identity;
use crate::gateway::wire::{Change, Reaction, ReactionOrigin};
use crate::map::{self, ChatInfo};

/// Decode a single action without silently accepting incomplete metadata from an older gateway.
pub fn action(change: &Change) -> Result<Reaction, &'static str> {
    let target = change.message_id.as_deref().ok_or("target message is missing")?;
    if !map::gateway_id(target) {
        return Err("target message is invalid");
    }
    let reaction: Reaction = serde_json::from_value(change.reaction.clone().ok_or("action details are missing")?)
        .map_err(|_| "action details are invalid")?;
    if !map::gateway_id(&reaction.actor.id) {
        return Err("actor is invalid");
    }
    if !opaque_ok(&reaction.emoji, 128) {
        return Err("emoji is invalid");
    }
    if reaction.action_ms <= 0 {
        return Err("action time is invalid");
    }
    if change.seq == 0 || change.seq > i64::MAX as u64 {
        return Err("revision is invalid");
    }
    Ok(reaction)
}

/// Uses the action's journal seq, not the last seq of the transport batch. All timestamps and
/// provenance are preserved; only a positively identified bot/app is conservatively automated.
pub fn item(change: &Change, action: &Reaction, chat: &ChatInfo, me: &Identity) -> Value {
    let origin = if matches!(action.actor.kind.as_deref(), Some("bot" | "app")) {
        ReactionOrigin::Automated
    } else {
        action.origin
    };
    json!({
        "type": "reaction",
        "chat_id": chat.ext_id,
        "source_id": change.message_id,
        "actor": map::sender(&action.actor, Some(me)),
        "emoji": action.emoji,
        "change": action.change,
        "action_ms": action.action_ms,
        "origin": origin,
        "revision": change.seq,
        "time_source": action.time_source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::wire::{ReactionAction, ReactionTimeSource, User};

    fn event() -> Change {
        serde_json::from_value(json!({"seq":17,"kind":"reaction","message_id":"501","chat":"7",
            "reaction":{"actor":{"id":"100"},"emoji":"THUMBSUP","change":"created",
                "action_ms":1762430401234_i64,"time_source":"provider","origin":"manual"}}))
        .unwrap()
    }

    fn identity() -> Identity {
        Identity {
            user: User { id: "100".into(), union_id: Some("on_ada".into()), ..User::default() },
            tenant_id: "t1".into(),
        }
    }

    fn chat() -> ChatInfo {
        ChatInfo { internal: "7".into(), ext_id: "oc_group".into(), kind: "group".into(), name: None, external: None }
    }

    #[test]
    fn a_reaction_preserves_target_origin_revision_and_provider_time() {
        let change = event();
        let action = action(&change).unwrap();
        let item = item(&change, &action, &chat(), &identity());
        assert_eq!(item["source_id"], "501");
        assert_eq!(item["revision"], 17);
        assert_eq!(item["origin"], "manual");
        assert_eq!(item["action_ms"], 1762430401234_i64);
        assert_eq!(item["time_source"], "provider");
        assert_eq!(item["actor"]["aliases"], json!([{"kind":"platform","system":"feishu","value":"on_ada"}]));
        assert!(item.get("id").is_none(), "no platform message id was supplied");
    }

    #[test]
    fn unknown_and_missing_origins_are_context_not_manual() {
        for origin in [None, Some(json!("unknown")), Some(json!("future_source"))] {
            let mut change = event();
            let details = change.reaction.as_mut().unwrap().as_object_mut().unwrap();
            details.remove("origin");
            if let Some(origin) = origin {
                details.insert("origin".into(), origin);
            }
            let action = action(&change).unwrap();
            assert_eq!(item(&change, &action, &chat(), &identity())["origin"], "unknown");
        }
    }

    #[test]
    fn removal_and_observed_time_are_not_rewritten() {
        let change = event();
        let mut action = action(&change).unwrap();
        action.change = ReactionAction::Removed;
        action.time_source = ReactionTimeSource::Observed;
        action.origin = ReactionOrigin::Automated;
        let item = item(&change, &action, &chat(), &identity());
        assert_eq!(item["change"], "removed");
        assert_eq!(item["time_source"], "observed");
        assert_eq!(item["origin"], "automated");
        assert_eq!(item["action_ms"], action.action_ms);
    }

    #[test]
    fn an_app_actor_is_never_promoted_to_a_manual_response() {
        let change = event();
        let mut action = action(&change).unwrap();
        action.actor.kind = Some("app".into());
        let item = item(&change, &action, &chat(), &identity());
        assert_eq!(item["origin"], "automated");
        assert_eq!(item["actor"]["is_bot"], true);
    }

    #[test]
    fn incomplete_metadata_never_acquires_a_time_or_a_source() {
        let mut missing = event();
        missing.reaction = None;
        assert_eq!(action(&missing), Err("action details are missing"));
        for field in ["actor", "emoji", "change", "action_ms", "time_source"] {
            let mut change = event();
            change.reaction.as_mut().unwrap().as_object_mut().unwrap().remove(field);
            assert!(action(&change).is_err(), "accepted missing {field}");
        }
        for (field, value) in [
            ("emoji", json!("bad\nemoji")),
            ("action_ms", json!(0)),
            ("change", json!("update")),
            ("time_source", json!("guessed")),
        ] {
            let mut change = event();
            change.reaction.as_mut().unwrap()[field] = value;
            assert!(action(&change).is_err(), "accepted invalid {field}");
        }
        for seq in [0, i64::MAX as u64 + 1, u64::MAX] {
            let mut change = event();
            change.seq = seq;
            assert_eq!(action(&change), Err("revision is invalid"));
        }
    }
}
