//! The bridge's standing connection to the gateway: it reconnects, knows who is signed in and
//! keeps the session and push state the Hub is told about.

use std::path::PathBuf;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, watch};
use tokio::time::Instant;

use super::client::{self, GwConn, GwError};
use super::wire::{Me, PushStatus, User};
use crate::config::Settings;
use crate::store;

/// The state of the user's sign-in at the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Session {
    Ok,
    Expired,
    Locked,
}

impl Session {
    fn parse(s: &str) -> Session {
        match s {
            "expired" => Session::Expired,
            "locked" => Session::Locked,
            _ => Session::Ok,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkStatus {
    /// Not connected yet since the start.
    Connecting,
    Up,
    Down,
    /// The gateway does not accept the plugin's credential.
    Refused,
}

/// Who is signed in at the gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub user: User,
    pub tenant_id: String,
}

impl Identity {
    fn same_account(&self, other: &Identity) -> bool {
        self.user.id == other.user.id && self.tenant_id == other.tenant_id
    }
}

#[derive(Clone)]
pub struct LinkState {
    pub status: LinkStatus,
    pub session: Session,
    /// Whether the gateway's push stream from Lark is connected, when it says.
    pub push_connected: Option<bool>,
    pub identity: Option<Identity>,
    /// The gateway's features and the credential's scopes, as last seen.
    pub features: Vec<String>,
    pub scopes: Vec<String>,
    pub conn: Option<Arc<GwConn>>,
}

impl LinkState {
    pub fn has_feature(&self, f: &str) -> bool {
        self.features.iter().any(|x| x == f)
    }

    pub fn has_scope(&self, s: &str) -> bool {
        self.scopes.iter().any(|x| x == s)
    }
}

/// What survives a restart, so that hello can answer while the gateway is away.
#[derive(Serialize, Deserialize)]
struct Saved {
    identity: Identity,
    features: Vec<String>,
    scopes: Vec<String>,
}

pub struct Link {
    settings: Settings,
    state: watch::Sender<LinkState>,
    /// One health check at a time; holds when the last one ended.
    checking: Mutex<Option<Instant>>,
    saved_path: PathBuf,
    /// What the file holds, so that an unchanged account is not written again.
    saved_last: std::sync::Mutex<String>,
}

impl Link {
    /// Loads what the last run knew and starts connecting.
    pub fn start(settings: &Settings, stop: watch::Receiver<bool>) -> Arc<Link> {
        let saved_path = settings.data.join("state.json");
        let saved: Option<Saved> = store::read_json(&saved_path);
        let (identity, features, scopes) = match saved {
            Some(s) => (Some(s.identity), s.features, s.scopes),
            None => (None, Vec::new(), Vec::new()),
        };
        let (state, _) = watch::channel(LinkState {
            status: LinkStatus::Connecting,
            session: Session::Ok,
            push_connected: None,
            identity,
            features,
            scopes,
            conn: None,
        });
        let link = Arc::new(Link {
            settings: settings.clone(),
            state,
            checking: Mutex::new(None),
            saved_path,
            saved_last: std::sync::Mutex::new(String::new()),
        });
        tokio::spawn(link.clone().supervise(stop));
        link
    }

    pub fn state(&self) -> LinkState {
        self.state.borrow().clone()
    }

    pub fn watch(&self) -> watch::Receiver<LinkState> {
        self.state.subscribe()
    }

    /// One gateway call over the standing connection.
    pub async fn call<T: DeserializeOwned>(&self, method: &'static str, params: Value) -> Result<T, GwError> {
        let conn = {
            let st = self.state.borrow();
            match (st.status, &st.conn) {
                (LinkStatus::Refused, _) => return Err(GwError::Unauthorized),
                (LinkStatus::Up, Some(conn)) => conn.clone(),
                _ => return Err(GwError::Down),
            }
        };
        conn.call(method, params).await
    }

    async fn supervise(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        let timing = self.settings.timing.clone();
        let mut backoff = timing.reconnect_min;
        loop {
            let attempt = client::connect(&self.settings.gateway, &self.settings.gateway_token, &timing, None);
            let ended = tokio::select! {
                r = attempt => match r {
                    Ok(conn) => {
                        backoff = timing.reconnect_min;
                        let conn = Arc::new(conn);
                        self.adopt(conn.clone()).await;
                        tracing::info!("connected to the gateway");
                        let mut tick = tokio::time::interval(timing.health_tick);
                        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        tick.tick().await;
                        loop {
                            tokio::select! {
                                e = conn.closed() => break e,
                                _ = tick.tick() => {
                                    if let Err(e) = self.check().await {
                                        break e;
                                    }
                                }
                                _ = stop.changed() => return,
                            }
                        }
                    }
                    Err(e) => e,
                },
                _ = stop.changed() => return,
            };
            let status = if ended == GwError::Unauthorized { LinkStatus::Refused } else { LinkStatus::Down };
            let was = self.state.borrow().status;
            if was != status {
                tracing::warn!(
                    "the gateway link is {}: {ended}",
                    if status == LinkStatus::Refused { "refused" } else { "down" }
                );
            }
            self.state.send_modify(|st| {
                st.status = status;
                st.conn = None;
            });
            tokio::select! {
                () = tokio::time::sleep(backoff) => {}
                _ = stop.changed() => return,
            }
            backoff = (backoff * 2).min(timing.reconnect_max);
        }
    }

    /// Takes a fresh connection into use: its hello says the session, `me.get` who is signed in.
    async fn adopt(&self, conn: Arc<GwConn>) {
        let hello = conn.hello.clone();
        let session = Session::parse(&hello.account.session);
        let mut identity = match (hello.account.user, hello.account.tenant) {
            (Some(user), Some(tenant)) if !user.id.is_empty() && !tenant.id.is_empty() => {
                Some(Identity { user, tenant_id: tenant.id })
            }
            _ => None,
        };
        let mut from_me = false;
        if session == Session::Ok
            && let Ok(me) = conn.call::<Me>("me.get", json!({})).await
        {
            identity = Some(Identity { user: me.user, tenant_id: me.tenant.id });
            from_me = true;
        }
        let push_connected = self.push_status(&conn, &hello.features, &hello.scopes).await.ok().flatten();
        self.state.send_modify(|st| {
            st.status = LinkStatus::Up;
            st.session = session;
            st.push_connected = push_connected;
            st.features = hello.features;
            st.scopes = hello.scopes;
            st.conn = Some(conn);
            if let Some(new) = identity {
                // hello alone has fewer of the user's ids than me.get gave earlier
                let keep = !from_me && st.identity.as_ref().is_some_and(|old| old.same_account(&new));
                if !keep {
                    st.identity = Some(new);
                }
            }
        });
        self.save();
    }

    /// Reads the session and push state again. `Err` when the connection is of no use any more.
    ///
    /// The gateway names the kind of a broken session only in hello, so a `session` answer is
    /// followed by a short second connection that reads it there.
    pub async fn check(&self) -> Result<(), GwError> {
        let mut last = self.checking.lock().await;
        if last.is_some_and(|at| at.elapsed() < self.settings.timing.health_tick / 8) {
            return Ok(());
        }
        let st = self.state();
        let Some(conn) = st.conn.clone() else { return Err(GwError::Down) };
        let mut session = st.session;
        let mut identity = None;
        match conn.call::<Me>("me.get", json!({})).await {
            Ok(me) => {
                session = Session::Ok;
                identity = Some(Identity { user: me.user, tenant_id: me.tenant.id });
            }
            Err(e) if e.code() == Some("session") => {
                let probe =
                    client::connect(&self.settings.gateway, &self.settings.gateway_token, &self.settings.timing, None);
                session = match probe.await.map(|c| Session::parse(&c.hello.account.session)) {
                    Ok(Session::Ok) | Err(_) if session != Session::Ok => session,
                    Ok(Session::Ok) | Err(_) => Session::Expired,
                    Ok(kind) => kind,
                };
            }
            Err(GwError::Rpc { .. }) => {}
            Err(e) => return Err(e),
        }
        let push = self.push_status(&conn, &st.features, &st.scopes).await?;
        self.state.send_modify(|now| {
            now.session = session;
            if push.is_some() {
                now.push_connected = push;
            }
            if identity.is_some() {
                now.identity = identity;
            }
        });
        self.save();
        *last = Some(Instant::now());
        Ok(())
    }

    /// `Ok(None)` when the gateway or the credential does not offer the push state.
    async fn push_status(
        &self,
        conn: &GwConn,
        features: &[String],
        scopes: &[String],
    ) -> Result<Option<bool>, GwError> {
        if !features.iter().any(|f| f == "push_status.v1") || !scopes.iter().any(|s| s == "messages.read") {
            return Ok(None);
        }
        match conn.call::<PushStatus>("push.status", json!({})).await {
            Ok(p) => Ok(Some(p.connected)),
            Err(GwError::Rpc { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn save(&self) {
        let st = self.state.borrow();
        let Some(identity) = st.identity.clone() else { return };
        let saved = Saved { identity, features: st.features.clone(), scopes: st.scopes.clone() };
        drop(st);
        let text = serde_json::to_string(&saved).unwrap_or_default();
        let mut last = self.saved_last.lock().unwrap_or_else(|e| e.into_inner());
        if *last == text {
            return;
        }
        match store::write_json(&self.saved_path, &saved) {
            Ok(()) => *last = text,
            Err(e) => tracing::warn!("the account could not be saved: {e}"),
        }
    }
}
