//! Loopback-only controlled gateway for manually exercising the real plugin binary.
//! It never connects to Lark, a real account or an installed Steve service.
#[path = "../tests/common/mod.rs"]
mod common;

use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    routing::{get, post},
};
use common::{Fake, GRACE, GROUP, GW_TOKEN, Gateway, HUB_TOKEN, ME, now_ms};
use serde_json::{Value, json};
use tokio::net::TcpListener;

type F = Extension<Arc<Fake>>;

async fn message(Extension(fake): F, Json(body): Json<Value>) -> Json<Value> {
    let chat = body["chat"].as_str().unwrap_or(GROUP);
    let sender = body["sender"].as_str().unwrap_or(GRACE);
    let text = body["text"].as_str().unwrap_or("fixture question");
    let created = body["created_ms"].as_i64().unwrap_or_else(now_ms);
    Json(fake.post_at(chat, sender, text, created))
}

async fn reaction(Extension(fake): F, Json(body): Json<Value>) -> Json<Value> {
    let at = body["action_ms"].as_i64().unwrap_or_else(now_ms);
    let seq = fake.append(json!({"kind":"reaction",
    "message_id":body["message"].as_str().unwrap_or("501"),
    "chat":body["chat"].as_str().unwrap_or(GROUP),
    "reaction":{
        "actor":{"id":body["actor"].as_str().unwrap_or(ME)},
        "emoji":body["emoji"].as_str().unwrap_or("THUMBSUP"),
        "change":body["change"].as_str().unwrap_or("created"),
        "action_ms":at,
        "origin":body["origin"].as_str().unwrap_or("unknown"),
        "time_source":body["time_source"].as_str().unwrap_or("observed")
    }}));
    Json(json!({"seq":seq,"action_ms":at}))
}

async fn state(Extension(fake): F) -> Json<Value> {
    let world = fake.w();
    Json(json!({"head":world.head,"journal":world.journal,"acks":world.acks,
        "methods":world.calls.iter().map(|(m,_)|m).collect::<Vec<_>>()}))
}

async fn disconnect(Extension(fake): F) -> Json<Value> {
    fake.kick();
    Json(json!({"disconnected":true}))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut gateway = Gateway::start(|w| w.features.push("reaction_changes.v1".into())).await;
    let files = tempfile::tempdir()?;
    let token = files.path().join("hub-token");
    let config = files.path().join("config.json");
    let data = files.path().join("plugin-data");
    std::fs::write(&token, HUB_TOKEN)?;
    std::fs::write(
        &config,
        json!({"gateway_url":format!("ws://{}",gateway.addr),"gateway_token":GW_TOKEN}).to_string(),
    )?;
    std::fs::create_dir(&data)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    println!(
        "{}",
        json!({"kind":"isolated_fake_gateway","gateway":format!("ws://{}",gateway.addr),
        "control":format!("http://{}",listener.local_addr()?),"token_file":token,"config":config,"data":data})
    );
    let routes = Router::new()
        .route("/message", post(message))
        .route("/reaction", post(reaction))
        .route("/state", get(state))
        .route("/disconnect", post(disconnect))
        .layer(Extension(gateway.fake.clone()));
    axum::serve(listener, routes)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    gateway.stop().await;
    Ok(())
}
