//! A protocol client for the isolated reaction_gateway and the real plugin process.
//! Usage: reaction_watch <plugin ws URL> <token file> [resume cursor]. Prints no credential.
use std::io::Write;

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

fn shown(value: &Value) {
    println!("{value}");
    let _ = std::io::stdout().flush();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !(2..=3).contains(&args.len()) {
        return Err("usage: reaction_watch <plugin ws URL> <token file> [resume cursor]".into());
    }
    let token = std::fs::read_to_string(&args[1])?;
    let mut request = args[0].as_str().into_client_request()?;
    request.headers_mut().insert("Authorization", format!("Bearer {}", token.trim()).parse()?);
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":1,"method":"hello",
        "params":{"protocol":{"min":1,"max":1},"client":{"name":"manual-reaction-watch"}}})
            .to_string()
            .into(),
        ))
        .await?;
    let mut next_id = 3_u64;
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>{ socket.close(None).await?;break; }
            _=tick.tick()=>{
                socket.send(Message::Text(json!({"jsonrpc":"2.0","id":next_id,"method":"ping","params":{}}).to_string().into())).await?;
                next_id+=1;
            }
            frame=socket.next()=>{
                let Some(frame)=frame else {break};
                match frame? {
                    Message::Text(text)=>{
                        let frame:Value=serde_json::from_str(text.as_str())?;
                        if frame["id"]==1 {
                            shown(&frame);
                            if frame.get("error").is_some(){return Err("plugin hello failed".into())}
                            let from=args.get(2).map_or(json!("now"),|cursor|json!({"cursor":cursor}));
                            socket.send(Message::Text(json!({"jsonrpc":"2.0","id":2,"method":"changes.subscribe","params":{"from":from}}).to_string().into())).await?;
                        } else if frame["id"]==2 {
                            shown(&frame);
                            if frame.get("error").is_some(){return Err("plugin subscription failed".into())}
                        } else if frame["method"]=="changes.batch" {
                            shown(&frame);
                            let p=&frame["params"];
                            socket.send(Message::Text(json!({"jsonrpc":"2.0","id":next_id,"method":"changes.ack",
                                "params":{"subscription":p["subscription"],"seq":p["seq"]}}).to_string().into())).await?;
                            next_id+=1;
                        } else if frame.get("error").is_some() {
                            shown(&frame);
                            return Err("plugin request failed".into());
                        } else if frame.get("method").is_some() { shown(&frame); }
                    }
                    Message::Close(_)=>break,
                    _=>{}
                }
            }
        }
    }
    Ok(())
}
