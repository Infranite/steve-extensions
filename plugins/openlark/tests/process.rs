//! The run contract of the binary itself: its settings by flag and by environment, what ends
//! it, and what does not.

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::oneshot;

const BIN: &str = env!("CARGO_BIN_EXE_steve-plugin-openlark");

/// The files the plugin is started with.
struct Files {
    dir: tempfile::TempDir,
    token: PathBuf,
    config: PathBuf,
    data: PathBuf,
}

fn files(gateway: SocketAddr) -> Files {
    let dir = tempfile::tempdir().unwrap();
    let (token, config, data) = (dir.path().join("token"), dir.path().join("config.json"), dir.path().join("data"));
    std::fs::write(&token, format!("{HUB_TOKEN}\n")).unwrap();
    let settings = json!({"gateway_url": format!("ws://{gateway}"), "gateway_token": GW_TOKEN});
    std::fs::write(&config, settings.to_string()).unwrap();
    Files { dir, token, config, data }
}

/// The binary, with nothing of the caller's environment that it would read.
fn plugin() -> Command {
    let mut command = Command::new(BIN);
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("STEVE_PLUGIN_") {
            command.env_remove(name);
        }
    }
    command.env_remove("RUST_LOG").stdout(Stdio::null()).stderr(Stdio::piped()).kill_on_drop(true);
    command
}

fn by_flags(files: &Files, listen: &str) -> Command {
    let mut command = plugin();
    command.args(["--listen", listen]);
    command.arg("--token-file").arg(&files.token).arg("--config").arg(&files.config).arg("--data").arg(&files.data);
    command
}

fn by_env(files: &Files, listen: &str) -> Command {
    let mut command = plugin();
    command
        .env("STEVE_PLUGIN_LISTEN", listen)
        .env("STEVE_PLUGIN_TOKEN_FILE", &files.token)
        .env("STEVE_PLUGIN_CONFIG", &files.config)
        .env("STEVE_PLUGIN_DATA", &files.data);
    command
}

struct Plugin {
    child: Child,
    /// Held open the way the Hub holds it; `None` when the plugin got an input already at its end.
    stdin: Option<ChildStdin>,
    /// Where it says it serves, when it got that far.
    addr: Option<SocketAddr>,
    log: Arc<Mutex<String>>,
}

impl Plugin {
    /// Starts it and waits until it serves or its standard error ends.
    async fn start(mut command: Command, hold_stdin: bool) -> Plugin {
        command.stdin(if hold_stdin { Stdio::piped() } else { Stdio::null() });
        let mut child = command.spawn().expect("the plugin binary starts");
        let stdin = child.stdin.take();
        let stderr = child.stderr.take().unwrap();
        let log = Arc::new(Mutex::new(String::new()));
        let (serving, served) = oneshot::channel::<SocketAddr>();
        let kept = log.clone();
        tokio::spawn(async move {
            let mut serving = Some(serving);
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                // with port 0, this line is how a caller learns the port
                if let Some(at) = line.find("serving the Hub on ws://") {
                    let addr = line[at..].trim_start_matches("serving the Hub on ws://").trim_end_matches("/ws");
                    if let Some(serving) = serving.take() {
                        let _ = serving.send(addr.parse().expect("an address in the serving line"));
                    }
                }
                let mut kept = kept.lock().unwrap();
                kept.push_str(&line);
                kept.push('\n');
            }
        });
        let addr = tokio::time::timeout(Duration::from_secs(20), served).await.ok().and_then(Result::ok);
        Plugin { child, stdin, addr, log }
    }

    fn addr(&self) -> SocketAddr {
        self.addr.unwrap_or_else(|| panic!("the plugin does not serve:\n{}", self.log()))
    }

    fn log(&self) -> String {
        self.log.lock().unwrap().clone()
    }

    fn running(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    fn signal(&self, name: &str) {
        let pid = self.child.id().expect("the plugin is running").to_string();
        assert!(std::process::Command::new("kill").args([name, &pid]).status().unwrap().success());
    }

    /// Its exit code, once it ends.
    async fn exit(&mut self) -> Option<i32> {
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait()).await;
        let status = status.unwrap_or_else(|_| panic!("the plugin does not exit:\n{}", self.log())).unwrap();
        // everything it wrote is read before the log is looked at
        tokio::time::sleep(Duration::from_millis(100)).await;
        status.code()
    }

    fn says_no_secret(&self) {
        let log = self.log();
        assert!(!log.is_empty());
        assert!(!log.contains(HUB_TOKEN) && !log.contains(GW_TOKEN), "a token is in the log:\n{log}");
    }
}

async fn port_is_free(addr: SocketAddr) -> bool {
    for _ in 0..50 {
        if tokio::net::TcpListener::bind(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// Under the Hub: standard input at its end means the Hub is gone, and the plugin goes too.
#[tokio::test]
async fn a_supervised_plugin_ends_when_its_standard_input_does() {
    let gw = Gateway::start(|_| {}).await;
    let files = files(gw.addr);
    let mut with_flag = by_flags(&files, "127.0.0.1:0");
    with_flag.arg("--supervised");
    let mut with_env = by_env(&files, "127.0.0.1:0");
    // the finest level, to see that no secret is logged at any
    with_env.env("STEVE_PLUGIN_SUPERVISED", "1").env("RUST_LOG", "trace");

    for command in [with_flag, with_env] {
        let mut plugin = Plugin::start(command, true).await;
        let addr = plugin.addr();
        let mut hub = Hub::connect(addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
        let hello = hub.hello().await.unwrap();
        assert_eq!((hello["realm"].as_str(), hello["session"]["status"].as_str()), (Some("lark-t1"), Some("ok")));
        hub.subscribe_now().await;
        gw.fake.post(GROUP, GRACE, "hello");
        assert_eq!(hub.items(1).await[0]["message"]["text"], "hello");
        // input the Hub writes is not a reason to stop, only its end is
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(plugin.running());

        drop(plugin.stdin.take());
        assert_eq!(hub.until_closed().await.0, Some(1001));
        assert_eq!(plugin.exit().await, Some(0));
        assert!(port_is_free(addr).await);
        assert!(plugin.log().contains("standard input closed"));
        plugin.says_no_secret();
    }
    // the state it keeps is in its data directory, and neither token is
    let state = std::fs::read_to_string(files.data.join("state.json")).unwrap();
    assert!(state.contains("t1") && !state.contains(GW_TOKEN) && !state.contains(HUB_TOKEN));
}

/// By hand or under a service manager: standard input is at its end from the start, and says
/// nothing.
#[tokio::test]
async fn an_unsupervised_plugin_ignores_standard_input_and_ends_on_a_signal() {
    let gw = Gateway::start(|_| {}).await;
    let files = files(gw.addr);
    let plain = by_flags(&files, "127.0.0.1:0");
    let mut switched_off = by_env(&files, "127.0.0.1:0");
    switched_off.env("STEVE_PLUGIN_SUPERVISED", "0");
    let mut left_empty = by_env(&files, "127.0.0.1:0");
    left_empty.env("STEVE_PLUGIN_SUPERVISED", "");

    for (command, signal) in [(plain, "-TERM"), (switched_off, "-INT"), (left_empty, "-TERM")] {
        let mut plugin = Plugin::start(command, false).await;
        let addr = plugin.addr();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(plugin.running(), "it ended on its own:\n{}", plugin.log());
        let mut hub = Hub::connect(addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
        hub.hello().await.unwrap();

        plugin.signal(signal);
        assert_eq!(hub.until_closed().await.0, Some(1001));
        assert_eq!(plugin.exit().await, Some(0), "{signal}");
        assert!(port_is_free(addr).await);
        plugin.says_no_secret();
    }
}

#[tokio::test]
async fn a_plugin_that_cannot_start_says_why_and_exits() {
    let gw = Gateway::start(|_| {}).await;
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let taken = taken.local_addr().unwrap().to_string();

    let cases: Vec<(&str, Box<dyn Fn(&Files) -> Command>, &str)> = vec![
        (
            "no token file",
            Box::new(|f| {
                std::fs::remove_file(&f.token).unwrap();
                by_flags(f, "127.0.0.1:0")
            }),
            "the token file",
        ),
        (
            "a token too short to be one",
            Box::new(|f| {
                std::fs::write(&f.token, "short").unwrap();
                by_flags(f, "127.0.0.1:0")
            }),
            "the token file",
        ),
        (
            "a config that is not JSON",
            Box::new(|f| {
                std::fs::write(&f.config, format!("gateway_token = {GW_TOKEN}")).unwrap();
                by_env(f, "127.0.0.1:0")
            }),
            "the config file",
        ),
        (
            "a config without the credential",
            Box::new(|f| {
                std::fs::write(&f.config, r#"{"gateway_url":"ws://127.0.0.1:9"}"#).unwrap();
                by_flags(f, "127.0.0.1:0")
            }),
            "gateway_token",
        ),
        (
            "a gateway across the network in the clear",
            Box::new(|f| {
                let settings = json!({"gateway_url": "ws://gateway.example.com:7800", "gateway_token": GW_TOKEN});
                std::fs::write(&f.config, settings.to_string()).unwrap();
                by_flags(f, "127.0.0.1:0")
            }),
            "gateway_url",
        ),
        (
            "a data directory that cannot be one",
            Box::new(|f| {
                std::fs::write(&f.data, "a file").unwrap();
                by_flags(f, "127.0.0.1:0")
            }),
            "the data directory",
        ),
        ("every interface", Box::new(|f| by_flags(f, "0.0.0.0:0")), "not a loopback address"),
        ("a name instead of an address", Box::new(|f| by_env(f, "localhost:7801")), "listen address"),
        ("a port somebody else has", Box::new(move |f| by_flags(f, &taken)), "cannot listen"),
    ];
    for (what, command, said) in cases {
        let files = files(gw.addr);
        let mut plugin = Plugin::start(command(&files), false).await;
        assert!(plugin.addr.is_none(), "{what}: it serves");
        assert_eq!(plugin.exit().await, Some(2), "{what}");
        assert!(plugin.log().contains(said), "{what}:\n{}", plugin.log());
        plugin.says_no_secret();
        drop(files.dir);
    }
    // a setting that is missing altogether is a usage error
    let mut plugin = Plugin::start(plugin(), false).await;
    assert!(plugin.exit().await.is_some_and(|code| code != 0));
    // none of them got as far as the gateway
    assert!(gw.fake.w().calls.is_empty());
}

/// §9.2: a gateway that refuses the credential is a state the Hub is told. The plugin stays,
/// and it mends when the credential does.
#[tokio::test]
async fn a_refused_credential_does_not_end_the_plugin() {
    let gw = Gateway::start(|w| w.token = "olk_another_credential_02".into()).await;
    let files = files(gw.addr);
    let mut plugin = Plugin::start(by_flags(&files, "127.0.0.1:0"), false).await;
    let addr = plugin.addr();

    let mut hub = Hub::connect(addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
    hub.ask("hello", json!({"protocol": {"min": 1, "max": 1}, "client": {"name": "steve-hub"}})).await;
    let (code, frames) = hub.until_closed().await;
    assert_eq!(reason(&frames[0]["error"]), "session_expired");
    assert_eq!(frames[0]["error"]["data"]["detail"], "gateway_credential_refused");
    assert_eq!(code, Some(1013));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(plugin.running(), "it ended:\n{}", plugin.log());

    gw.fake.w().token = GW_TOKEN.into();
    let accepted = tokio::time::Instant::now();
    loop {
        let mut hub = Hub::connect(addr, "/ws", Some(HUB_TOKEN)).await.unwrap();
        match hub.hello().await {
            Ok(hello) => break assert_eq!(hello["session"]["status"], "ok"),
            Err(_) if accepted.elapsed() < Duration::from_secs(15) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("the credential is accepted, and hello still says {e}"),
        }
    }
    plugin.signal("-TERM");
    assert_eq!(plugin.exit().await, Some(0));
    plugin.says_no_secret();
}
