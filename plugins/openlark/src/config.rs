//! The run contract: where to listen, the token the Hub presents, how to reach the gateway and
//! where state may be written. Every setting is an environment variable with an equivalent flag.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use serde_json::Value;
use url::Url;

#[derive(Parser, Debug, Clone)]
#[command(name = "steve-plugin-openlark", version, about = "Steve channel plugin: Feishu / Lark as you")]
pub struct Args {
    /// The address to serve the Hub on, `127.0.0.1:<port>`. Only a loopback address is accepted.
    #[arg(long, env = "STEVE_PLUGIN_LISTEN")]
    pub listen: String,
    /// A file holding the bearer token the Hub must present.
    #[arg(long, env = "STEVE_PLUGIN_TOKEN_FILE")]
    pub token_file: PathBuf,
    /// A JSON file with the string settings `gateway_url` and `gateway_token`.
    #[arg(long, env = "STEVE_PLUGIN_CONFIG")]
    pub config: PathBuf,
    /// A directory the plugin may write its state to.
    #[arg(long, env = "STEVE_PLUGIN_DATA")]
    pub data: PathBuf,
    /// Exit as soon as standard input reaches end of file (the Hub holds it open while it runs).
    #[arg(long, env = "STEVE_PLUGIN_SUPERVISED")]
    pub supervised: bool,
}

/// Why the plugin cannot start. No message carries the content of a secret.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("the listen address {0:?} is not an address like 127.0.0.1:7801")]
    ListenSyntax(String),
    #[error("the listen address {0} is not a loopback address; the plugin serves without TLS")]
    ListenNotLoopback(SocketAddr),
    #[error("cannot read {what} {path}: {source}")]
    Read { what: &'static str, path: PathBuf, source: std::io::Error },
    #[error("the token file {0} must hold 16 to 4096 printable ASCII characters with no space")]
    Token(PathBuf),
    #[error("the config file {0} is not a JSON object of string values")]
    ConfigShape(PathBuf),
    #[error("the config file {path} has no usable {key}")]
    ConfigKey { path: PathBuf, key: &'static str },
    #[error("gateway_url is not usable: {0}")]
    GatewayUrl(&'static str),
    #[error("cannot use the data directory {path}: {source}")]
    Data { path: PathBuf, source: std::io::Error },
}

/// Timeouts and intervals. The defaults are the protocol's; tests shorten them.
#[derive(Debug, Clone)]
pub struct Timing {
    /// The Hub must say hello within this (§3.2).
    pub hello_deadline: Duration,
    /// A Hub silent for this long is dropped (§9.1).
    pub idle: Duration,
    /// Unacked batches are sent again after this (§5.4).
    pub ack_timeout: Duration,
    /// How often the gateway's session and push state are read.
    pub health_tick: Duration,
    /// The longest wait for one gateway answer.
    pub call_timeout: Duration,
    /// Reconnect backoff towards the gateway, first and longest wait.
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    /// How long hello waits for the gateway when the account is not known yet.
    pub hello_wait: Duration,
    /// A gateway silent for this long is taken as gone (it pings every 20 s).
    pub gateway_idle: Duration,
    /// The wait between lookups of a group whose shared chat id is not known yet.
    pub oc_retry: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            hello_deadline: Duration::from_secs(10),
            idle: Duration::from_secs(60),
            ack_timeout: Duration::from_secs(30),
            health_tick: Duration::from_secs(15),
            call_timeout: Duration::from_secs(30),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            hello_wait: Duration::from_secs(5),
            gateway_idle: Duration::from_secs(70),
            oc_retry: Duration::from_secs(1),
        }
    }
}

/// Where the gateway is: its agent socket and the base of its media routes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    pub agent: Url,
    pub media: Url,
}

/// Everything the bridge runs on.
#[derive(Debug, Clone)]
pub struct Settings {
    /// The bearer token the Hub presents.
    pub token: String,
    pub gateway: Endpoints,
    /// The gateway credential.
    pub gateway_token: String,
    pub data: PathBuf,
    pub timing: Timing,
}

impl Settings {
    /// Reads the files the arguments name. The listen address is checked here too.
    pub fn load(args: &Args) -> Result<(SocketAddr, Settings), ConfigError> {
        let addr = listen_addr(&args.listen)?;
        let token = read_token(&args.token_file)?;
        let (gateway, gateway_token) = read_gateway(&args.config)?;
        prepare_data(&args.data)?;
        Ok((addr, Settings { token, gateway, gateway_token, data: args.data.clone(), timing: Timing::default() }))
    }
}

/// The address to bind: a literal loopback IP and a port.
pub fn listen_addr(s: &str) -> Result<SocketAddr, ConfigError> {
    let addr: SocketAddr = s.trim().parse().map_err(|_| ConfigError::ListenSyntax(s.to_string()))?;
    if !addr.ip().is_loopback() {
        return Err(ConfigError::ListenNotLoopback(addr));
    }
    Ok(addr)
}

/// A bearer token as the Hub accepts one: 16–4096 printable ASCII bytes with no whitespace.
pub fn token_ok(t: &str) -> bool {
    (16..=4096).contains(&t.len()) && t.bytes().all(|b| b.is_ascii_graphic())
}

pub fn read_token(path: &Path) -> Result<String, ConfigError> {
    let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        what: "the token file",
        path: path.to_path_buf(),
        source,
    })?;
    let token = raw.trim();
    if !token_ok(token) {
        return Err(ConfigError::Token(path.to_path_buf()));
    }
    Ok(token.to_string())
}

/// The config file: a flat JSON object of string values keyed by the setting's key.
pub fn read_gateway(path: &Path) -> Result<(Endpoints, String), ConfigError> {
    let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        what: "the config file",
        path: path.to_path_buf(),
        source,
    })?;
    let map: HashMap<String, Value> =
        serde_json::from_str(&raw).map_err(|_| ConfigError::ConfigShape(path.to_path_buf()))?;
    let text = |key: &'static str| -> Result<String, ConfigError> {
        match map.get(key).and_then(Value::as_str).map(str::trim) {
            Some(v) if !v.is_empty() => Ok(v.to_string()),
            _ => Err(ConfigError::ConfigKey { path: path.to_path_buf(), key }),
        }
    };
    let url = text("gateway_url")?;
    let token = text("gateway_token")?;
    if token.len() > 4096 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(ConfigError::ConfigKey { path: path.to_path_buf(), key: "gateway_token" });
    }
    Ok((endpoints(&url)?, token))
}

/// Turns the configured address into the gateway's agent socket and media base.
///
/// `ws://` and `http://` are accepted only towards this machine, because the credential would
/// cross the network in the clear otherwise; `wss://` and `https://` go anywhere.
pub fn endpoints(raw: &str) -> Result<Endpoints, ConfigError> {
    let url = Url::parse(raw).map_err(|_| ConfigError::GatewayUrl("it is not a URL"))?;
    let secure = match url.scheme() {
        "ws" | "http" => false,
        "wss" | "https" => true,
        _ => return Err(ConfigError::GatewayUrl("the scheme must be ws, wss, http or https")),
    };
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConfigError::GatewayUrl("it must not carry a user or password"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(ConfigError::GatewayUrl("it must not carry a query or fragment"));
    }
    let local = match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        None => return Err(ConfigError::GatewayUrl("it has no host")),
    };
    if !secure && !local {
        return Err(ConfigError::GatewayUrl("without TLS only a gateway on this machine is accepted"));
    }
    // a prefix in front of the gateway's own routes is kept (a reverse proxy)
    let path = url.path().trim_end_matches('/');
    let prefix = path.strip_suffix("/v0/agent").unwrap_or(path).to_string();
    let mut agent = url.clone();
    let mut media = url;
    let schemes = if secure { ("wss", "https") } else { ("ws", "http") };
    agent.set_scheme(schemes.0).map_err(|()| ConfigError::GatewayUrl("it is not a URL"))?;
    media.set_scheme(schemes.1).map_err(|()| ConfigError::GatewayUrl("it is not a URL"))?;
    agent.set_path(&format!("{prefix}/v0/agent"));
    media.set_path(&format!("{prefix}/v0/media/"));
    Ok(Endpoints { agent, media })
}

/// The data directory exists and can be written to.
pub fn prepare_data(dir: &Path) -> Result<(), ConfigError> {
    let fail = |source| ConfigError::Data { path: dir.to_path_buf(), source };
    std::fs::create_dir_all(dir).map_err(fail)?;
    let probe = dir.join(".write-check");
    std::fs::write(&probe, b"").map_err(fail)?;
    std::fs::remove_file(&probe).map_err(fail)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_loopback_address_is_served() {
        assert!(listen_addr("127.0.0.1:7801").is_ok());
        assert!(listen_addr("[::1]:7801").is_ok());
        assert!(matches!(listen_addr("0.0.0.0:7801"), Err(ConfigError::ListenNotLoopback(_))));
        assert!(matches!(listen_addr("192.0.2.7:7801"), Err(ConfigError::ListenNotLoopback(_))));
        assert!(matches!(listen_addr("localhost:7801"), Err(ConfigError::ListenSyntax(_))));
        assert!(matches!(listen_addr("7801"), Err(ConfigError::ListenSyntax(_))));
    }

    #[test]
    fn tokens_are_printable_and_long_enough() {
        assert!(token_ok("0123456789abcdef"));
        assert!(!token_ok("short"));
        assert!(!token_ok("0123456789 abcdef"));
        assert!(!token_ok("0123456789abcdé"));
    }

    #[test]
    fn the_gateway_address_becomes_both_routes() {
        let e = endpoints("ws://127.0.0.1:7788").unwrap();
        assert_eq!(e.agent.as_str(), "ws://127.0.0.1:7788/v0/agent");
        assert_eq!(e.media.as_str(), "http://127.0.0.1:7788/v0/media/");
        let e = endpoints("https://gateway.example.com/lark/v0/agent").unwrap();
        assert_eq!(e.agent.as_str(), "wss://gateway.example.com/lark/v0/agent");
        assert_eq!(e.media.as_str(), "https://gateway.example.com/lark/v0/media/");
        assert_eq!(endpoints("http://localhost:1/").unwrap().agent.as_str(), "ws://localhost:1/v0/agent");
    }

    #[test]
    fn a_plain_gateway_address_must_be_local() {
        assert!(endpoints("ws://gateway.example.com:7788").is_err());
        assert!(endpoints("ws://192.0.2.7:7788").is_err());
        assert!(endpoints("wss://user:pw@gateway.example.com").is_err());
        assert!(endpoints("wss://gateway.example.com/?token=x").is_err());
        assert!(endpoints("ftp://127.0.0.1").is_err());
    }

    #[test]
    fn the_config_is_a_flat_object_of_strings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, r#"{"gateway_url":"ws://127.0.0.1:9","gateway_token":"olk_example","other":"x"}"#).unwrap();
        let (e, t) = read_gateway(&path).unwrap();
        assert_eq!(e.agent.as_str(), "ws://127.0.0.1:9/v0/agent");
        assert_eq!(t, "olk_example");
        std::fs::write(&path, r#"{"gateway_url":"ws://127.0.0.1:9"}"#).unwrap();
        assert!(matches!(read_gateway(&path), Err(ConfigError::ConfigKey { key: "gateway_token", .. })));
        std::fs::write(&path, r#"{"gateway_url":7,"gateway_token":"x"}"#).unwrap();
        assert!(matches!(read_gateway(&path), Err(ConfigError::ConfigKey { key: "gateway_url", .. })));
        std::fs::write(&path, "[]").unwrap();
        assert!(matches!(read_gateway(&path), Err(ConfigError::ConfigShape(_))));
    }
}
