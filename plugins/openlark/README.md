# openlark: Feishu / Lark as you

`steve-plugin-openlark` lets a [Steve](https://github.com/Infranite/steve) Hub read and write Feishu / Lark as the person who is signed in, not as a bot. It is a bridge between two protocols:

- towards the Hub it is a channel service: a server of Steve's external channel protocol v1 (WebSocket JSON-RPC, and attachments over HTTP on the same address);
- towards Lark it is a client of an openlark-gateway (openlark-protocol v0), the program that holds the signed-in session.

One process bridges one gateway credential, which is one signed-in user. The plugin keeps no Lark session and no message store of its own: it translates.

## What it needs

- An openlark-gateway that you run and have signed in to, reachable from the machine the plugin runs on. The plugin builds and is tested without one; it only needs one to do anything useful.
- A credential of that gateway (`openlark-gateway cred create`). What the credential may do decides what Steve can do, see [What Steve gets](#what-steve-gets).
- A Rust toolchain (1.99 or later) to build it.

## Build

```
cd plugins/openlark
cargo build --release
```

The binary is `target/release/steve-plugin-openlark`. The plugin is its own Cargo workspace with exactly pinned dependencies; it depends on nothing outside this directory.

## Run

Every setting is an environment variable with an equivalent flag.

| Variable | Flag | What it is |
|---|---|---|
| `STEVE_PLUGIN_LISTEN` | `--listen` | The address to serve the Hub on, `127.0.0.1:<port>` (or `[::1]:<port>`). Port `0` picks a free one. |
| `STEVE_PLUGIN_TOKEN_FILE` | `--token-file` | A file holding the bearer token the Hub must present: 16 to 4096 printable ASCII characters. |
| `STEVE_PLUGIN_CONFIG` | `--config` | A JSON file with the plugin's settings, below. |
| `STEVE_PLUGIN_DATA` | `--data` | A directory the plugin may write its state to. It is created when missing. |
| `STEVE_PLUGIN_SUPERVISED` | `--supervised` | Set to `1` by a Hub that starts the plugin itself, see [Under the Hub](#under-the-hub). |

The config file is a flat JSON object of string values:

```json
{"gateway_url": "ws://127.0.0.1:<gateway port>", "gateway_token": "<the gateway credential>"}
```

`gateway_url` is the gateway's address; a path in front of the gateway's own routes (a reverse proxy) is kept. `ws://` and `http://` are accepted only for a gateway on the same machine, because the credential would cross the network in the clear otherwise; use `wss://` or `https://` for a gateway elsewhere.

By hand:

```
umask 077
mkdir -p ~/.local/state/steve-plugin-openlark
head -c 32 /dev/urandom | base64 | tr -d '=+/' > ~/.local/state/steve-plugin-openlark/token
$EDITOR ~/.local/state/steve-plugin-openlark/config.json

steve-plugin-openlark \
  --listen 127.0.0.1:7801 \
  --token-file ~/.local/state/steve-plugin-openlark/token \
  --config ~/.local/state/steve-plugin-openlark/config.json \
  --data ~/.local/state/steve-plugin-openlark/data
```

The paths and the port are examples; any will do. Both files hold a secret, so keep them readable by you only.

What to expect of the process:

- **It serves the Hub at `ws://127.0.0.1:<port>/ws`**, and attachments at `http://127.0.0.1:<port>/v1/media/<key>`. On start it logs `serving the Hub on ws://127.0.0.1:<port>/ws`, which is how a caller that asked for port `0` learns the port.
- **Loopback only.** The plugin serves without TLS, so it refuses to listen on anything but a loopback address. To reach it from a Hub on another machine, put it behind something that adds TLS; the plugin does not do that itself.
- **Logs go to standard error** and never contain either token. `RUST_LOG` may name a level (`error`, `warn`, `info`, `debug`, `trace`) for the plugin's own lines; the libraries under it stay at `info`.
- **It stops on SIGTERM or SIGINT** with exit code 0, after closing the Hub's connections.
- **It exits with a non-zero code** when it cannot start: an address it may not or cannot listen on, a token or config file it cannot read or use, a data directory it cannot write to. The reason is the last line on standard error.
- **A gateway that is away, or that refuses the credential, does not end it.** The plugin keeps trying, and tells the Hub through the protocol: the upstream is `down`, or the session is `expired` with the detail `gateway_credential_refused`. When the gateway returns or the credential is accepted again, it recovers on its own.

### Under the Hub

A Hub that starts the plugin itself sets `STEVE_PLUGIN_SUPERVISED=1` (or passes `--supervised`) and holds the plugin's standard input open. The plugin then exits, cleanly and with code 0, as soon as standard input reaches end of file: that is how it learns the Hub is gone, so it never outlives the Hub while holding the port.

Without that setting standard input is ignored. A plugin started by hand, under `nohup` or under a service manager sees end of file at once and keeps running. An empty value, `0` or `false` leave the setting off.

### State

The data directory holds three small files, none with a secret:

- `state.json`: who is signed in and what the credential offers, so that the plugin can answer the Hub's hello after a restart while the gateway is away;
- `chats.json`: the id each chat was first announced under, so that a chat keeps one id for good;
- `idempotency.json`: the results of the writes of the last 48 hours, so that a write the Hub retries is not sent twice, also across a restart.

Deleting the directory is safe while the plugin is stopped. The Hub then reads recent history again, and a group whose shared id the gateway did not know earlier may appear as a new chat.

## Connect it in Steve

Steve's plugin store starts the plugin and connects it for you. By hand, start the plugin as above, then in Steve open **Connect · External channel** and enter:

- the address `ws://127.0.0.1:<port>/ws`;
- the token from the token file.

The Hub says hello, asks the plugin who is signed in, and the connection becomes yours. Steve's [deployment guide](https://github.com/Infranite/steve/blob/main/docs/deployment.md#an-external-channel-service) describes the Hub's side.

## What Steve gets

The plugin announces only what the gateway and the credential can really serve.

| Credential scope | Steve can |
|---|---|
| `chats.read` | look up a chat's kind and name |
| `messages.read` | receive new messages, edits and recalls as they happen; read a message by id; read a chat's history over a stretch of time; download attachments |
| `messages.send` | send and reply as you, with mentions |
| `messages.recall` | recall a message you sent |
| `reactions.write` | add and remove the three reactions Steve uses for its own state: 👀, ✅ and ❌ |
| `people.read` | look up a person's name, mail address and avatar |

A credential with none of the three write scopes makes the connection read-only. With any of them, the gateway's own write policy still decides chat by chat; a chat it refuses is reported to Steve as forbidden for that write.

## What is not supported

These are limits of gateway v0 or of this bridge, not settings.

- **No list of chats and no member lists.** A chat becomes known to Steve when a message arrives in it.
- **No platform message ids.** Messages are identified by the gateway's own ids. A reply names its parent by that id, which Steve can follow for messages that came through this plugin.
- **Direct chats, and a group in its first minute, are known by the gateway's chat id**, not by the shared `oc_…` id. A chat keeps the id it was first announced under.
- **History is read by position, not by time.** The plugin walks back from a chat's newest message to the start of the range asked for, at most 40 pages; a range that lies deeper is answered as incomplete rather than passed off as whole. Replies inside a thread are not part of a chat's history at the gateway.
- **People are looked up by the gateway's user id only**, not by mail address; title and organisation are not known. Only the signed-in user carries a `union_id`.
- **Text only when writing.** No editing, no attachments to upload, no cards; other emoji than the three above are refused.
- **No read state.**
- **What the gateway misses, the plugin cannot know.** When the gateway says it discarded changes, the plugin tells Steve which stretch of time to read again. Messages the gateway itself never saw are not reported by anyone.

## Tests

```
cargo test
cargo clippy --all-targets
```

The tests need no gateway and no network: they start the bridge against a fake gateway inside the test process (`tests/common/mod.rs`) and drive it the way the Hub does, through the conformance checklist and test vectors of the external channel protocol. `tests/process.rs` starts the real binary for the run contract above.
