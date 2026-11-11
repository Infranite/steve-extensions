# Steve extensions

The official plugin library of [Steve](https://github.com/Infranite/steve): plugins that are not part of a default Steve installation live here.

A plugin is a separate program. It never links into the Hub: it runs as its own process and talks to the Hub through one of Steve's documented contracts, so a plugin that crashes or is removed leaves the Hub as it was.

## Installing a plugin

In Steve, open **Plugins**. This library is listed on a new installation; the instance's owner reads it, installs a plugin, and anyone signed in can then connect it with their own settings. To add the library by hand, enter `Infranite/steve-extensions`.

Installing a plugin runs its program as the Hub's user. What is installed is the archive whose sha256 [`steve-store.json`](steve-store.json) names, built by this repository's release workflow from the source under `plugins/`.

## Plugins

| Plugin | Kind | What it does |
|---|---|---|
| [`openlark`](plugins/openlark) | channel | Feishu / Lark as you, through an openlark-gateway you run yourself. |

## Layout

```
steve-store.json           the index Steve reads: every released plugin and its archives
plugins/<id>/              one plugin
  plugin.json              its entry in the index, without the archives
  package.sh               builds it for $TARGET and puts its archive's files under $STAGE
  test.sh                  optional: its tests, run by CI
tools/store.py             checks manifests, packs archives, writes the index
```

The index format and what a plugin of each kind has to implement are specified in Steve's [deployment guide](https://github.com/Infranite/steve/blob/main/docs/deployment.md#plugins).

## Adding or changing a plugin

1. Put the plugin in `plugins/<id>/` with a `plugin.json`:

   ```json
   {
     "id": "example",
     "name": "Example",
     "summary": {"en": "What it does", "zh-CN": "它做什么"},
     "kind": "channel",
     "version": "0.1.0",
     "contract": 1,
     "entry": "bin/example",
     "homepage": "https://github.com/Infranite/steve-extensions/tree/main/plugins/example",
     "config": [
       {"key": "address", "label": "Service address", "hint": "wss://…"},
       {"key": "token", "label": "Access token", "secret": true}
     ]
   }
   ```

2. Add a `package.sh` that builds for `$TARGET` (a Rust target triple) and leaves the archive's files, the `entry` among them, under `$STAGE`. `$VERSION` is the manifest's version. Only regular files and directories go into an archive.
3. Check it and try it in a Steve of your own:

   ```sh
   python3 tools/store.py check
   python3 tools/store.py local /absolute/path/to/a/directory
   ```

   `local` builds every plugin for this machine and writes a directory library there; add that directory on Steve's Plugins page and install from it.

## Releasing

A plugin is released by changing `version` in its `plugin.json` on `main`. The release workflow builds the archive for each target Steve is released for (`aarch64-apple-darwin`, `x86_64-unknown-linux-gnu`), publishes them as the GitHub release `<id>-v<version>`, and then commits the new entry to `steve-store.json`. Hubs offer the update the next time their owner reads the library.

`steve-store.json` is written by that workflow; do not edit a plugin's entry by hand.

## License

MIT, see [LICENSE](LICENSE).
