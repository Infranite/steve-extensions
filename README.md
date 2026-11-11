# Steve extensions

The official plugin library of [Steve](https://github.com/Infranite/steve): plugins that are not part of a default Steve installation live here.

A plugin is a separate program. It never links into the Hub: it runs as its own process and talks to the Hub through one of Steve's documented contracts, so a plugin that crashes or is removed leaves the Hub as it was.

## Layout

```
plugins/<id>/      one plugin: its source, its README and what it needs to run
```

## Plugins

| Plugin | Kind | What it does |
|---|---|---|
| [`openlark`](plugins/openlark) | channel service | Feishu / Lark as you, through an openlark-gateway you run yourself. In development. |

## Status

Steve's plugin store (register a library like this one, see its plugins, install one) is in development. Until it ships, a channel plugin is started by hand and connected in Steve under **Connect · External channel**, as described in Steve's [deployment guide](https://github.com/Infranite/steve/blob/main/docs/deployment.md#an-external-channel-service).

## License

MIT, see [LICENSE](LICENSE).
