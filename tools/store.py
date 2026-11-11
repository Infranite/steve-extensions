#!/usr/bin/env python3
"""The library's index, `steve-store.json`, and the archives it names.

A plugin is a directory `plugins/<id>/` with

- `plugin.json`: its entry in the index without `artifacts` (id, name, summary, kind, version,
  contract, entry, homepage, config), and
- `package.sh`: builds it for `$TARGET` and puts the files of its archive under `$STAGE`.

    store.py check                      manifests and the index are well formed and agree
    store.py pending                    ids whose manifest version is not the released one (JSON)
    store.py version <id>               the manifest's version
    store.py package <id> --target T --out DIR
                                        DIR/<id>-<version>-<T>.tar.gz
    store.py index <id> --dist DIR --base URL
                                        point the index's entry at the archives in DIR, served at URL
    store.py local DIR                  a directory library of every plugin for this machine:
                                        add DIR in Steve's Plugins page to try what is in the tree

Needs Python 3.9 and nothing else; `package.sh` needs what the plugin builds with.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INDEX = ROOT / "steve-store.json"
PLUGINS = ROOT / "plugins"

# The targets Steve itself is released for; a release of a plugin has an archive for each.
TARGETS = ["aarch64-apple-darwin", "x86_64-unknown-linux-gnu"]

ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,39}\Z")
SEMVER = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?\Z")
KINDS = {"channel": {1}}
MANIFEST_KEYS = ["id", "name", "summary", "kind", "version", "contract", "entry", "homepage", "config"]
MAX_PLUGINS, MAX_FIELDS = 200, 20


class Wrong(Exception):
    pass


def text_ok(v, limit: int) -> bool:
    def one(s) -> bool:
        return isinstance(s, str) and bool(s.strip()) and len(s.encode()) <= limit

    if isinstance(v, dict):
        return 0 < len(v) <= 20 and all(isinstance(k, str) and one(s) for k, s in v.items())
    return one(v)


def path_ok(s) -> bool:
    return isinstance(s, str) and 0 < len(s) <= 200 and not s.startswith("/") and all(
        part not in ("", ".", "..") for part in s.split("/")
    )


def check_manifest(m: dict, where: str) -> None:
    def need(ok: bool, what: str) -> None:
        if not ok:
            raise Wrong(f"{where}: {what}")

    need(isinstance(m, dict), "not an object")
    need(isinstance(m.get("id"), str) and bool(ID.match(m["id"])), "id is a lowercase letter or digit, then a-z 0-9 - _ (40 at most)")
    need(text_ok(m.get("name"), 100), "name is a string or a map from language to string")
    need(text_ok(m.get("summary"), 500), "summary is a string or a map from language to string")
    need(isinstance(m.get("version"), str) and bool(SEMVER.match(m["version"])), "version is a semantic version")
    kind, contract = m.get("kind"), m.get("contract")
    need(kind in KINDS, f"kind is one of {sorted(KINDS)}")
    need(isinstance(contract, int) and contract in KINDS[kind], f"contract of a {kind} plugin is one of {sorted(KINDS[kind])}")
    need(path_ok(m.get("entry")), "entry is a path inside the archive")
    home = m.get("homepage")
    need(home is None or (isinstance(home, str) and home.startswith("https://")), "homepage is https://")
    config = m.get("config", [])
    need(isinstance(config, list) and len(config) <= MAX_FIELDS, f"config is a list of at most {MAX_FIELDS} settings")
    seen = set()
    for f in config:
        need(isinstance(f, dict) and isinstance(f.get("key"), str) and bool(ID.match(f["key"])), "a setting's key is like an id")
        need(f["key"] not in seen, f"setting {f['key']} twice")
        seen.add(f["key"])
        need(text_ok(f.get("label"), 100), f"setting {f['key']}: label")
        need("hint" not in f or text_ok(f["hint"], 300), f"setting {f['key']}: hint")
        need(all(isinstance(f.get(k, False), bool) for k in ("secret", "required")), f"setting {f['key']}: secret and required are true or false")


def manifest(plugin: str) -> dict:
    path = PLUGINS / plugin / "plugin.json"
    if not path.is_file():
        raise Wrong(f"no plugins/{plugin}/plugin.json")
    try:
        m = json.loads(path.read_text())
    except ValueError as e:
        raise Wrong(f"plugins/{plugin}/plugin.json: {e}") from None
    check_manifest(m, f"plugins/{plugin}/plugin.json")
    if m["id"] != plugin:
        raise Wrong(f"plugins/{plugin}/plugin.json: id is {m['id']!r}, the directory is {plugin!r}")
    extra = sorted(set(m) - set(MANIFEST_KEYS))
    if extra:
        raise Wrong(f"plugins/{plugin}/plugin.json: {', '.join(extra)} is not part of a manifest (artifacts are written by a release)")
    cargo = PLUGINS / plugin / "Cargo.toml"
    if cargo.is_file():
        found = re.search(r'(?m)^version\s*=\s*"([^"]+)"', cargo.read_text())
        if found and found.group(1) != m["version"]:
            raise Wrong(f"plugins/{plugin}: plugin.json says {m['version']}, Cargo.toml says {found.group(1)}")
    return m


def plugin_ids() -> list[str]:
    if not PLUGINS.is_dir():
        return []
    return sorted(p.name for p in PLUGINS.iterdir() if p.is_dir() and not p.name.startswith("."))


def read_index() -> dict:
    try:
        index = json.loads(INDEX.read_text())
    except (OSError, ValueError) as e:
        raise Wrong(f"steve-store.json: {e}") from None
    if index.get("store") != 1 or not text_ok(index.get("name"), 100) or not isinstance(index.get("plugins"), list):
        raise Wrong('steve-store.json: needs "store": 1, a name and a list of plugins')
    return index


def write_index(index: dict, to: Path = INDEX) -> None:
    to.write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n")


def released(index: dict, plugin: str) -> dict | None:
    return next((p for p in index["plugins"] if p.get("id") == plugin), None)


def check() -> None:
    index = read_index()
    ids = plugin_ids()
    if len(index["plugins"]) > MAX_PLUGINS:
        raise Wrong(f"steve-store.json lists more than {MAX_PLUGINS} plugins")
    for plugin in ids:
        manifest(plugin)
        if not (PLUGINS / plugin / "package.sh").is_file():
            raise Wrong(f"no plugins/{plugin}/package.sh")
    seen = set()
    for entry in index["plugins"]:
        where = f"steve-store.json: {entry.get('id')!r}"
        check_manifest(entry, where)
        if entry["id"] in seen:
            raise Wrong(f"{where} is listed twice")
        seen.add(entry["id"])
        if entry["id"] not in ids:
            raise Wrong(f"{where} has no directory under plugins/")
        artifacts = entry.get("artifacts")
        if not isinstance(artifacts, dict) or not artifacts:
            raise Wrong(f"{where} has no artifacts")
        for target, a in artifacts.items():
            ok = isinstance(a, dict) and isinstance(a.get("url"), str) and a["url"].startswith("https://")
            if not ok or not re.fullmatch(r"[0-9a-f]{64}", str(a.get("sha256"))):
                raise Wrong(f"{where}: artifact for {target} needs an https:// url and a sha256")
    print(f"{len(ids)} plugin(s), {len(index['plugins'])} released")


def pending() -> list[str]:
    index = read_index()
    out = []
    for plugin in plugin_ids():
        entry = released(index, plugin)
        if entry is None or entry["version"] != manifest(plugin)["version"]:
            out.append(plugin)
    return out


def host_target() -> str:
    try:
        out = subprocess.run(["rustc", "-vV"], capture_output=True, text=True, check=True).stdout
        for line in out.splitlines():
            if line.startswith("host: "):
                return line[6:].strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    machine = {"arm64": "aarch64", "amd64": "x86_64"}.get(os.uname().machine.lower(), os.uname().machine.lower())
    system = {"darwin": "apple-darwin", "linux": "unknown-linux-gnu"}.get(sys.platform)
    if system is None:
        raise Wrong(f"no target known for {sys.platform}; pass --target")
    return f"{machine}-{system}"


def archive_name(m: dict, target: str) -> str:
    return f"{m['id']}-{m['version']}-{target}.tar.gz"


def pack(stage: Path, to: Path) -> str:
    """A `.tar.gz` of the regular files and directories under `stage`, the same bytes for the same
    files; returns its sha256."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.GNU_FORMAT) as tar:
        for path in sorted(stage.rglob("*")):
            rel = path.relative_to(stage).as_posix()
            if path.is_symlink() or not (path.is_file() or path.is_dir()):
                raise Wrong(f"{rel}: an archive holds regular files and directories only")
            info = tarfile.TarInfo(rel)
            info.mtime, info.uid, info.gid, info.uname, info.gname = 0, 0, 0, "", ""
            if path.is_dir():
                info.type, info.mode = tarfile.DIRTYPE, 0o755
                tar.addfile(info)
            else:
                info.size = path.stat().st_size
                info.mode = 0o755 if os.access(path, os.X_OK) else 0o644
                with path.open("rb") as f:
                    tar.addfile(info, f)
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb", mtime=0, filename="") as gz:
        gz.write(raw.getvalue())
    data = out.getvalue()
    to.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def package(plugin: str, target: str, out: Path) -> tuple[str, str]:
    """Builds and packs one plugin; returns the archive's file name and sha256."""
    m = manifest(plugin)
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"stage-{plugin}-") as tmp:
        stage = Path(tmp)
        env = dict(os.environ, TARGET=target, STAGE=str(stage), VERSION=m["version"])
        subprocess.run(["bash", "package.sh"], cwd=PLUGINS / plugin, env=env, check=True)
        entry = stage / m["entry"]
        if not entry.is_file() or not os.access(entry, os.X_OK):
            raise Wrong(f"plugins/{plugin}/package.sh left no executable {m['entry']} in $STAGE")
        name = archive_name(m, target)
        return name, pack(stage, out / name)


def entry_for(m: dict, artifacts: dict) -> dict:
    entry = {k: m[k] for k in MANIFEST_KEYS if k in m}
    entry["artifacts"] = dict(sorted(artifacts.items()))
    return entry


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def point_index(plugin: str, dist: Path, base: str, targets: list[str]) -> None:
    m = manifest(plugin)
    if not base.startswith("https://"):
        raise Wrong("--base is the https:// address the archives are served under")
    artifacts = {}
    for target in targets:
        archive = dist / archive_name(m, target)
        if not archive.is_file():
            raise Wrong(f"no {archive.name} in {dist}")
        artifacts[target] = {"url": f"{base.rstrip('/')}/{archive.name}", "sha256": sha256_of(archive)}
    index = read_index()
    entry = entry_for(m, artifacts)
    index["plugins"] = sorted([p for p in index["plugins"] if p.get("id") != plugin] + [entry], key=lambda p: p["id"])
    write_index(index)
    print(f"steve-store.json: {plugin} {m['version']} for {', '.join(targets)}")


def local(directory: Path, target: str, only: list[str]) -> None:
    directory = directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    dist = directory / "dist"
    if dist.exists():
        shutil.rmtree(dist)
    entries = []
    for plugin in only or plugin_ids():
        name, sha = package(plugin, target, dist)
        entries.append(entry_for(manifest(plugin), {target: {"url": f"dist/{name}", "sha256": sha}}))
    name = read_index()["name"]
    write_index({"store": 1, "name": name, "plugins": entries}, directory / "steve-store.json")
    print(f"{directory}: {len(entries)} plugin(s) for {target}. Add this directory as a library in Steve's Plugins page.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check")
    sub.add_parser("pending")
    sub.add_parser("version").add_argument("plugin")
    p = sub.add_parser("package")
    p.add_argument("plugin")
    p.add_argument("--target")
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("index")
    p.add_argument("plugin")
    p.add_argument("--dist", type=Path, required=True)
    p.add_argument("--base", required=True)
    p.add_argument("--target", action="append", help="instead of every released target")
    p = sub.add_parser("local")
    p.add_argument("directory", type=Path)
    p.add_argument("--target")
    p.add_argument("--plugin", action="append", default=[])
    args = ap.parse_args()
    try:
        if args.cmd == "check":
            check()
        elif args.cmd == "pending":
            print(json.dumps(pending()))
        elif args.cmd == "version":
            print(manifest(args.plugin)["version"])
        elif args.cmd == "package":
            name, sha = package(args.plugin, args.target or host_target(), args.out)
            print(f"{args.out / name}  sha256 {sha}")
        elif args.cmd == "index":
            point_index(args.plugin, args.dist, args.base, args.target or TARGETS)
        elif args.cmd == "local":
            local(args.directory, args.target or host_target(), args.plugin)
    except Wrong as e:
        print(f"store.py: {e}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as e:
        print(f"store.py: {' '.join(map(str, e.cmd))} failed ({e.returncode})", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
