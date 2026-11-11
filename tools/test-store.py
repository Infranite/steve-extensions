#!/usr/bin/env python3
"""store.py against a library of its own in a temporary directory: a plugin that is a shell script
goes from manifest to archive to index, and what is wrong is refused."""

import hashlib
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent


def run(root: Path, *args: str, ok: bool = True) -> str:
    done = subprocess.run([sys.executable, str(root / "tools/store.py"), *args], capture_output=True, text=True)
    assert (done.returncode == 0) == ok, f"store.py {' '.join(args)}: {done.returncode}\n{done.stdout}{done.stderr}"
    return done.stdout + done.stderr


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "library"
        (root / "tools").mkdir(parents=True)
        shutil.copy(HERE / "store.py", root / "tools/store.py")
        (root / "steve-store.json").write_text(json.dumps({"store": 1, "name": "Test", "plugins": []}))
        plugin = root / "plugins/echo"
        plugin.mkdir(parents=True)
        manifest = {
            "id": "echo", "name": {"en": "Echo", "zh-CN": "回声"}, "summary": "Says it back", "kind": "channel",
            "version": "0.1.0", "contract": 1, "entry": "bin/echo",
            "config": [{"key": "address", "label": "Address"}, {"key": "token", "label": "Token", "secret": True}],
        }
        (plugin / "plugin.json").write_text(json.dumps(manifest))
        (plugin / "package.sh").write_text(
            'set -eu\nmkdir -p "$STAGE/bin"\nprintf \'#!/bin/sh\\necho %s %s\\n\' "$TARGET" "$VERSION" > "$STAGE/bin/echo"\n'
            'chmod +x "$STAGE/bin/echo"\necho notes > "$STAGE/README"\n'
        )

        assert "1 plugin(s), 0 released" in run(root, "check")
        assert json.loads(run(root, "pending")) == ["echo"]
        assert run(root, "version", "echo").strip() == "0.1.0"

        # the same files make the same archive, with only modes 755 and 644 in it
        dist = root / "dist"
        for target in ("aarch64-apple-darwin", "x86_64-unknown-linux-gnu"):
            run(root, "package", "echo", "--target", target, "--out", str(dist))
        one = dist / "echo-0.1.0-aarch64-apple-darwin.tar.gz"
        first = one.read_bytes()
        run(root, "package", "echo", "--target", "aarch64-apple-darwin", "--out", str(dist))
        assert one.read_bytes() == first
        with tarfile.open(one) as tar:
            modes = {m.name: (m.mode, m.isfile()) for m in tar.getmembers()}
        assert modes == {"README": (0o644, True), "bin": (0o755, False), "bin/echo": (0o755, True)}, modes

        # the index names the archives only when all of them are there
        base = "https://example.org/releases/echo-v0.1.0"
        run(root, "index", "echo", "--dist", str(dist), "--base", "http://example.org", ok=False)
        run(root, "index", "echo", "--dist", str(root / "nowhere"), "--base", base, ok=False)
        assert json.loads((root / "steve-store.json").read_text())["plugins"] == []
        run(root, "index", "echo", "--dist", str(dist), "--base", base)
        entry = json.loads((root / "steve-store.json").read_text())["plugins"][0]
        assert entry["version"] == "0.1.0" and entry["config"] == manifest["config"]
        mac = entry["artifacts"]["aarch64-apple-darwin"]
        assert mac == {"url": f"{base}/{one.name}", "sha256": hashlib.sha256(first).hexdigest()}, mac
        assert sorted(entry["artifacts"]) == ["aarch64-apple-darwin", "x86_64-unknown-linux-gnu"]
        assert "1 plugin(s), 1 released" in run(root, "check")
        assert json.loads(run(root, "pending")) == []

        # a new version is pending until it is released
        manifest["version"] = "0.2.0"
        (plugin / "plugin.json").write_text(json.dumps(manifest))
        assert json.loads(run(root, "pending")) == ["echo"]

        # a directory library for this machine, with relative addresses
        out = Path(tmp) / "local"
        run(root, "local", str(out), "--target", "x86_64-unknown-linux-gnu")
        local = json.loads((out / "steve-store.json").read_text())
        artifact = local["plugins"][0]["artifacts"]["x86_64-unknown-linux-gnu"]
        assert artifact["url"] == "dist/echo-0.2.0-x86_64-unknown-linux-gnu.tar.gz"
        assert hashlib.sha256((out / artifact["url"]).read_bytes()).hexdigest() == artifact["sha256"]

        # refused: an id that is not the directory, artifacts in a manifest, a package without its entry, a link
        for change, said in [
            ({"id": "other"}, "the directory is"),
            ({"artifacts": {}}, "not part of a manifest"),
            ({"version": "1.0"}, "semantic version"),
            ({"kind": "tool"}, "kind is one of"),
            ({"entry": "../echo"}, "entry is a path inside"),
        ]:
            (plugin / "plugin.json").write_text(json.dumps({**manifest, **change}))
            assert said in run(root, "check", ok=False), change
        (plugin / "plugin.json").write_text(json.dumps({**manifest, "entry": "bin/other"}))
        assert "left no executable" in run(root, "package", "echo", "--target", "t", "--out", str(dist), ok=False)
        (plugin / "plugin.json").write_text(json.dumps(manifest))
        with (plugin / "package.sh").open("a") as f:
            f.write('ln -s /bin/sh "$STAGE/bin/sh"\n')
        assert "regular files and directories only" in run(root, "package", "echo", "--target", "t", "--out", str(dist), ok=False)
    print("store.py: ok")


if __name__ == "__main__":
    main()
