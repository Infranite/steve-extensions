#!/usr/bin/env bash
# Builds the plugin for $TARGET and puts its archive's files under $STAGE (see the library's README).
set -euo pipefail
: "${TARGET:?the Rust target triple to build for}" "${STAGE:?the directory the archive is made from}"
cd "$(dirname "$0")"
cargo build --release --locked --target "$TARGET"
mkdir -p "$STAGE/bin"
install -m 0755 "target/$TARGET/release/steve-plugin-openlark" "$STAGE/bin/steve-plugin-openlark"
install -m 0644 README.md "$STAGE/README.md"
install -m 0644 ../../LICENSE "$STAGE/LICENSE"
