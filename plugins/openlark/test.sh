#!/usr/bin/env bash
# The plugin's checks, as CI runs them.
set -euo pipefail
cd "$(dirname "$0")"
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
