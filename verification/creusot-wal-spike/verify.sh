#!/bin/sh
set -eu

cd "$(dirname "$0")"

if ! command -v cargo-creusot >/dev/null 2>&1; then
    echo "cargo-creusot is not installed; follow https://guide.creusot.rs/installation.html" >&2
    exit 1
fi

cargo creusot
