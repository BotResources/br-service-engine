#!/usr/bin/env bash
set -euo pipefail

rev="${1:-}"
manifest="Cargo.toml"

if [ -n "$rev" ]; then
  if ! content=$(git show "${rev}:${manifest}" 2>/dev/null); then
    echo "::error::${manifest} is missing at ${rev}" >&2
    exit 1
  fi
else
  if [ ! -f "$manifest" ]; then
    echo "::error::${manifest} is missing" >&2
    exit 1
  fi
  content=$(cat "$manifest")
fi

version=$(printf '%s\n' "$content" | awk -F'"' '
  /^\[workspace\.package\]/ { in_pkg = 1; next }
  /^\[/ { in_pkg = 0 }
  in_pkg && /^version *=/ { print $2; exit }')
if [ -z "$version" ]; then
  echo "::error file=${manifest}::could not extract the [workspace.package] version" >&2
  exit 1
fi

echo "$version"
