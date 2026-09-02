#!/usr/bin/env bash
set -euo pipefail

version=$(grep -m1 '^version = ' Cargo.toml | sed -E 's/^version = "([^"]+)".*/\1/')
if [ -z "$version" ]; then
  echo "::error file=Cargo.toml::could not extract the workspace version" >&2
  exit 1
fi

changelog="CHANGELOG.md"
if [ ! -f "$changelog" ]; then
  echo "::error::workspace version ${version} but ${changelog} is missing" >&2
  exit 1
fi

if ! grep -qE "^## ${version}( |\$)" "$changelog"; then
  echo "::error file=${changelog}::v${version} has no '## ${version}' entry. Add a plain '## ${version}' section before merging." >&2
  exit 1
fi

echo "✓ v${version}"
