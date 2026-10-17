#!/usr/bin/env bash
set -euo pipefail

rev="${1:-}"
version=$(bash "$(dirname "$0")/library-version.sh" "$rev")
changelog="CHANGELOG.md"

if [ -n "$rev" ]; then
  content=$(git show "${rev}:${changelog}" 2>/dev/null) || content=""
else
  content=$(cat "$changelog" 2>/dev/null) || content=""
fi

if [ -z "$content" ]; then
  echo "::error::version ${version} but ${changelog} is missing" >&2
  exit 1
fi

if ! grep -qE "^## ${version}( |\$)" <<<"$content"; then
  echo "::error file=${changelog}::${version} has no '## ${version}' entry. Add a plain '## ${version}' section before merging." >&2
  exit 1
fi

echo "✓ ${version}"
