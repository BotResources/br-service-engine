#!/usr/bin/env bash
set -euo pipefail

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPTS/library-meta.sh"

repo_url="https://github.com/BotResources/br-service-engine"
readme="README.md"
version=$(bash "$SCRIPTS/library-version.sh")
expected_tag=$(release_tag "$version")

if [ ! -f "$readme" ]; then
  echo "::error::${readme} is missing" >&2
  exit 1
fi

fail=0
found=0

while IFS= read -r line; do
  [ -n "$line" ] || continue
  found=1
  if ! grep -qF "tag = \"${expected_tag}\"" <<<"$line"; then
    echo "::error file=${readme}::self-pin does not carry tag = \"${expected_tag}\": ${line}" >&2
    fail=1
    continue
  fi
  if ! grep -qF "version = \"${version}\"" <<<"$line"; then
    echo "::error file=${readme}::self-pin does not carry version = \"${version}\" beside the tag (a tag-only git dep is a wildcard and cargo-deny denies it): ${line}" >&2
    fail=1
    continue
  fi
  echo "✓ ${readme}: self-pin on ${expected_tag} / ${version}"
done < <(grep -F "${repo_url}\"" "$readme" | grep -F 'git = ' || true)

if [ "$found" -eq 0 ]; then
  echo "::error file=${readme}::no self-pin line found (expected a git = \"${repo_url}\" dependency snippet)" >&2
  fail=1
fi

exit $fail
