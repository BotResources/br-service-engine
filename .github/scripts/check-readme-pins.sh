#!/usr/bin/env bash

set -euo pipefail
shopt -s nullglob

repo_url="https://github.com/BotResources/br-service-engine"

workspace_version=$(grep -m1 '^version = ' Cargo.toml | sed -E 's/^version = "([^"]+)".*/\1/')
if [ -z "$workspace_version" ]; then
  echo "::error file=Cargo.toml::could not extract [workspace.package] version" >&2
  exit 1
fi
expected_tag="v${workspace_version}"

fail=0

for readme in README.md crates/*/README.md; do
  [ -f "$readme" ] || continue

  while IFS= read -r line; do
    [ -n "$line" ] || continue
    if ! grep -qE "tag = \"${expected_tag}\"" <<<"$line"; then
      echo "::error file=${readme}::self-pin does not carry tag = \"${expected_tag}\": ${line}" >&2
      fail=1
      continue
    fi
    if ! grep -qE "version = \"${workspace_version}\"" <<<"$line"; then
      echo "::error file=${readme}::self-pin does not carry version = \"${workspace_version}\" beside the tag (a tag-only git dep is a wildcard and cargo-deny denies it): ${line}" >&2
      fail=1
      continue
    fi
    echo "✓ ${readme}: self-pin on ${expected_tag} / ${workspace_version}"
  done < <(grep -F "${repo_url}\"" "$readme" | grep -F 'git = ' || true)
done

for toml in crates/*/Cargo.toml; do
  crate=$(basename "$(dirname "$toml")")
  readme="crates/${crate}/README.md"
  if [ ! -f "$readme" ]; then
    echo "::error file=${toml}::${crate} has no README.md" >&2
    fail=1
    continue
  fi
  if ! grep -qE "package = \"${crate}\", tag = \"${expected_tag}\", version = \"${workspace_version}\"" "$readme"; then
    echo "::error file=${readme}::${crate} README does not document its install pin as package = \"${crate}\", tag = \"${expected_tag}\", version = \"${workspace_version}\"" >&2
    fail=1
  fi
done

exit $fail
