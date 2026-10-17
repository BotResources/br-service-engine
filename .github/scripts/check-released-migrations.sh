#!/usr/bin/env bash
set -euo pipefail

migrations="crates/service-engine/migrations"

tags=$(bash "$(dirname "$0")/release-tags.sh")
if [ -z "$tags" ]; then
  echo "::error::no release tag is visible; check out with fetch-depth: 0 so the released migration sets can be compared" >&2
  exit 1
fi

released="$(mktemp -d)"
trap 'rm -rf "$released"' EXIT

exported=0
for tag in $tags; do
  if ! git cat-file -e "${tag}:${migrations}" 2>/dev/null; then
    echo "${tag}: ships no engine migration"
    continue
  fi
  dir="${released}/${tag//\//_}"
  mkdir -p "$dir"
  git archive "$tag" "$migrations" | tar -x -C "$dir" --strip-components=3
  count=$(find "$dir" -name '*.sql' | wc -l | tr -d ' ')
  echo "${tag}: ${count} engine migrations exported"
  exported=$((exported + count))
done

if [ "$exported" -eq 0 ]; then
  echo "::error::no release tag ships an engine migration under ${migrations}; the guard would check nothing" >&2
  exit 1
fi

ENGINE_RELEASED_MIGRATIONS="$released" cargo test -p service-engine --lib --locked -- \
  --ignored --exact schema::released::tests::every_released_engine_migration_is_current_or_registered
