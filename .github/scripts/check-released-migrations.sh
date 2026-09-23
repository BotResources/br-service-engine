#!/usr/bin/env bash
# A released engine migration is immutable. sqlx stores the SHA-384 of every
# applied file and fails `migrate` (VersionMismatch) when the embedded file no
# longer matches it, so an edit to a file a release shipped breaks every
# database that release migrated. v0.3.0 did exactly that to
# 9113000023_mirror_stream_identity.sql, which v0.2.0 had applied.
#
# This guard exports the engine migrations of every release tag (v*) and runs
# the service-engine test that holds each released file against HEAD: the file
# must still exist with the same bytes, or the SHA-384 of its released bytes
# must be registered for its version in EDITED_AFTER_RELEASE
# (crates/service-engine/src/schema/released.rs), which `migrate` then adopts.
# Every tag is checked, not only the last one, so a checksum registered for an
# older release can never be dropped silently.
set -euo pipefail

migrations="crates/service-engine/migrations"

tags=$(git tag --list 'v*' | sort -V)
if [ -z "$tags" ]; then
  echo "::error::no v* release tag is visible; check out with fetch-depth: 0 so the released migration sets can be compared" >&2
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
  mkdir -p "${released}/${tag}"
  git archive "$tag" "$migrations" | tar -x -C "${released}/${tag}" --strip-components=3
  count=$(find "${released}/${tag}" -name '*.sql' | wc -l | tr -d ' ')
  echo "${tag}: ${count} engine migrations exported"
  exported=$((exported + count))
done

if [ "$exported" -eq 0 ]; then
  echo "::error::no release tag ships an engine migration under ${migrations}; the guard would check nothing" >&2
  exit 1
fi

ENGINE_RELEASED_MIGRATIONS="$released" cargo test -p service-engine --lib --locked -- \
  --ignored --exact schema::released::tests::every_released_engine_migration_is_current_or_registered
