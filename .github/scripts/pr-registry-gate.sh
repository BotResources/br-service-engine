#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
SCRIPTS="$PWD/.github/scripts"
LAW="$SCRIPTS/release-line.sh"
source "$SCRIPTS/library-meta.sh"

BASE_REF="${BASE_REF:-main}"

git fetch -q origin "$BASE_REF"

line="$("$LAW" check-ref "refs/heads/${BASE_REF}")"

version="$(bash "$SCRIPTS/library-version.sh")"
base_version="$(bash "$SCRIPTS/library-version.sh" "origin/${BASE_REF}" 2>/dev/null || true)"

if [[ "$version" == "$base_version" ]]; then
    echo "No version bump in this PR — nothing to gate."
    exit 0
fi
if non_release "$version"; then
    echo "${LIBRARY_NAME}: ${version} is non-release — not gated."
    exit 0
fi

echo "${LIBRARY_NAME}: ${base_version:-<absent>} -> ${version} — gating against ${line} and the registry."
"$LAW" check "$LIBRARY_NAME" "$version" "refs/heads/${line}"
bash "$SCRIPTS/registry-gate.sh" "$version"
