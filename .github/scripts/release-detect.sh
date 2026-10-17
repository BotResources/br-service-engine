#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
SCRIPTS="$PWD/.github/scripts"
LAW="$SCRIPTS/release-line.sh"
source "$SCRIPTS/library-meta.sh"

: "${GITHUB_REF:?GITHUB_REF is required}"
EVENT_NAME="${EVENT_NAME:-push}"
HEAD_SHA="${GITHUB_SHA:-$(git rev-parse HEAD)}"

library_version() {
    bash "$SCRIPTS/library-version.sh" "$@"
}

tag_exists() {
    git rev-parse -q --verify "refs/tags/$1" >/dev/null \
        || git ls-remote --exit-code --tags origin "$1" >/dev/null 2>&1
}

require_own_line() {
    local branch="$1" line_library
    if [[ "$branch" == release/* ]]; then
        read -r line_library _ _ <<<"$("$LAW" parse "$branch")"
        if [[ "$line_library" != "$LIBRARY_NAME" ]]; then
            echo "::error::${branch} is the release line of ${line_library}, not of ${LIBRARY_NAME}; nothing publishes from it." >&2
            return 1
        fi
    fi
}

explicit_entry() {
    local version="$1" branch tag commit declared
    branch="$("$LAW" check-ref "$GITHUB_REF")" || return 1
    require_own_line "$branch" || return 1
    "$LAW" check "$LIBRARY_NAME" "$version" "refs/heads/${branch}" >&2 || return $?
    tag="$(release_tag "$version")"
    if tag_exists "$tag"; then
        git fetch -q origin "refs/tags/${tag}:refs/tags/${tag}" 2>/dev/null || true
        commit="$(git rev-list -n 1 "refs/tags/${tag}")" || return 1
        if ! git merge-base --is-ancestor "$commit" "$HEAD_SHA"; then
            echo "::error::${tag} points at ${commit}, which is not on ${branch}; run this workflow from the line that carries it." >&2
            return 1
        fi
    else
        commit="$HEAD_SHA"
    fi
    declared="$(library_version "$commit")" || return 1
    if [[ "$declared" != "$version" ]]; then
        echo "::error::${LIBRARY_NAME} at ${commit} declares ${declared}, not ${version}: releasing it would publish that code as ${version}." >&2
        return 1
    fi
    printf '%s %s %s\n' "$version" "$branch" "$commit"
}

ENTRIES=()
REFUSED=()

if [[ "$EVENT_NAME" == "workflow_dispatch" ]]; then
    ENTRIES+=("$(explicit_entry "${INPUT_VERSION:-}")")
    echo "workflow_dispatch: ${ENTRIES[0]}"
else
    BRANCH="$("$LAW" check-ref "$GITHUB_REF")"
    require_own_line "$BRANCH"
    version="$(library_version)"
    parent_version="$(library_version "${HEAD_SHA}^" 2>/dev/null || true)"
    tag="$(release_tag "$version")"
    if non_release "$version"; then
        echo "${LIBRARY_NAME}: version ${version} is non-release — skipping."
    elif tag_exists "$tag" && [[ "$version" == "$parent_version" ]]; then
        echo "${LIBRARY_NAME}: ${tag} already exists and the version did not change — already shipped."
    else
        rc=0
        verdict="$("$LAW" check "$LIBRARY_NAME" "$version" "refs/heads/${BRANCH}" 2>&1)" || rc=$?
        if [[ "$rc" -eq 3 ]]; then
            printf '%s\n' "$verdict" >&2
            REFUSED+=("${LIBRARY_NAME} ${version}: $(sed -n 's/^::error::release-line: //p' <<<"$verdict" | head -n 1)")
            echo "${LIBRARY_NAME} ${version}: refused by the release-line law — not released."
        elif [[ "$rc" -ne 0 ]]; then
            printf '%s\n' "$verdict" >&2
            exit "$rc"
        else
            echo "$verdict"
            ENTRIES+=("${version} ${BRANCH} ${HEAD_SHA}")
            echo "Will release: ${LIBRARY_NAME} ${version} (line ${BRANCH})"
        fi
    fi
fi

MATRIX="$(python3 -c '
import json, sys
keys = ("version", "line", "commit")
print(json.dumps([dict(zip(keys, line.split(" "))) for line in sys.argv[1:]], separators=(",", ":")))
' ${ENTRIES[@]+"${ENTRIES[@]}"})"
REFUSED_JSON="$(python3 -c '
import json, sys
print(json.dumps(sys.argv[1:], separators=(",", ":")))
' ${REFUSED[@]+"${REFUSED[@]}"})"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    {
        echo "matrix=${MATRIX}"
        echo "refused=${REFUSED_JSON}"
    } >>"$GITHUB_OUTPUT"
else
    echo "matrix=${MATRIX}"
    echo "refused=${REFUSED_JSON}"
fi
if [[ ${#ENTRIES[@]} -eq 0 ]]; then
    echo "Nothing to release."
else
    echo "Release matrix: ${MATRIX}"
fi
if [[ ${#REFUSED[@]} -gt 0 ]]; then
    echo "::warning::${#REFUSED[@]} version(s) refused by the release-line law: ${REFUSED_JSON}"
fi
