#!/usr/bin/env bash
LIBRARY_META_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$LIBRARY_META_DIR/tracked-libraries.sh"
LIBRARY_UUID_RE='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'

release_tag() {
    case " ${LEGACY_TAG_VERSIONS:-} " in
        *" $1 "*) printf 'v%s\n' "$1" ;;
        *) printf '%s/v%s\n' "$LIBRARY_NAME" "$1" ;;
    esac
}

non_release() {
    [[ "$1" == *-dev* || "$1" == *-rc* || "$1" == *-pre* ]]
}

assert_lockstep() {
    local rev="${1:-}" package manifest content
    for package in $LIBRARY_PACKAGES; do
        manifest="crates/${package}/Cargo.toml"
        if [[ -n "$rev" ]]; then
            content="$(git show "${rev}:${manifest}" 2>/dev/null)" || content=""
        else
            content="$(cat "$manifest" 2>/dev/null)" || content=""
        fi
        if ! grep -qE '^version\.workspace *= *true' <<<"$content"; then
            echo "::error::library ids: ${manifest} must inherit version.workspace = true: ${LIBRARY_NAME} releases its packages under one version." >&2
            return 1
        fi
    done
}

library_ids() {
    local want_version="${1:-}" rev="${2:-}" declared
    local file="registry.toml"

    if [[ ! -f "$file" ]]; then
        echo "::error::library ids: ${file} not found. The library commits its id: library-id = \"<uuid>\"." >&2
        return 1
    fi

    local stray
    stray=$(grep -vE '^[[:space:]]*(library-id[[:space:]]*=[[:space:]]*"[^"]*"[[:space:]]*)?$' "$file" || true)
    if [[ -n "$stray" ]]; then
        echo "::error::library ids: ${file} must hold exactly one key, library-id; found: ${stray%%$'\n'*}" >&2
        return 1
    fi

    LIB_REG_LIBRARY_ID=$(sed -n 's/^[[:space:]]*library-id[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$file" | head -1)
    if [[ ! "$LIB_REG_LIBRARY_ID" =~ $LIBRARY_UUID_RE ]]; then
        echo "::error::library ids: ${file} has no well-formed library-id (got '${LIB_REG_LIBRARY_ID}')." >&2
        return 1
    fi

    assert_lockstep "$rev" || return 1

    if [[ -n "$want_version" ]]; then
        declared=$(bash "$LIBRARY_META_DIR/library-version.sh" "$rev") || return 1
        if [[ "$declared" != "$want_version" ]]; then
            echo "::error::library ids: asked to act on ${LIBRARY_NAME} ${want_version}, but ${rev:-this checkout} declares ${declared} in Cargo.toml. The API listing and the dependencies are read from that tree, so proceeding would describe ${declared} inside ${want_version}'s registry patch." >&2
            return 1
        fi
    fi
}
