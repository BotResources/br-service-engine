#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "usage: $0 <version> <commit> [<baseline-tag>]" >&2
    exit 2
fi

VERSION="$1"
COMMIT="$2"
BASELINE_TAG="${3:-}"

cd "$(git rev-parse --show-toplevel)"
ROOT="$PWD"
SCRIPTS="$ROOT/.github/scripts"
source "$SCRIPTS/library-meta.sh"
source "$SCRIPTS/registry-gql.sh"

require_registry_key "registry prepare (${LIBRARY_NAME} ${VERSION})"
library_ids "$VERSION" "$COMMIT"
LIBRARY_ID="$LIB_REG_LIBRARY_ID"

OUT="${REGISTRY_WORK:-$ROOT/target/registry}/${LIBRARY_NAME}-${VERSION}"
rm -rf "$OUT"
mkdir -p "$OUT"

WORK="$(mktemp -d)"
SRC="$ROOT"
cleanup() {
    if [[ "$SRC" != "$ROOT" ]]; then
        git worktree remove --force "$SRC" 2>/dev/null || true
        git worktree prune
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

if [[ "$(git rev-parse "${COMMIT}^{commit}")" != "$(git rev-parse HEAD)" ]]; then
    SRC="$WORK/tree"
    git worktree add --detach "$SRC" "$COMMIT" >/dev/null
fi

if [[ -n "$BASELINE_TAG" ]] && ! git rev-parse -q --verify "refs/tags/${BASELINE_TAG}" >/dev/null; then
    echo "::error::registry prepare: the baseline tag ${BASELINE_TAG} named by the registry is not in this clone." >&2
    exit 1
fi

: >"$OUT/listing.txt"
: >"$OUT/diff.txt"
for package in $LIBRARY_PACKAGES; do
    listing="$(cd "$SRC" && cargo public-api -p "$package" --color never --omit blanket-impls,auto-trait-impls,auto-derived-impls)"
    if [[ -z "$listing" ]]; then
        echo "::error::registry prepare: cargo public-api printed an empty API for ${package} at ${COMMIT}." >&2
        exit 1
    fi
    printf '## %s\n%s\n' "$package" "$listing" >>"$OUT/listing.txt"
    if [[ -n "$BASELINE_TAG" ]]; then
        changes="$(cd "$SRC" && cargo public-api -p "$package" --color never --omit blanket-impls,auto-trait-impls,auto-derived-impls diff "${BASELINE_TAG}..${COMMIT}")"
        printf '## %s\n%s\n' "$package" "$changes" >>"$OUT/diff.txt"
    fi
done

(cd "$SRC" && cargo metadata --format-version 1 --locked --all-features) >"$WORK/metadata.json"

python3 - "$WORK/metadata.json" "$LIBRARY_PACKAGES" >"$WORK/external.json" <<'PY'
import json, sys

meta = json.load(open(sys.argv[1]))
library = sys.argv[2].split()
packages = {p["id"]: p for p in meta["packages"]}
members = set(meta["workspace_members"])
nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}

external = set()
for name in library:
    root = next(p for p in meta["packages"] if p["name"] == name and p["id"] in members)
    for dep in nodes[root["id"]]["deps"]:
        if all(kind["kind"] == "dev" for kind in dep["dep_kinds"]):
            continue
        package = packages[dep["pkg"]]
        if package["name"] in library:
            continue
        if dep["pkg"] in members:
            sys.exit("::error::registry prepare: %s depends on the workspace member %s, which the library does not carry." % (name, package["name"]))
        external.add((package["name"], package["version"]))
json.dump([{"packageName": n, "version": v} for n, v in sorted(external)], sys.stdout)
PY

python3 -c '
import json, sys
json.dump({"p": json.load(open(sys.argv[1]))}, open(sys.argv[2], "w"))
' "$WORK/external.json" "$WORK/lookup-vars.json"

echo '{"data":{"librariesLookupPackages":[]}}' >"$WORK/lookup.json"
if [[ "$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))))' "$WORK/external.json")" -gt 0 ]]; then
    registry_post 'query($p: [LibrariesPackageRef!]!) { librariesLookupPackages(packages: $p) { found packageName version line lineStatus library { id name } } }' \
        "$WORK/lookup-vars.json" "$WORK/lookup.json"
    read -r VERDICT REASON MESSAGE <<<"$(registry_verdict "$WORK/lookup.json")"
    if [[ "$VERDICT" != "OK" ]]; then
        echo "::error::registry prepare: $(registry_refusal_text "package look-up for ${LIBRARY_NAME} ${VERSION}" "$REASON" "$MESSAGE")" >&2
        exit 1
    fi
fi

python3 - "$WORK/lookup.json" "$LIBRARY_ID" >"$OUT/dependencies.json" <<'PY'
import json, sys

answers = (json.load(open(sys.argv[1])).get("data") or {}).get("librariesLookupPackages") or []
own = sys.argv[2]
seen, dependencies = set(), []
for entry in answers:
    library = entry.get("library")
    if not (entry.get("found") and entry.get("line") and library):
        continue
    key = (library["id"], entry["version"])
    if library["id"] == own or key in seen:
        continue
    seen.add(key)
    dependencies.append({"libraryId": library["id"], "version": entry["version"]})
dependencies.sort(key=lambda e: (e["libraryId"], e["version"]))
json.dump(dependencies, sys.stdout, indent=1)
PY

echo "registry prepare: ${LIBRARY_NAME} ${VERSION} at ${COMMIT}"
echo "  api listing: $(wc -c <"$OUT/listing.txt" | tr -d ' ') bytes"
echo "  api diff:    $(wc -c <"$OUT/diff.txt" | tr -d ' ') bytes (baseline: ${BASELINE_TAG:-none})"
echo "  dependencies on registered libraries: $(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))))' "$OUT/dependencies.json")"
