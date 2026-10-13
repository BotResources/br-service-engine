#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "usage: $0 <version> <tag> <commit> [<baseline-tag>]" >&2
    exit 2
fi

VERSION="$1"
TAG="$2"
COMMIT="$3"
BASELINE_TAG="${4:-}"

cd "$(git rev-parse --show-toplevel)"
ROOT="$PWD"
SCRIPTS="$ROOT/.github/scripts"
source "$SCRIPTS/library-meta.sh"
source "$SCRIPTS/registry-gql.sh"

require_registry_key "registry record (${LIBRARY_NAME} ${VERSION})"
library_ids "$VERSION" "$COMMIT"
LIBRARY_ID="$LIB_REG_LIBRARY_ID"

if [[ "$TAG" != "$(release_tag "$VERSION")" ]]; then
    echo "::error::registry record: the release tag of ${VERSION} must be $(release_tag "$VERSION"), got '${TAG}'." >&2
    exit 1
fi
if [[ "$(git rev-list -n 1 "refs/tags/${TAG}" 2>/dev/null || true)" != "$(git rev-parse "${COMMIT}^{commit}")" ]]; then
    echo "::error::registry record: ${TAG} does not point at ${COMMIT}." >&2
    exit 1
fi

OUT="${REGISTRY_WORK:-$ROOT/target/registry}/${LIBRARY_NAME}-${VERSION}"
if [[ ! -s "$OUT/listing.txt" || ! -f "$OUT/diff.txt" || ! -f "$OUT/dependencies.json" ]]; then
    bash "$SCRIPTS/registry-prepare.sh" "$VERSION" "$COMMIT" "$BASELINE_TAG"
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

record_call() {
    local field="$1" input_type="$2" what="$3" input_file="$4"
    python3 -c '
import json, sys
json.dump({"input": json.load(open(sys.argv[1]))}, open(sys.argv[2], "w"))
' "$input_file" "$WORK/vars.json"
    registry_post "mutation(\$input: ${input_type}!) { ${field}(input: \$input) { success warnings { code params } } }" \
        "$WORK/vars.json" "$WORK/response.json"
    read -r VERDICT REASON MESSAGE <<<"$(registry_verdict "$WORK/response.json")"
    if [[ "$VERDICT" == "REFUSED" && "$REASON" == "PATCH_IMPLEMENTED" ]]; then
        echo "::notice::registry record: ${LIBRARY_NAME} ${VERSION} is already implemented — the records are complete."
        exit 0
    fi
    if [[ "$VERDICT" != "OK" ]]; then
        echo "::error::registry record: $(registry_refusal_text "${what} of ${LIBRARY_NAME} ${VERSION}" "$REASON" "$MESSAGE")" >&2
        exit 1
    fi
    python3 - "$WORK/response.json" "$field" <<'PY'
import json, sys

ack = (json.load(open(sys.argv[1])).get("data") or {}).get(sys.argv[2])
if not isinstance(ack, dict) or ack.get("success") is not True:
    print("::error::registry record: the registry did not acknowledge the record (%r)." % (ack,), file=sys.stderr)
    sys.exit(1)
for warning in ack.get("warnings") or []:
    print("::warning::registry: %s %s" % (warning.get("code"), json.dumps(warning.get("params"))))
PY
    echo "recorded ${what} of ${LIBRARY_NAME} ${VERSION}"
}

python3 -c '
import json, sys
out, library_id, version = sys.argv[1], sys.argv[2], sys.argv[3]
json.dump({
    "libraryId": library_id,
    "version": version,
    "apiListing": open(out + "/listing.txt").read(),
    "apiDiff": open(out + "/diff.txt").read(),
    "generated": True,
}, open(sys.argv[4], "w"))
' "$OUT" "$LIBRARY_ID" "$VERSION" "$WORK/listing-input.json"
echo "api listing $(wc -c <"$OUT/listing.txt" | tr -d ' ') bytes, api diff $(wc -c <"$OUT/diff.txt" | tr -d ' ') bytes"
record_call librariesRecordApiListing LibrariesRecordApiListingInput "API listing" "$WORK/listing-input.json"

python3 -c '
import json, sys
json.dump({
    "libraryId": sys.argv[2],
    "version": sys.argv[3],
    "dependencies": json.load(open(sys.argv[1] + "/dependencies.json")),
}, open(sys.argv[4], "w"))
' "$OUT" "$LIBRARY_ID" "$VERSION" "$WORK/dependencies-input.json"
record_call librariesRecordDependencies LibrariesRecordDependenciesInput "dependencies" "$WORK/dependencies-input.json"

python3 -c '
import json, sys
json.dump({"libraryId": sys.argv[1], "version": sys.argv[2], "releaseTag": sys.argv[3], "releaseCommit": sys.argv[4]}, open(sys.argv[5], "w"))
' "$LIBRARY_ID" "$VERSION" "$TAG" "$(git rev-parse "${COMMIT}^{commit}")" "$WORK/release-input.json"
record_call librariesRecordRelease LibrariesRecordReleaseInput "release" "$WORK/release-input.json"

echo "registry record: ${LIBRARY_NAME} ${VERSION} recorded (${TAG} at ${COMMIT})."
