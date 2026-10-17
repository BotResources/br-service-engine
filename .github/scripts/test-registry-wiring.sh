#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/test-lib.sh"

make_fixture
install_stubs
FIXTURE_COMMIT="$(g rev-parse HEAD)"
set_version 0.4.2
RELEASE_COMMIT="$(commit_all "0.4.2")"
g push -q origin main

gate() { in_dev env BR_REGISTRY_KEY=k GITHUB_OUTPUT="$WORK/gate.out" bash .github/scripts/registry-gate.sh "$@"; }
gate_nokey() { in_dev bash .github/scripts/registry-gate.sh "$@"; }
record() { in_dev env BR_REGISTRY_KEY=k REGISTRY_WORK="$WORK/reg" bash .github/scripts/registry-record.sh "$@"; }
release() { in_dev env GITHUB_REPOSITORY=o/r bash .github/scripts/release-library.sh "$@"; }
requests() { ls "$STUB_DIR/requests" | wc -l | tr -d ' '; }
request_field() { python3 -c '
import json, sys
d = json.load(open(sys.argv[1]))
v = d["variables"]
for key in sys.argv[2].split("."):
    v = v[int(key)] if key.isdigit() else v[key]
print(json.dumps(v) if not isinstance(v, str) else v)
' "$STUB_DIR/requests/$1.json" "$2"; }
lookup_answers() {
    stub_response librariesLookupPackages "{\"data\":{\"librariesLookupPackages\":[
        {\"found\":true,\"packageName\":\"extdep\",\"version\":\"2.3.4\",\"line\":\"2.3\",\"lineStatus\":\"ACTIVE\",\"library\":{\"id\":\"$EXTERNAL_ID\",\"name\":\"extdep\"}},
        {\"found\":true,\"packageName\":\"lostdep\",\"version\":\"1.3.0\",\"line\":null,\"lineStatus\":null,\"library\":{\"id\":\"$LOST_ID\",\"name\":\"lostdep\"}},
        {\"found\":false,\"packageName\":\"ghostdep\",\"version\":\"5.0.0\",\"line\":null,\"lineStatus\":null,\"library\":null}
    ]}}"
}

echo "== registry gate"
stub_reset
expect 1 'BR_REGISTRY_KEY is not set' "no registry key: refuses" gate_nokey 0.4.2
eligible_target '"v0.4.1"' false 0.4.2
: >"$WORK/gate.out"
expect 0 'eligible for release' "eligible target" gate 0.4.2
expect_file_has "$WORK/gate.out" 'baseline_tag=v0.4.1' "baseline tag written to GITHUB_OUTPUT"
expect_file_has "$WORK/gate.out" 'opens_line=false' "opensLine written to GITHUB_OUTPUT"
expect_file_has "$WORK/gate.out" 'implemented=false' "implemented=false written"
expect_file_has "$STUB_DIR/requests/1.json" "$LIBRARY_ID" "the committed library id is the one queried"
expect 1 'declares 0.4.2 in Cargo.toml' "version differing from Cargo.toml: refuses" gate 0.9.0
for reason in FORBIDDEN NOT_FOUND VERSION_INVALID PATCH_NOT_FOUND PATCH_NOT_SEALED LINE_RETIRED; do
    stub_error librariesValidateReleaseTarget "$reason" "m"
    expect 1 "($reason)" "refusal $reason maps to a failure naming it" gate 0.4.2
done
stub_error librariesValidateReleaseTarget PATCH_IMPLEMENTED "m"
expect 1 'immutable' "PATCH_IMPLEMENTED without a tag argument: fails" gate 0.4.2
expect 1 'does not exist in this repository' "PATCH_IMPLEMENTED, tag argument but no tag: fails" gate 0.4.2 "$LIBRARY/v0.4.2"
g tag -a "$LIBRARY/v0.4.2" -m t "$RELEASE_COMMIT"
: >"$WORK/gate.out"
expect 0 'idempotent re-run' "PATCH_IMPLEMENTED with the tag present: exit 0" gate 0.4.2 "$LIBRARY/v0.4.2"
expect_file_has "$WORK/gate.out" 'implemented=true' "implemented=true written"
g tag -d "$LIBRARY/v0.4.2" >/dev/null
printf '<html>' >"$STUB_DIR/resp-librariesValidateReleaseTarget.json"
printf '502' >"$STUB_DIR/status-librariesValidateReleaseTarget"
expect 1 'no eligible verdict' "gateway error: fails" gate 0.4.2

echo "== committed library id"
stub_reset
eligible_target null true 0.4.2
mv "$DEV/registry.toml" "$WORK/registry.toml"
expect 1 'not found' "missing registry.toml: refuses" gate 0.4.2
printf 'library-id = "not-a-uuid"\n' >"$DEV/registry.toml"
expect 1 'well-formed' "malformed id: refuses" gate 0.4.2
printf 'library-id = "%s"\nextra = "x"\n' "$LIBRARY_ID" >"$DEV/registry.toml"
expect 1 'exactly one key' "a second key: refuses" gate 0.4.2
mv "$WORK/registry.toml" "$DEV/registry.toml"

echo "== tag and release"
gh_log="$STUB_DIR/gh.log"
expect 0 "creating tag $LIBRARY/v0.4.2" "tag $LIBRARY/v0.4.2 created" release 0.4.2 refs/heads/main "$RELEASE_COMMIT"
expect_file_has "$gh_log" '--latest=true' "main moves Latest"
expect_file_has "$gh_log" "release create $LIBRARY/v0.4.2 --title $LIBRARY 0.4.2" "the GitHub release carries the library name and version"
expect 0 '' "tag pushed to origin" git -C "$ORIGIN" rev-parse -q --verify "refs/tags/$LIBRARY/v0.4.2"
expect 0 'already exists' "second run: tag exists, nothing recreated" release 0.4.2 refs/heads/main "$RELEASE_COMMIT"
expect 0 'already exists' "the imported version keeps v0.4.1: no new tag" release 0.4.1 refs/heads/main "$FIXTURE_COMMIT"
expect 1 '' "no $LIBRARY/v0.4.1 tag was created" git -C "$ORIGIN" rev-parse -q --verify "refs/tags/$LIBRARY/v0.4.1"
g branch -q "release/$LIBRARY-0.4" "$RELEASE_COMMIT"
g tag -d "$LIBRARY/v0.4.2" >/dev/null
git -C "$ORIGIN" tag -d "$LIBRARY/v0.4.2" >/dev/null
: >"$gh_log"
expect 0 "creating tag $LIBRARY/v0.4.2" "tag from a release line" release 0.4.2 "refs/heads/release/$LIBRARY-0.4" "$RELEASE_COMMIT"
expect_file_has "$gh_log" '--latest=false' "a release line never moves Latest"
expect 1 'does not declare' "tagging a version the commit does not carry: refuses" release 0.9.0 refs/heads/main "$RELEASE_COMMIT"

echo "== record: both packages, dependencies kept only on a real line"
stub_reset
rm -rf "$WORK/reg"
lookup_answers
expect 0 "$LIBRARY 0.4.2 recorded" "recorded" record 0.4.2 "$LIBRARY/v0.4.2" "$RELEASE_COMMIT"
expect_equal "$(requests)" 4 "look-up plus three records"
expect_equal "$(request_field 1 p)" '[{"packageName": "extdep", "version": "2.3.4"}, {"packageName": "ghostdep", "version": "5.0.0"}, {"packageName": "lostdep", "version": "1.3.0"}]' "look-up asks for the external packages of both packages once each, without the dev-dependency, the library's own packages or the example crate"
expect_equal "$(request_field 2 input.generated)" true "listing is marked generated"
expect_file_has "$STUB_DIR/cargo.log" "public-api -p service-engine --color never" "the listing is taken for service-engine"
expect_file_has "$STUB_DIR/cargo.log" "public-api -p conformance-service-engine --color never" "the listing is taken for conformance-service-engine"
expect_equal "$(request_field 2 input.apiListing)" "$(printf '## service-engine\npub fn listing()\n## conformance-service-engine\npub fn listing()')" "one listing, one section per package"
expect_equal "$(request_field 2 input.apiDiff)" "" "no baseline: empty diff"
expect_equal "$(request_field 3 input.dependencies)" "[{\"libraryId\": \"$EXTERNAL_ID\", \"version\": \"2.3.4\"}]" "a package found on a line is a dependency; one found without a line and one not found are not"
expect_equal "$(request_field 4 input.releaseTag)" "$LIBRARY/v0.4.2" "release tag recorded"
expect_equal "$(request_field 4 input.releaseCommit)" "$RELEASE_COMMIT" "release commit recorded"
expect_equal "$(request_field 4 input.libraryId)" "$LIBRARY_ID" "the library id is the committed one"

echo "== record: no registered dependency at all"
stub_reset
rm -rf "$WORK/reg"
stub_response librariesLookupPackages '{"data":{"librariesLookupPackages":[]}}'
expect 0 'recorded' "empty look-up answer" record 0.4.2 "$LIBRARY/v0.4.2" "$RELEASE_COMMIT"
expect_equal "$(request_field 3 input.dependencies)" "[]" "no dependency: empty set"

echo "== record: the imported version records its legacy tag"
stub_reset
rm -rf "$WORK/reg"
g tag -d "$LIBRARY/v0.4.2" >/dev/null
lookup_answers
expect 0 'recorded' "0.4.1 recorded at the legacy tag's commit" record 0.4.1 v0.4.1 "$FIXTURE_COMMIT"
expect_equal "$(request_field 4 input.releaseTag)" "v0.4.1" "the existing tag is the one recorded"
expect_equal "$(request_field 4 input.releaseCommit)" "$FIXTURE_COMMIT" "the legacy tag's commit is recorded"
expect 1 'must be v0.4.1' "0.4.1 under the new tag format: refuses" record 0.4.1 "$LIBRARY/v0.4.1" "$FIXTURE_COMMIT"
g tag -a "$LIBRARY/v0.4.2" -m t "$RELEASE_COMMIT"

echo "== record: baseline"
stub_reset
rm -rf "$WORK/reg"
lookup_answers
stub_response librariesRecordDependencies '{"data":{"librariesRecordDependencies":{"success":true,"warnings":[{"code":"DEPENDENCY_LINE_DEPRECATED","params":{"x":1}}]}}}'
expect 0 '::warning::registry: DEPENDENCY_LINE_DEPRECATED' "warnings printed as ::warning::" record 0.4.2 "$LIBRARY/v0.4.2" "$RELEASE_COMMIT" v0.4.1
expect_equal "$(request_field 2 input.apiDiff)" "$(printf '## service-engine\nDIFF-OUTPUT\n## conformance-service-engine\nDIFF-OUTPUT')" "baseline: one diff section per package"
expect_file_has "$STUB_DIR/cargo.log" "diff v0.4.1..$RELEASE_COMMIT" "diff is taken from the baseline tag to the release commit"
rm -rf "$WORK/reg"
expect 1 'is not in this clone' "a baseline tag the clone lacks: refuses" record 0.4.2 "$LIBRARY/v0.4.2" "$RELEASE_COMMIT" v0.0.7

echo "== record: a dependency on a workspace member the library does not carry"
mkdir -p "$DEV/crates/example-util/src"
printf '[package]\nname = "example-util"\nversion.workspace = true\nedition = "2024"\n' >"$DEV/crates/example-util/Cargo.toml"
echo "pub fn u() {}" >"$DEV/crates/example-util/src/lib.rs"
sed -i.bak 's#^\[dependencies\]#&\nexample-util = { path = "../example-util" }#' "$DEV/crates/service-engine/Cargo.toml" && rm -f "$DEV/crates/service-engine/Cargo.toml.bak"
set_version 0.4.9
LEAK_COMMIT="$(commit_all "service-engine depends on an example crate")"
g tag -a "$LIBRARY/v0.4.9" -m t "$LEAK_COMMIT"
stub_reset
rm -rf "$WORK/reg"
expect 1 'does not carry' "service-engine depending on an example crate: refuses" record 0.4.9 "$LIBRARY/v0.4.9" "$LEAK_COMMIT"
g reset -q --hard "$RELEASE_COMMIT"
g tag -d "$LIBRARY/v0.4.9" >/dev/null

echo "== record: refusals and recovery"
stub_reset
rm -rf "$WORK/reg"
stub_error librariesRecordApiListing API_LISTING_TOO_LARGE "too big"
expect 1 'API_LISTING_TOO_LARGE' "listing too large: surfaced clearly" record 0.4.2 "$LIBRARY/v0.4.2" "$RELEASE_COMMIT"
stub_reset
rm -rf "$WORK/reg"
stub_error librariesRecordRelease PATCH_NOT_SEALED "x"
expect 1 'PATCH_NOT_SEALED' "release record refused: fails after the first two records" record 0.4.2 "$LIBRARY/v0.4.2" "$RELEASE_COMMIT"
stub_reset
rm -rf "$WORK/reg"
stub_error librariesRecordApiListing PATCH_IMPLEMENTED "x"
expect 0 'already implemented' "PATCH_IMPLEMENTED during record: complete, exit 0" record 0.4.2 "$LIBRARY/v0.4.2" "$RELEASE_COMMIT"
OTHER_COMMIT="$(commit_all "another commit carrying the same version")"
expect 1 'does not point at' "tag on another commit: refuses" record 0.4.2 "$LIBRARY/v0.4.2" "$OTHER_COMMIT"
expect 1 "must be $LIBRARY/v0.4.2" "tag in the wrong format: refuses" record 0.4.2 v0.4.2 "$RELEASE_COMMIT"
expect 1 'BR_REGISTRY_KEY is not set' "no key: refuses" in_dev bash .github/scripts/registry-record.sh 0.4.2 "$LIBRARY/v0.4.2" "$RELEASE_COMMIT"

summary
