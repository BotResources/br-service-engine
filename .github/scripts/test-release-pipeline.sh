#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/test-lib.sh"

make_fixture
install_stubs

detect() {
    in_dev env GITHUB_REF="$1" EVENT_NAME="${2:-push}" GITHUB_SHA="$(g rev-parse HEAD)" \
        INPUT_VERSION="${3:-}" bash .github/scripts/release-detect.sh
}

matrix_of() {
    sed -n 's/^matrix=//p' <<<"$OUT" | head -1
}

echo "== the imported version keeps its legacy tag"
FIXTURE_COMMIT="$(g rev-parse HEAD)"
commit_all "next commit" >/dev/null
expect 0 'matrix=[]' "main at 0.4.1 with v0.4.1 present: already shipped" detect refs/heads/main
expect_equal "$(in_dev bash -c 'source .github/scripts/library-meta.sh; release_tag 0.4.1')" "v0.4.1" "0.4.1 resolves to its legacy tag"
expect_equal "$(in_dev bash -c 'source .github/scripts/library-meta.sh; release_tag 0.4.2')" "$LIBRARY/v0.4.2" "0.4.2 resolves to the library tag"

echo "== a version bump is released"
set_version 0.4.2
commit_all "0.4.2" >/dev/null
run detect refs/heads/main
if [[ "$RC" -eq 0 ]] && python3 -c '
import json, sys
m = json.loads(sys.argv[1])
assert [(e["version"], e["line"]) for e in m] == [("0.4.2", "main")], m
assert len(m[0]["commit"]) == 40 and "crate" not in m[0]
' "$(matrix_of)"; then verdict yes "main: exactly 0.4.2 on main at HEAD"; else verdict no "main: exactly 0.4.2 on main at HEAD"; fi

echo "== a non-release version is skipped"
set_version 0.5.0-rc.1
commit_all "rc" >/dev/null
expect 0 'matrix=[]' "0.5.0-rc.1: non-release" detect refs/heads/main

echo "== already tagged and unchanged: nothing to do"
set_version 0.4.2
commit_all "back to 0.4.2" >/dev/null
g tag -a "$LIBRARY/v0.4.2" -m t
g push -q origin "$LIBRARY/v0.4.2"
commit_all "unrelated" >/dev/null
expect 0 'matrix=[]' "tag exists, version unchanged" detect refs/heads/main

echo "== the release-line law"
g branch "release/$LIBRARY-0.4" "$LIBRARY/v0.4.2"
g push -q origin "release/$LIBRARY-0.4"
set_version 0.4.3
commit_all "0.4.3 on main" >/dev/null
run detect refs/heads/main
if [[ "$RC" -eq 0 && "$(matrix_of)" == '[]' ]] && grep -q '^refused=\["br-service-engine 0.4.3' <<<"$OUT"; then verdict yes "main may not publish 0.4.3: refused"; else verdict no "main may not publish 0.4.3: refused"; fi
set_version 0.5.0
commit_all "0.5.0 on main" >/dev/null
run detect refs/heads/main
if grep -q '"version":"0.5.0","line":"main"' <<<"$OUT"; then verdict yes "main publishes 0.5.0"; else verdict no "main publishes 0.5.0"; fi

echo "== a release line publishes its own range only"
g checkout -q "release/$LIBRARY-0.4"
set_version 0.4.3
commit_all "hotfix" >/dev/null
run detect "refs/heads/release/$LIBRARY-0.4"
if grep -q "\"version\":\"0.4.3\",\"line\":\"release/$LIBRARY-0.4\"" <<<"$OUT"; then verdict yes "release/$LIBRARY-0.4: 0.4.3"; else verdict no "release/$LIBRARY-0.4: 0.4.3"; fi
set_version 0.5.1
commit_all "a stray minor on the line" >/dev/null
run detect "refs/heads/release/$LIBRARY-0.4"
if [[ "$RC" -eq 0 && "$(matrix_of)" == '[]' ]] && grep -q '^refused=\["br-service-engine 0.5.1' <<<"$OUT"; then verdict yes "release/$LIBRARY-0.4 refuses 0.5.1"; else verdict no "release/$LIBRARY-0.4 refuses 0.5.1"; fi
g checkout -q -B release/br-other-0.4
expect 1 'not of br-service-engine' "another library's release line does not publish this one" detect refs/heads/release/br-other-0.4
expect 1 'not a release line' "release/oops is not a publishing line" detect refs/heads/release/oops
expect 1 'not a publishing line' "a feature branch does not publish" detect refs/heads/feat/x

echo "== workflow_dispatch"
g checkout -q main
expect 0 '"version":"0.5.0"' "dispatch 0.5.0 from main" detect refs/heads/main workflow_dispatch 0.5.0
expect 1 'declares 0.5.0, not 0.9.0' "dispatch naming another version than the tree" detect refs/heads/main workflow_dispatch 0.9.0
expect 3 'may not publish' "dispatch refused by the law (0.4.3 on main)" detect refs/heads/main workflow_dispatch 0.4.3
expect 3 'may not publish' "dispatch of 0.4.1 from main: the 0.4 line owns it" detect refs/heads/main workflow_dispatch 0.4.1
g checkout -q "release/$LIBRARY-0.4"
g reset -q --hard "$LIBRARY/v0.4.2"
run detect "refs/heads/release/$LIBRARY-0.4" workflow_dispatch 0.4.2
if [[ "$RC" -eq 0 ]] && grep -q "\"commit\":\"$(g rev-parse "$LIBRARY/v0.4.2^{commit}")\"" <<<"$OUT"; then verdict yes "dispatch of a tagged version: the entry carries the tag's commit"; else verdict no "dispatch of a tagged version: the entry carries the tag's commit"; fi
run detect "refs/heads/release/$LIBRARY-0.4" workflow_dispatch 0.4.1
if [[ "$RC" -eq 0 ]] && grep -q "\"version\":\"0.4.1\".*\"commit\":\"$FIXTURE_COMMIT\"" <<<"$OUT"; then verdict yes "dispatch of the imported version: the entry carries the legacy tag's commit"; else verdict no "dispatch of the imported version: the entry carries the legacy tag's commit"; fi

echo "== PR-time gate"
g checkout -q main
commit_all "state" >/dev/null
g push -q origin main
g checkout -q -b feat/nothing
commit_all "no bump" >/dev/null
expect 0 'nothing to gate' "no version bump: nothing to gate" in_dev env BASE_REF=main bash .github/scripts/pr-registry-gate.sh
g checkout -q -b feat/bump main
set_version 0.6.0
commit_all "0.6.0" >/dev/null
expect 1 'BR_REGISTRY_KEY is not set' "bump with no registry key (fork PR): fails loudly" in_dev env BASE_REF=main bash .github/scripts/pr-registry-gate.sh
stub_reset
eligible_target null true 0.6.0
expect 0 'eligible for release' "bump with an eligible target: passes" in_dev env BASE_REF=main BR_REGISTRY_KEY=k bash .github/scripts/pr-registry-gate.sh
stub_error librariesValidateReleaseTarget PATCH_NOT_SEALED "not sealed"
expect 1 'not sealed' "bump on an unsealed patch: fails" in_dev env BASE_REF=main BR_REGISTRY_KEY=k bash .github/scripts/pr-registry-gate.sh
stub_error librariesValidateReleaseTarget PATCH_IMPLEMENTED "implemented"
expect 1 'immutable' "bump on an implemented patch: already released, fails" in_dev env BASE_REF=main BR_REGISTRY_KEY=k bash .github/scripts/pr-registry-gate.sh
g checkout -q main
g branch -q -D feat/bump
g checkout -q -b feat/line main
set_version 0.4.5
commit_all "0.4.5 into main" >/dev/null
expect 3 'may not publish' "bump into main inside a minor a line owns: refused by the law" in_dev env BASE_REF=main BR_REGISTRY_KEY=k bash .github/scripts/pr-registry-gate.sh

echo "== lockstep"
g checkout -q main
g checkout -q -b feat/diverge
sed -i.bak 's/^version\.workspace = true/version = "0.1.0"/' "$DEV/crates/conformance-service-engine/Cargo.toml" && rm -f "$DEV/crates/conformance-service-engine/Cargo.toml.bak"
set_version 0.6.0
commit_all "a package that leaves the lockstep" >/dev/null
stub_reset
eligible_target null true 0.6.0
expect 1 'must inherit version.workspace = true' "a package off the workspace version: refused" in_dev env BASE_REF=main BR_REGISTRY_KEY=k bash .github/scripts/pr-registry-gate.sh

summary
