#!/usr/bin/env bash
set -euo pipefail

SRC_SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for tool in git python3 cargo; do
    command -v "$tool" >/dev/null || { echo "${tool} is required" >&2; exit 2; }
done
REAL_CARGO="$(command -v cargo)"
export REAL_CARGO

WORK="$(mktemp -d "${TMPDIR:-/tmp}/test-release.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME=scenario GIT_AUTHOR_EMAIL=scenario@example.invalid
export GIT_COMMITTER_NAME=scenario GIT_COMMITTER_EMAIL=scenario@example.invalid
unset GITHUB_OUTPUT GITHUB_REF GITHUB_REF_NAME GITHUB_SHA GITHUB_REPOSITORY GITHUB_BASE_REF \
    EVENT_NAME INPUT_VERSION BASE_REF GATE_REV RELEASE_BRANCHES \
    BR_REGISTRY_KEY BR_REGISTRY_URL REGISTRY_WORK

ORIGIN="$WORK/origin.git"
DEV="$WORK/dev"
STUB_DIR="$WORK/stubs"
export STUB_DIR

LIBRARY_ID="01a13598-ee69-7ff0-9c3d-4b8b15209030"
export EXTERNAL_ID="01a00000-0000-7000-8000-000000000009"
export LOST_ID="01a00000-0000-7000-8000-00000000000a"
export LIBRARY="br-service-engine"

failures=0
cases=0
OUT=""
RC=0

g() { git -C "$DEV" "$@"; }

run() {
    RC=0
    OUT="$("$@" 2>&1)" || RC=$?
}

verdict() {
    local ok="$1" what="$2"
    cases=$((cases + 1))
    if [[ "$ok" == yes ]]; then
        printf '  ok  %s\n' "$what"
    else
        failures=$((failures + 1))
        printf 'FAIL  %s (exit %s)\n' "$what" "$RC"
        printf '%s\n' "$OUT" | sed 's/^/        | /' | head -12
    fi
}

expect() {
    local want_rc="$1" needle="$2" what="$3"
    shift 3
    run "$@"
    if [[ "$RC" -eq "$want_rc" ]] && { [[ -z "$needle" ]] || grep -qF -- "$needle" <<<"$OUT"; }; then
        verdict yes "$what"
    else
        verdict no "$what"
    fi
}

expect_equal() {
    if [[ "$1" == "$2" ]]; then verdict yes "$3"; else OUT="got: $1 / want: $2"; verdict no "$3"; fi
}

expect_file_has() {
    if grep -qF -- "$2" "$1" 2>/dev/null; then verdict yes "$3"; else OUT="$(cat "$1" 2>/dev/null || true)"; verdict no "$3"; fi
}

expect_file_lacks() {
    if grep -qF -- "$2" "$1" 2>/dev/null; then OUT="$(cat "$1")"; verdict no "$3"; else verdict yes "$3"; fi
}

summary() {
    echo
    if [[ "$failures" -gt 0 ]]; then
        echo "${failures}/${cases} case(s) failed" >&2
        exit 1
    fi
    echo "${cases} cases passed"
}

install_stubs() {
    mkdir -p "$WORK/bin" "$STUB_DIR/requests"
    cat >"$WORK/bin/curl" <<'STUB'
#!/usr/bin/env bash
out=""
data=""
while [ $# -gt 0 ]; do
    case "$1" in
        -o) out="$2"; shift ;;
        --data-binary) data="${2#@}"; shift ;;
    esac
    shift
done
n=$(( $(ls "$STUB_DIR/requests" | wc -l) + 1 ))
cp "$data" "$STUB_DIR/requests/$n.json"
field=$(grep -o 'libraries[A-Za-z]*(' "$data" | head -1 | tr -d '(')
if [ -f "$STUB_DIR/resp-$field.json" ]; then
    cp "$STUB_DIR/resp-$field.json" "$out"
elif [ "$field" = librariesLookupPackages ]; then
    printf '{"data":{"librariesLookupPackages":[]}}' >"$out"
else
    printf '{"data":{"%s":{"success":true,"warnings":[]}}}' "$field" >"$out"
fi
cat "$STUB_DIR/status-$field" 2>/dev/null || echo 200
STUB
    cat >"$WORK/bin/gh" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "gh $*" >>"$STUB_DIR/gh.log"
[ "${1:-}" = release ] && [ "${2:-}" = view ] && exit 1
exit 0
STUB
    cat >"$WORK/bin/cargo" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = public-api ]; then
    printf '%s\n' "$*" >>"$STUB_DIR/cargo.log"
    if printf ' %s ' "$@" | grep -q ' diff '; then
        printf 'DIFF-OUTPUT\n'
    else
        printf 'pub fn listing()\n'
    fi
    exit 0
fi
exec "$REAL_CARGO" "$@"
STUB
    chmod +x "$WORK/bin/curl" "$WORK/bin/gh" "$WORK/bin/cargo"
    export PATH="$WORK/bin:$PATH"
}

stub_reset() {
    rm -rf "$STUB_DIR"
    mkdir -p "$STUB_DIR/requests"
}

stub_response() {
    printf '%s' "$2" >"$STUB_DIR/resp-$1.json"
}

stub_error() {
    stub_response "$1" "{\"errors\":[{\"message\":\"$3\",\"extensions\":{\"reason\":\"$2\"}}],\"data\":null}"
}

eligible_target() {
    stub_response librariesValidateReleaseTarget "{\"data\":{\"librariesValidateReleaseTarget\":{\"baselineTag\":${1:-null},\"baselineVersion\":null,\"libraryId\":\"$LIBRARY_ID\",\"line\":\"0.4\",\"opensLine\":${2:-true},\"version\":\"$3\"}}}"
}

write_crate() {
    local dir="$DEV/crates/$1"
    mkdir -p "$dir/src"
    {
        printf '[package]\nname = "%s"\nversion.workspace = true\nedition = "2024"\n' "$1"
        printf '%b' "${2:-}"
    } >"$dir/Cargo.toml"
    echo "pub fn x() {}" >"$dir/src/lib.rs"
}

write_vendor() {
    mkdir -p "$DEV/vendor/$1/src"
    printf '[package]\nname = "%s"\nversion = "%s"\nedition = "2024"\n' "$1" "$2" >"$DEV/vendor/$1/Cargo.toml"
    echo "pub fn v() {}" >"$DEV/vendor/$1/src/lib.rs"
}

make_fixture() {
    git init -q --bare -b main "$ORIGIN"
    git clone -q "$ORIGIN" "$DEV" 2>/dev/null
    g checkout -q -b main
    mkdir -p "$DEV/.github"
    cp -R "$SRC_SCRIPTS" "$DEV/.github/scripts"
    printf '[workspace]\nresolver = "3"\nmembers = ["crates/*"]\nexclude = ["vendor"]\n\n[workspace.package]\nversion = "0.4.1"\n' >"$DEV/Cargo.toml"
    write_vendor extdep 2.3.4
    write_vendor lostdep 1.3.0
    write_vendor ghostdep 5.0.0
    write_vendor devdep 9.9.9
    write_crate service-engine '\n[dependencies]\nextdep = { path = "../../vendor/extdep" }\nlostdep = { path = "../../vendor/lostdep" }\nghostdep = { path = "../../vendor/ghostdep" }\n\n[dev-dependencies]\ndevdep = { path = "../../vendor/devdep" }\n'
    write_crate conformance-service-engine '\n[dependencies]\nservice-engine = { path = "../service-engine" }\nextdep = { path = "../../vendor/extdep" }\n'
    write_crate example-app '\n[dependencies]\nservice-engine = { path = "../service-engine" }\n'
    printf 'library-id = "%s"\n' "$LIBRARY_ID" >"$DEV/registry.toml"
    printf '# Changelog\n\n## 0.4.1 - 2026-10-08\n\n- shipped\n' >"$DEV/CHANGELOG.md"
    printf 'target\n' >"$DEV/.gitignore"
    (cd "$DEV" && cargo generate-lockfile --offline -q)
    g add -A && g commit -q -m "fixture" && g push -q origin main
    g tag -a v0.4.1 -m legacy
    g push -q origin v0.4.1
}

set_version() {
    sed -e "s/^version = \".*\"/version = \"$1\"/" "$DEV/Cargo.toml" >"$DEV/Cargo.toml.new" && mv "$DEV/Cargo.toml.new" "$DEV/Cargo.toml"
    printf '\n## %s\n\n- change\n' "$1" >>"$DEV/CHANGELOG.md"
    (cd "$DEV" && cargo generate-lockfile --offline -q)
}

commit_all() {
    g add -A && g commit -q -m "$1" --allow-empty && g rev-parse HEAD
}

in_dev() {
    (cd "$DEV" && "$@")
}
