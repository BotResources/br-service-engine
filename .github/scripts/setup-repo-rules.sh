#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DECL="$ROOT/.github/required-checks.json"
DRY_RUN=false
VERIFY_ONLY=false

if [[ -z "${REPO:-}" ]]; then
    origin="$(git -C "$ROOT" remote get-url origin 2>/dev/null || true)"
    REPO="$(sed -E 's#^git@github\.com:##; s#^https://github\.com/##; s#\.git$##' <<<"$origin")"
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)     DRY_RUN=true; shift ;;
        --verify-only) VERIFY_ONLY=true; shift ;;
        -h|--help)
            echo "usage: [REPO=owner/name] $0 [--dry-run | --verify-only]"
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

if [[ -z "${REPO}" || "${REPO}" != */* ]]; then
    echo "ERROR: could not read the repository from the 'origin' remote. Pass it: REPO=owner/name $0" >&2
    exit 1
fi
if [[ ! -f "$DECL" ]]; then
    echo "ERROR: $DECL not found" >&2
    exit 1
fi

CONTEXTS="$(jq -c '.required' "$DECL")"
if [[ "$(jq 'length' <<<"$CONTEXTS")" -eq 0 ]]; then
    echo "ERROR: $DECL declares no required check" >&2
    exit 1
fi

checks_param="$(jq -c '{strict_required_status_checks_policy: true, required_status_checks: map({context: .})}' <<<"$CONTEXTS")"

branch_ruleset="$(jq -n --argjson checks "$checks_param" '{
    name: "release-lines",
    target: "branch",
    enforcement: "active",
    bypass_actors: [],
    conditions: {ref_name: {include: ["refs/heads/release/**"], exclude: []}},
    rules: [
        {type: "deletion"},
        {type: "non_fast_forward"},
        {type: "required_linear_history"},
        {type: "pull_request", parameters: {
            required_approving_review_count: 0,
            dismiss_stale_reviews_on_push: true,
            require_code_owner_review: false,
            require_last_push_approval: false,
            required_review_thread_resolution: true
        }},
        {type: "required_status_checks", parameters: $checks}
    ]
}')"

tag_ruleset='{
    "name": "release-tags",
    "target": "tag",
    "enforcement": "active",
    "bypass_actors": [],
    "conditions": {"ref_name": {"include": ["refs/tags/**/v*"], "exclude": []}},
    "rules": [{"type": "deletion"}, {"type": "non_fast_forward"}, {"type": "update"}]
}'

main_checks="$(jq -c '{strict: true, contexts: (. | sort)}' <<<"$CONTEXTS")"

RULESET_SHAPE='{
    target,
    enforcement,
    bypass: (.bypass_actors | length),
    include: .conditions.ref_name.include,
    exclude: .conditions.ref_name.exclude,
    rules: ([.rules[].type] | sort),
    checks: ([.rules[] | select(.type == "required_status_checks") | .parameters.required_status_checks[].context] | sort)
}'

echo "repo: ${REPO}"
echo "required checks:"
jq -r '.[] | "  - " + .' <<<"$CONTEXTS"
echo "rulesets: release-lines (refs/heads/release/**), release-tags (refs/tags/**/v*)"
echo "main: required status checks replaced by the declared list"
echo "security: secret scanning, push protection"
echo "actions: fork PR approval for all outside contributors, SHA pinning enforced"

if [[ "${DRY_RUN}" == "true" ]]; then
    echo "DRY-RUN: nothing applied"
    exit 0
fi

if ! gh auth status >/dev/null 2>&1; then
    echo "ERROR: gh CLI is not authenticated" >&2
    exit 1
fi

upsert_ruleset() {
    local name="$1" payload="$2" id
    id="$(gh api "repos/${REPO}/rulesets" --jq ".[] | select(.name == \"${name}\") | .id")"
    if [[ -n "$id" ]]; then
        gh api -X PUT --input - "repos/${REPO}/rulesets/${id}" <<<"$payload" >/dev/null
        echo "ruleset ${name}: updated"
    else
        gh api -X POST --input - "repos/${REPO}/rulesets" <<<"$payload" >/dev/null
        echo "ruleset ${name}: created"
    fi
}

apply_rules() {
    upsert_ruleset release-lines "$branch_ruleset"
    upsert_ruleset release-tags "$tag_ruleset"
    gh api -X PATCH --input - "repos/${REPO}/branches/main/protection/required_status_checks" <<<"$main_checks" >/dev/null
    gh api -X PATCH --input - "repos/${REPO}" >/dev/null <<<'{"security_and_analysis":{"secret_scanning":{"status":"enabled"},"secret_scanning_push_protection":{"status":"enabled"}}}'
    gh api -X PUT --input - "repos/${REPO}/actions/permissions/fork-pr-contributor-approval" >/dev/null <<<'{"approval_policy":"all_external_contributors"}'
    gh api -X PUT --input - "repos/${REPO}/actions/permissions" >/dev/null <<<'{"enabled":true,"allowed_actions":"all","sha_pinning_required":true}'
    echo "applied"
}

read_main_checks() {
    gh api "repos/${REPO}/branches/main/protection/required_status_checks" --jq '{strict, contexts: (.contexts | sort)}'
}

read_ruleset() {
    local id
    id="$(gh api "repos/${REPO}/rulesets" --jq ".[] | select(.name == \"$1\") | .id")"
    if [[ -z "$id" ]]; then
        echo null
        return
    fi
    gh api "repos/${REPO}/rulesets/${id}" --jq "$RULESET_SHAPE"
}

read_security() {
    gh api "repos/${REPO}" --jq '{secret_scanning: .security_and_analysis.secret_scanning.status, push_protection: .security_and_analysis.secret_scanning_push_protection.status}'
}

read_fork_approval() {
    gh api "repos/${REPO}/actions/permissions/fork-pr-contributor-approval" --jq '{approval_policy}'
}

read_sha_pinning() {
    gh api "repos/${REPO}/actions/permissions" --jq '{sha_pinning_required}'
}

FAILURES=0

expect() {
    local label="$1" expected actual
    expected="$(jq -cS . <<<"$2")"
    shift 2
    for _ in 1 2 3; do
        actual="$("$@" 2>/dev/null | jq -cS . 2>/dev/null || true)"
        if [[ "$actual" == "$expected" ]]; then
            echo "verified  ${label}"
            return 0
        fi
        sleep 2
    done
    echo "MISMATCH  ${label}"
    echo "  expected: ${expected}"
    echo "  actual  : ${actual:-unreadable}"
    FAILURES=$((FAILURES + 1))
}

verify_rules() {
    expect "main required status checks" "$main_checks" read_main_checks
    expect "ruleset release-lines" "$(jq -c "$RULESET_SHAPE" <<<"$branch_ruleset")" read_ruleset release-lines
    expect "ruleset release-tags" "$(jq -c "$RULESET_SHAPE" <<<"$tag_ruleset")" read_ruleset release-tags
    expect "secret scanning and push protection" '{"secret_scanning":"enabled","push_protection":"enabled"}' read_security
    expect "fork PR approval" '{"approval_policy":"all_external_contributors"}' read_fork_approval
    expect "SHA pinning" '{"sha_pinning_required":true}' read_sha_pinning
    if [[ "$FAILURES" -gt 0 ]]; then
        echo "${FAILURES} setting(s) differ from the declared state" >&2
        exit 1
    fi
    echo "all settings match the declared state"
}

if [[ "${VERIFY_ONLY}" != "true" ]]; then
    apply_rules
fi
verify_rules
