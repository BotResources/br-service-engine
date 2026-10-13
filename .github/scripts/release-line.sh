#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_RE='^br-[a-z0-9]+(-[a-z0-9]+)*$'
VERSION_RE='^([0-9]+)\.([0-9]+)\.([0-9]+)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$'
BRANCH_RE='^release/(.+)-([0-9]+)\.([0-9]+)$'

die() {
    echo "::error::$*" >&2
    exit 1
}

refuse() {
    echo "::error::$*" >&2
    exit 3
}

usage() {
    cat >&2 <<'USAGE'
usage:
  release-line.sh parse <branch>
  release-line.sh check-ref <git-ref>
  release-line.sh check <library> <version> <git-ref>
  release-line.sh may-move-latest <git-ref>
USAGE
    exit 2
}

branch_of() {
    case "$1" in
        refs/heads/*) printf '%s\n' "${1#refs/heads/}" ;;
        refs/*) printf '\n' ;;
        *) printf '%s\n' "$1" ;;
    esac
}

parse() {
    local branch="$1"
    [[ "$branch" =~ $BRANCH_RE ]] || return 1
    local artifact="${BASH_REMATCH[1]}" major="${BASH_REMATCH[2]}" minor="${BASH_REMATCH[3]}"
    [[ "$artifact" =~ $ARTIFACT_RE ]] || return 1
    printf '%s %s %s\n' "$artifact" "$major" "$minor"
}

release_branches() {
    if [[ -n "${RELEASE_BRANCHES+set}" ]]; then
        printf '%s\n' "$RELEASE_BRANCHES" | tr -s ' ' '\n'
        return 0
    fi
    local listing
    listing="$(git ls-remote --heads origin 'refs/heads/release/*')" \
        || die "release-line: cannot list the release branches on origin, so the one-range-one-branch law cannot be verified."
    awk '{ sub("^refs/heads/", "", $2); print $2 }' <<<"$listing"
}

version_parts() {
    [[ "$1" =~ $VERSION_RE ]] || die "release-line: '$1' is not a version (M.m.p, optional -prerelease and +build, in [0-9A-Za-z.-])."
    printf '%s %s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
}

cmd_check_ref() {
    local ref="$1" branch
    branch="$(branch_of "$ref")"
    if [[ "$branch" == "main" ]]; then
        printf '%s\n' "$branch"
        return 0
    fi
    if [[ "$branch" == release/* ]]; then
        parse "$branch" >/dev/null \
            || die "release-line: '${branch}' is not a release line. A maintenance branch names its library: release/<library>-<major>.<minor> (for example release/br-service-engine-0.4). Nothing publishes from '${branch}'."
        printf '%s\n' "$branch"
        return 0
    fi
    die "release-line: '${ref}' is not a publishing line. Only main and release/<library>-<major>.<minor> branches publish."
}

cmd_check() {
    local artifact="$1" version="$2" ref="$3" parts major minor branch
    [[ "$artifact" =~ $ARTIFACT_RE ]] || die "release-line: '${artifact}' is not a library name (br-<name>)."
    parts="$(version_parts "$version")"
    read -r major minor <<<"$parts"
    branch="$(cmd_check_ref "$ref")"

    local line line_artifact x y
    if [[ "$branch" == "main" ]]; then
        local branches b
        branches="$(release_branches)"
        while read -r b; do
            [[ -n "$b" ]] || continue
            if ! line="$(parse "$b")"; then
                echo "::warning::release-line: ignoring '${b}', not a release/<library>-<major>.<minor> name, so it owns no range." >&2
                continue
            fi
            read -r line_artifact x y <<<"$line"
            [[ "$line_artifact" == "$artifact" ]] || continue
            if (( 10#$major < 10#$x || (10#$major == 10#$x && 10#$minor <= 10#$y) )); then
                refuse "release-line: main may not publish ${artifact} ${version}. ${b} owns ${x}.${y}.z (one minor range, one branch): main publishes ${artifact} only from ${x}.$((10#$y + 1)).0 on. Ship a ${x}.${y} fix from ${b}, or bump main to ${x}.$((10#$y + 1)).0 or above."
            fi
        done <<<"$branches"
        echo "OK   ${artifact} ${version} may publish from main."
        return 0
    fi

    line="$(parse "$branch")"
    read -r line_artifact x y <<<"$line"
    if [[ "$artifact" != "$line_artifact" ]]; then
        refuse "release-line: ${branch} publishes ${line_artifact} only, not ${artifact} ${version}. Ship ${artifact} from main or from its own release/${artifact}-<major>.<minor> line."
    fi
    if (( 10#$major != 10#$x || 10#$minor != 10#$y )); then
        refuse "release-line: ${branch} publishes ${artifact} ${x}.${y}.z only, not ${version}. A new minor ships from main."
    fi
    echo "OK   ${artifact} ${version} may publish from ${branch}."
}

cmd_may_move_latest() {
    if [[ "$1" == "refs/heads/main" ]]; then
        echo true
    else
        echo false
    fi
}

main() {
    [[ $# -ge 1 ]] || usage
    local cmd="$1"
    shift
    case "$cmd" in
        parse)
            [[ $# -eq 1 ]] || usage
            parse "$1" || die "release-line: '$1' is not a release/<library>-<major>.<minor> branch."
            ;;
        check-ref)
            [[ $# -eq 1 ]] || usage
            cmd_check_ref "$1"
            ;;
        check)
            [[ $# -eq 3 ]] || usage
            cmd_check "$1" "$2" "$3"
            ;;
        may-move-latest)
            [[ $# -le 1 ]] || usage
            cmd_may_move_latest "${1:-}"
            ;;
        *) usage ;;
    esac
}

main "$@"
