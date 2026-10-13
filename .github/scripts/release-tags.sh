#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/tracked-libraries.sh"

upto="${1:-}"

git tag --list 'v[0-9]*' "${LIBRARY_NAME}/v[0-9]*" \
  | awk '{ v = $0; sub(/^.*\//, "", v); sub(/^v/, "", v); print v, $0 }' \
  | sort -V -k1,1 \
  | while read -r version tag; do
      if [[ -z "$upto" || "$(printf '%s\n%s\n' "$version" "$upto" | sort -V | tail -n1)" == "$upto" ]]; then
        echo "$tag"
      fi
    done
