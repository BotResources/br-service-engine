#!/usr/bin/env bash
set -euo pipefail

chart_dir="charts/br-engine-service"
chart_yaml="${chart_dir}/Chart.yaml"

base_ref="${GITHUB_BASE_REF:-}"
if [ -n "$base_ref" ]; then
  base="origin/${base_ref}"
else
  base="origin/main"
fi

if ! git rev-parse --verify "$base" >/dev/null 2>&1; then
  echo "::notice::base ${base} not found, skipping chart-version check (not a PR context)"
  exit 0
fi

merge_base=$(git merge-base HEAD "$base")

changed=$(git diff --name-only "$merge_base" HEAD -- charts || true)
if [ -z "$changed" ]; then
  echo "✓ no charts/ change"
  exit 0
fi

current=$(grep -m1 '^version:' "$chart_yaml" | sed -E 's/^version:[[:space:]]*"?([^"[:space:]]+)"?.*/\1/')
if [ -z "$current" ]; then
  echo "::error file=${chart_yaml}::could not extract the chart version" >&2
  exit 1
fi

if ! git cat-file -e "${merge_base}:${chart_yaml}" 2>/dev/null; then
  echo "✓ ${chart_dir} is new since ${base} (version ${current})"
  exit 0
fi

baseline=$(git show "${merge_base}:${chart_yaml}" | grep -m1 '^version:' | sed -E 's/^version:[[:space:]]*"?([^"[:space:]]+)"?.*/\1/')

if [ "$current" = "$baseline" ]; then
  echo "::error file=${chart_yaml}::charts/ changed but Chart.yaml version is still ${current}. A chart change is a chart release — bump ${chart_yaml} version. A contract change is a chart major under a new chart name." >&2
  exit 1
fi

echo "✓ chart version bumped ${baseline} -> ${current}"
