#!/usr/bin/env bash
# Frees the runner disk between the conformance battery and the example-service e2e
# in the `conformance` job.
#
# Every file under crates/conformance-service-engine/tests/*.rs is its own test binary
# (~260 of them). Once the battery has run, none of them is needed again in the job,
# but together they fill most of the runner's root disk. The example-service e2e then
# starts MinIO with its object store under /tmp, on that same disk, and MinIO answers
# every write with `507 Insufficient Storage` once the disk crosses its free-space
# floor. Deleting the battery's binaries first gives MinIO its room back.
#
# Only the battery's own test executables (and their dep-info files) are removed: the
# dependency rlibs stay, so the example-service build reuses them, and rust-cache prunes
# workspace artifacts before it saves anyway.
set -euo pipefail

target_dir="${CARGO_TARGET_DIR:-target}"
deps="${target_dir}/debug/deps"

report() {
  echo "::group::disk ${1}"
  df -h /
  if [ -d "${target_dir}" ]; then
    du -sh "${target_dir}"
  fi
  echo "::endgroup::"
}

report "before freeing the battery's test binaries"

if [ ! -d "${deps}" ]; then
  echo "::notice::${deps} does not exist, nothing to free"
  exit 0
fi

shopt -s nullglob
removed=0
for test_source in crates/conformance-service-engine/tests/*.rs; do
  name=$(basename "${test_source}" .rs)
  for artifact in "${deps}/${name}"-*; do
    rm -f -- "${artifact}"
    removed=$((removed + 1))
  done
done
echo "removed ${removed} battery test artifacts from ${deps}"

report "after freeing the battery's test binaries"
