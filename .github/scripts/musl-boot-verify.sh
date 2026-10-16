#!/usr/bin/env bash
set -euo pipefail

TARGET="${MUSL_TARGET:-x86_64-unknown-linux-musl}"
MIN_PEST="2.9.3"

root="$(git rev-parse --show-toplevel)"
cd "$root"

lock_backup="$(mktemp)"
cp Cargo.lock "$lock_backup"
restore_lock() {
  cp "$lock_backup" Cargo.lock
  rm -f "$lock_backup"
}
trap restore_lock EXIT

cargo update -p pest -p pest_derive -p pest_generator -p pest_meta

pest_version="$(cargo tree -i pest --prefix none | head -n 1 | sed -E 's/^pest v([0-9.]+).*/\1/')"
echo "pest resolved to ${pest_version}"
lowest="$(printf '%s\n%s\n' "$MIN_PEST" "$pest_version" | sort -V | head -n 1)"
if [ "$lowest" != "$MIN_PEST" ]; then
  echo "pest ${pest_version} is older than ${MIN_PEST}: this job would not exercise the stack check" >&2
  exit 1
fi

target_upper="$(printf '%s' "$TARGET" | tr 'a-z-' 'A-Z_')"
target_lower="$(printf '%s' "$TARGET" | tr '-' '_')"
export "CC_${target_lower}=musl-gcc"
export "CARGO_TARGET_${target_upper}_LINKER=musl-gcc"

cargo build --locked --release --target "$TARGET" -p example-service --bin example-service

bin="$root/target/$TARGET/release/example-service"
if readelf -lW "$bin" | grep -q INTERP; then
  echo "$bin is dynamically linked: the job must exercise a static musl binary" >&2
  exit 1
fi

EXAMPLE_SERVICE_BIN="$bin" cargo test --locked -p conformance-service-engine \
  --test bb01_readiness_and_two_pods \
  --test bb02_mutation_gate_and_affordance \
  --test bb07_boot_kit
