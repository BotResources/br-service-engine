#!/usr/bin/env bash
# Render tests for the required `port` of the br-service-engine-chart library
# (chart 2.0). The library gives the port number no default: a missing port
# fails the render and names `port`; only an integer from 1 to 65535 renders;
# a valid port reaches the `http` containerPort of `serve`, the `PORT` env var
# the engine reads, and the Service port. Both numeric kinds are exercised: a
# values file yields a float64, `--set` an int64.
#
# Run from the repository root, after `helm dependency build` of the fixture.
set -euo pipefail

chart="charts/br-service-engine-chart"
fixture="${chart}/ci/thin-example"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
fail=0

error() {
  echo "::error::$*"
  fail=1
}

# A values file holding one `port:` line, so the value arrives as YAML parses it.
port_file() {
  local file
  file=$(mktemp "${scratch}/port-XXXXXX")
  printf 'port: %s\n' "$1" >"$file"
  echo "$file"
}

# expect_port <port> <label> [helm args...]: the render succeeds and the port
# reaches the containerPort, the PORT env var and the Service port.
expect_port() {
  local want=$1 label=$2 out service
  shift 2
  if ! out=$(helm template thin-example "$fixture" "$@" 2>&1); then
    error "${label}: the render failed: ${out}"
    return
  fi
  grep -qE "^ *containerPort: ${want}$" <<<"$out" ||
    error "${label}: containerPort ${want} missing from the http port of serve"
  grep -A1 -E '^ *- name: PORT$' <<<"$out" | grep -qE "^ *value: \"${want}\"$" ||
    error "${label}: env PORT is not \"${want}\""
  service=$(awk '/^---/ { kind = ""; next } /^kind: / { kind = $2 } kind == "Service"' <<<"$out")
  grep -qE "^ *port: ${want}$" <<<"$service" ||
    error "${label}: the Service port is not ${want}"
  grep -qE '^ *targetPort: http$' <<<"$service" ||
    error "${label}: the Service does not target the named port http"
  echo "✓ ${label}: port ${want} reaches containerPort, PORT and the Service"
}

# expect_refusal <message regex> <label> [helm args...]: the render fails and
# its error matches the message, which names `port`.
expect_refusal() {
  local message=$1 label=$2 out
  shift 2
  if out=$(helm template thin-example "$fixture" "$@" 2>&1); then
    error "${label}: rendered, expected a refusal matching '${message}'"
    return
  fi
  if ! grep -qE "$message" <<<"$out"; then
    error "${label}: failed without the expected message '${message}': ${out}"
    return
  fi
  echo "✓ ${label}: refused ($(grep -m1 -oE "$message.*" <<<"$out"))"
}

required='port is required'
invalid='port must be an integer from 1 to 65535'

# Accepted.
expect_port 8090 "fixture values (float64 from YAML)"
expect_port 9000 "--set port=9000 (int64)" --set port=9000
expect_port 1 "lower bound from a values file" -f "$(port_file 1)"
expect_port 65535 "upper bound with --set" --set port=65535

# Missing: no default anywhere.
expect_refusal "$required" "port removed (--set port=null)" --set port=null
expect_refusal "$required" "port null in a values file" -f "$(port_file null)"
expect_refusal "$required" "port empty (--set port=)" --set port=

# Out of range or not an integer.
expect_refusal "$invalid" "port 0 from a values file" -f "$(port_file 0)"
expect_refusal "$invalid" "port 0 with --set" --set port=0
expect_refusal "$invalid" "port 65536 from a values file" -f "$(port_file 65536)"
expect_refusal "$invalid" "port 65536 with --set" --set port=65536
expect_refusal "$invalid" "port -1 from a values file" -f "$(port_file -1)"
expect_refusal "$invalid" "port 8080.5 from a values file" -f "$(port_file 8080.5)"
expect_refusal "$invalid" "port \"http\" with --set" --set port=http
expect_refusal "$invalid" "port \"8080\" as a string" --set-string port=8080
expect_refusal "$invalid" "port true" --set port=true

# The library's own values.yaml carries no port: a default there would only be
# documentation that lies, since it lands under `.Values.br-service-engine-chart`.
if helm show values "$chart" | grep -qE '^port:'; then
  error "${chart}/values.yaml sets port; the port number is the service's, the library gives it no default"
else
  echo "✓ ${chart}/values.yaml sets no port"
fi

exit $fail
