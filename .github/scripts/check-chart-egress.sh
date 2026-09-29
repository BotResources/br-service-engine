#!/usr/bin/env bash
# Render tests for the optional `networkPolicy.egress` of the
# br-service-engine-chart library (chart 2.1). The key's PRESENCE is the
# switch, as in br-common-service: absent, the NetworkPolicy governs Ingress
# only and carries no `egress` (the 2.0 output); present, `Egress` joins
# `policyTypes` and the list renders verbatim; present but empty (or null), the
# policy governs egress and allows none through it. Ingress stays a policy type in
# every case, as in 2.0. The library's values.yaml sets no `egress`: a key
# there would read as a default the templates never apply.
#
# Run from the repository root, after `helm dependency build` of the fixture.
# Needs mikefarah yq v4 (preinstalled on the GitHub-hosted Ubuntu runners).
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

# The egress rule a deploying repository would set for an in-cluster object
# store. The names are placeholders: the library names no namespace.
egress_file="${scratch}/egress.yaml"
cat >"$egress_file" <<'EOF'
networkPolicy:
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: object-store
          podSelector:
            matchLabels:
              app.kubernetes.io/name: object-store
      ports:
        - protocol: TCP
          port: 9000
EOF

# Drops the key, as a deploying repository leaves it out.
no_ingress_file="${scratch}/no-ingress.yaml"
printf 'networkPolicy:\n  ingress: null\n' >"$no_ingress_file"

# render_policy <label> [helm args...]: sets `np` to the rendered NetworkPolicy
# as compact JSON; fails the case when the render fails or yields no single
# NetworkPolicy. It runs in this shell, never in a command substitution, so
# `error` reaches `fail`.
np=""
render_policy() {
  local label=$1 out
  shift
  np=""
  if ! out=$(helm template thin-example "$fixture" "$@" 2>&1); then
    error "${label}: the render failed: ${out}"
    return 1
  fi
  np=$(yq -o=json -I=0 'select(.kind == "NetworkPolicy")' <<<"$out")
  if [ "$(grep -c . <<<"$np")" -ne 1 ]; then
    error "${label}: expected one NetworkPolicy, got: ${np}"
    return 1
  fi
}

# field <json> <yq expression>: the expression's value as compact JSON.
field() {
  yq -p=json -o=json -I=0 "$2" <<<"$1"
}

# expect <label> <yq expression> <want>: compares one field of `np`.
expect() {
  local label=$1 expr=$2 want=$3 got
  got=$(field "$np" "$expr")
  if [ "$got" != "$want" ]; then
    error "${label}: ${expr} is ${got}, expected ${want}"
    return 1
  fi
}

want_rule='[{"ports":[{"port":9000,"protocol":"TCP"}],"to":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"object-store"}},"podSelector":{"matchLabels":{"app.kubernetes.io/name":"object-store"}}}]}]'
want_ingress='[{"from":[{"podSelector":{"matchLabels":{"app.kubernetes.io/name":"ingress"}}}],"ports":[{"port":8090,"protocol":"TCP"}]}]'

# Absent: the 2.0 policy — Ingress only, no egress key.
label="egress absent (fixture values)"
if render_policy "$label"; then
  expect "$label" '.spec.policyTypes' '["Ingress"]' &&
    expect "$label" '.spec | has("egress")' 'false' &&
    expect "$label" '.spec.ingress' "$want_ingress" &&
    echo "✓ ${label}: policyTypes [Ingress], no egress"
fi

# A null the values do not drop is a present key, as in br-common-service:
# the policy governs egress and allows none. Leaving the key out is the only
# way to leave egress ungoverned.
label="egress null (--set networkPolicy.egress=null)"
if render_policy "$label" --set networkPolicy.egress=null; then
  expect "$label" '.spec.policyTypes' '["Ingress","Egress"]' &&
    expect "$label" '.spec.egress' '[]' &&
    echo "✓ ${label}: policyTypes [Ingress, Egress], no egress allowed"
fi

# Present: Egress joins, the rules render verbatim, ingress is untouched.
label="egress to the object store, ingress set"
if render_policy "$label" -f "$egress_file"; then
  expect "$label" '.spec.policyTypes' '["Ingress","Egress"]' &&
    expect "$label" '.spec.egress' "$want_rule" &&
    expect "$label" '.spec.ingress' "$want_ingress" &&
    echo "✓ ${label}: policyTypes [Ingress, Egress], the rule verbatim"
fi

label="egress to the object store, ingress absent"
if render_policy "$label" -f "$no_ingress_file" -f "$egress_file"; then
  expect "$label" '.spec.policyTypes' '["Ingress","Egress"]' &&
    expect "$label" '.spec.ingress' 'null' &&
    expect "$label" '.spec.egress' "$want_rule" &&
    echo "✓ ${label}: policyTypes [Ingress, Egress], no ingress allowed, the rule verbatim"
fi

label="egress empty list"
if render_policy "$label" --set-json 'networkPolicy.egress=[]'; then
  expect "$label" '.spec.policyTypes' '["Ingress","Egress"]' &&
    expect "$label" '.spec.egress' '[]' &&
    echo "✓ ${label}: policyTypes [Ingress, Egress], no egress allowed"
fi

# The library's own values.yaml documents `egress` in comments only.
if helm show values "$chart" | yq -e '.networkPolicy | has("egress")' >/dev/null 2>&1; then
  error "${chart}/values.yaml sets networkPolicy.egress; its presence is the switch, so the library documents it in comments only"
else
  echo "✓ ${chart}/values.yaml sets no networkPolicy.egress"
fi

exit $fail
