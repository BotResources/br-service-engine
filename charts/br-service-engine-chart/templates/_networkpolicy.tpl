{{- /*
br-service-engine-chart.networkpolicy — an additive NetworkPolicy on the
service's pods; the thin chart includes it under `networkPolicy.enabled`.

The rules are the deploying environment's topology (where the gateway, the
object store or the database run), so the library writes none and names no
namespace: it renders `networkPolicy.ingress` and `networkPolicy.egress`
verbatim, as Kubernetes rule lists.

- Ingress is always a policy type, as in 2.0: `networkPolicy.ingress` absent
  or empty allows no ingress through this policy.
- Egress (chart 2.1) is a policy type only when the `egress` key is present,
  the rule and value shape of br-common-service. Absent, the output is the 2.0
  output, byte for byte; present but empty, the policy governs egress and
  allows none through it.
*/ -}}
{{- define "br-service-engine-chart.networkpolicy" -}}
{{- $np := .Values.networkPolicy | default dict -}}
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "br-service-engine-chart.fullname" . }}
  labels:
    {{- include "br-service-engine-chart.labels" . | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      {{- include "br-service-engine-chart.selectorLabels" . | nindent 6 }}
  policyTypes:
    - Ingress
    {{- if hasKey $np "egress" }}
    - Egress
    {{- end }}
  ingress:
    {{- with $np.ingress }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- if hasKey $np "egress" }}
  egress:
    {{- $np.egress | default list | toYaml | nindent 4 }}
  {{- end }}
{{- end -}}
