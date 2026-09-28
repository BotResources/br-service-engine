{{- define "br-service-engine-chart.networkpolicy" -}}
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
  ingress:
    {{- with .Values.networkPolicy.ingress }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
{{- end -}}
