{{- define "br-engine-service.networkpolicy" -}}
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "br-engine-service.fullname" . }}
  labels:
    {{- include "br-engine-service.labels" . | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      {{- include "br-engine-service.selectorLabels" . | nindent 6 }}
  policyTypes:
    - Ingress
  ingress:
    {{- with .Values.networkPolicy.ingress }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
{{- end -}}
