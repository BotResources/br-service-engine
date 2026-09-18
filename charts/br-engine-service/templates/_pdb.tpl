{{- define "br-engine-service.pdb" -}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "br-engine-service.fullname" . }}
  labels:
    {{- include "br-engine-service.labels" . | nindent 4 }}
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      {{- include "br-engine-service.selectorLabels" . | nindent 6 }}
{{- end -}}
