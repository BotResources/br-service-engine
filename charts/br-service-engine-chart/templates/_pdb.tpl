{{- define "br-service-engine-chart.pdb" -}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "br-service-engine-chart.fullname" . }}
  labels:
    {{- include "br-service-engine-chart.labels" . | nindent 4 }}
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      {{- include "br-service-engine-chart.selectorLabels" . | nindent 6 }}
{{- end -}}
