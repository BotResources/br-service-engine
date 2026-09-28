{{- define "br-service-engine-chart.serviceaccount" -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "br-service-engine-chart.serviceAccountName" . }}
  labels:
    {{- include "br-service-engine-chart.labels" . | nindent 4 }}
{{- end -}}
