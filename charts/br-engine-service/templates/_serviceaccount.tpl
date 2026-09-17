{{- define "br-engine-service.serviceaccount" -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "br-engine-service.serviceAccountName" . }}
  labels:
    {{- include "br-engine-service.labels" . | nindent 4 }}
{{- end -}}
