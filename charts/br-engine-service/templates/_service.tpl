{{- define "br-engine-service.service" -}}
{{- $service := .Values.service | default dict -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "br-engine-service.fullname" . }}
  labels:
    {{- include "br-engine-service.labels" . | nindent 4 }}
    {{- with include "br-engine-service.extraLabels" (dict "labels" $service.labels "field" "service.labels") }}
    {{- . | nindent 4 }}
    {{- end }}
  {{- with $service.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  type: ClusterIP
  selector:
    {{- include "br-engine-service.selectorLabels" . | nindent 4 }}
  ports:
    - name: http
      port: {{ include "br-engine-service.port" . }}
      targetPort: http
      protocol: TCP
{{- end -}}
