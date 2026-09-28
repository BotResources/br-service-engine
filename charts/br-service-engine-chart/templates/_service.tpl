{{- define "br-service-engine-chart.service" -}}
{{- $service := .Values.service | default dict -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "br-service-engine-chart.fullname" . }}
  labels:
    {{- include "br-service-engine-chart.labels" . | nindent 4 }}
    {{- with include "br-service-engine-chart.extraLabels" (dict "labels" $service.labels "field" "service.labels") }}
    {{- . | nindent 4 }}
    {{- end }}
  {{- with $service.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  type: ClusterIP
  selector:
    {{- include "br-service-engine-chart.selectorLabels" . | nindent 4 }}
  ports:
    - name: http
      port: {{ include "br-service-engine-chart.port" . }}
      targetPort: http
      protocol: TCP
{{- end -}}
