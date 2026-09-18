{{- define "br-engine-service.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "br-engine-service.fullname" . }}
  labels:
    {{- include "br-engine-service.labels" . | nindent 4 }}
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
