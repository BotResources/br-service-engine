{{- define "br-engine-service.name" -}}
{{- required "serviceKey is required" .Values.serviceKey -}}
{{- end -}}

{{- define "br-engine-service.fullname" -}}
{{- include "br-engine-service.name" . -}}
{{- end -}}

{{- define "br-engine-service.serviceAccountName" -}}
{{- include "br-engine-service.fullname" . -}}
{{- end -}}

{{- define "br-engine-service.port" -}}
{{- .Values.port | default 8080 -}}
{{- end -}}

{{- define "br-engine-service.selectorLabels" -}}
app.kubernetes.io/name: {{ include "br-engine-service.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "br-engine-service.labels" -}}
{{ include "br-engine-service.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: br-service-engine
{{- end -}}

{{- define "br-engine-service.trustedNetworkEnv" -}}
{{- with .Values.postgres.trustedNetworkHosts }}
- name: TRUSTED_NETWORK_HOSTS
  value: {{ join "," . | quote }}
{{- end }}
{{- end -}}

{{- define "br-engine-service.objectStoreEnv" -}}
{{- if .Values.objectStore.enabled }}
- name: S3_ENDPOINT
  value: {{ required "objectStore.endpoint is required when objectStore.enabled" .Values.objectStore.endpoint | quote }}
- name: S3_BUCKET
  value: {{ required "objectStore.bucket is required when objectStore.enabled" .Values.objectStore.bucket | quote }}
- name: S3_REGION
  value: {{ .Values.objectStore.region | default "" | quote }}
- name: S3_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ required "objectStore.secret.name is required when objectStore.enabled" .Values.objectStore.secret.name }}
      key: {{ .Values.objectStore.secret.accessKeyKey | default "S3_ACCESS_KEY" }}
- name: S3_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.objectStore.secret.name }}
      key: {{ .Values.objectStore.secret.secretKeyKey | default "S3_SECRET_KEY" }}
{{- end }}
{{- end -}}
