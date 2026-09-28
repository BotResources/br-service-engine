{{- define "br-service-engine-chart.name" -}}
{{- required "serviceKey is required" .Values.serviceKey -}}
{{- end -}}

{{- define "br-service-engine-chart.fullname" -}}
{{- include "br-service-engine-chart.name" . -}}
{{- end -}}

{{- define "br-service-engine-chart.serviceAccountName" -}}
{{- include "br-service-engine-chart.fullname" . -}}
{{- end -}}

{{- /*
The port NUMBER is the service's: the thin chart sets `port`, and the library
gives it no default (chart 2.0). The library owns only the port WIRING — the
named container port `http`, the `PORT` env var the engine reads, and the
Service port — so every one of them renders the one value set here. A missing
or empty `port` fails the render; so does anything but an integer from 1 to
65535 (a string, "8080" included, a fraction, 0, 65536). A YAML values file
yields a float64 and `--set` an int64, so both numeric kinds are accepted when
the value is integral.
*/ -}}
{{- define "br-service-engine-chart.port" -}}
{{- $port := .Values.port -}}
{{- if or (kindIs "invalid" $port) (and (kindIs "string" $port) (eq $port "")) -}}
{{- fail "port is required: set the service's HTTP port (an integer from 1 to 65535) in the thin chart's values; br-service-engine-chart gives it no default" -}}
{{- end -}}
{{- $numeric := list "int" "int8" "int16" "int32" "int64" "uint" "uint8" "uint16" "uint32" "uint64" "float32" "float64" -}}
{{- if kindIs "string" $port -}}
{{- fail (printf "port must be an integer from 1 to 65535, got the string %q" $port) -}}
{{- end -}}
{{- if not (has (kindOf $port) $numeric) -}}
{{- fail (printf "port must be an integer from 1 to 65535, got %v (a %s)" $port (kindOf $port)) -}}
{{- end -}}
{{- $number := float64 $port -}}
{{- if or (ne $number (floor $number)) (lt $number 1.0) (gt $number 65535.0) -}}
{{- fail (printf "port must be an integer from 1 to 65535, got %v" $port) -}}
{{- end -}}
{{- int $number -}}
{{- end -}}

{{- define "br-service-engine-chart.selectorLabels" -}}
app.kubernetes.io/name: {{ include "br-service-engine-chart.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "br-service-engine-chart.labels" -}}
{{ include "br-service-engine-chart.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: br-service-engine
{{- end -}}

{{- define "br-service-engine-chart.trustedNetworkEnv" -}}
{{- with .Values.postgres.trustedNetworkHosts }}
- name: TRUSTED_NETWORK_HOSTS
  value: {{ join "," . | quote }}
{{- end }}
{{- end -}}

{{- define "br-service-engine-chart.objectStoreEnv" -}}
{{- if (.Values.objectStore | default dict).enabled }}
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
{{- with .Values.objectStore.publicEndpoint }}
- name: S3_PUBLIC_ENDPOINT
  value: {{ . | quote }}
{{- end }}
{{- end }}
{{- end -}}
