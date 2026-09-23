{{- /*
Pod-level helpers for the 1.1 neutral fields. A library chart's own values.yaml
lands under `.Values.br-engine-service` of the parent and never reaches the
top-level `.Values` these templates read, so every default lives HERE: an
absent key renders the default, a set key overrides it.
*/ -}}

{{- define "br-engine-service.defaultPodSecurityContext" -}}
{{- dict "runAsNonRoot" true "runAsUser" 65532 "runAsGroup" 65532 "fsGroup" 65532 "seccompProfile" (dict "type" "RuntimeDefault") | toYaml -}}
{{- end -}}

{{- define "br-engine-service.defaultContainerSecurityContext" -}}
{{- dict "allowPrivilegeEscalation" false "readOnlyRootFilesystem" true "capabilities" (dict "drop" (list "ALL")) | toYaml -}}
{{- end -}}

{{- /*
Key-by-key override of a default map: a key the thin chart sets replaces the
default key of the same name, a key set to null is removed, every other default
key stays. `mergeOverwrite` is not used on purpose: it skips zero values, so
`readOnlyRootFilesystem: false` would silently keep `true`.
*/ -}}
{{- define "br-engine-service.overrideByKey" -}}
{{- $out := .defaults | fromYaml -}}
{{- range $key, $value := (.given | default dict) }}
{{- if kindIs "invalid" $value }}
{{- $_ := unset $out $key }}
{{- else }}
{{- $_ := set $out $key $value }}
{{- end }}
{{- end }}
{{- with $out }}{{ toYaml . }}{{ end -}}
{{- end -}}

{{- define "br-engine-service.podSecurityContext" -}}
{{- include "br-engine-service.overrideByKey" (dict "defaults" (include "br-engine-service.defaultPodSecurityContext" .) "given" .Values.podSecurityContext) -}}
{{- end -}}

{{- define "br-engine-service.containerSecurityContext" -}}
{{- include "br-engine-service.overrideByKey" (dict "defaults" (include "br-engine-service.defaultContainerSecurityContext" .) "given" .Values.containerSecurityContext) -}}
{{- end -}}

{{- /*
Probe timing only. The probe path and port are ops contract v1 and belong to
the library, so a key outside the five timing fields fails the render.
Call with (dict "given" <map> "defaults" <map> "field" "<values path>").
*/ -}}
{{- define "br-engine-service.probeTiming" -}}
{{- $fields := list "initialDelaySeconds" "periodSeconds" "timeoutSeconds" "successThreshold" "failureThreshold" -}}
{{- $timing := deepCopy (.defaults | default dict) -}}
{{- range $key, $value := (.given | default dict) }}
{{- if not (has $key $fields) }}
{{- fail (printf "%s.%s is not a probe timing field (%s); the probe path and port are ops contract v1" $.field $key (join ", " $fields)) }}
{{- end }}
{{- $_ := set $timing $key $value }}
{{- end }}
{{- $lines := list }}
{{- range $fields }}
{{- if hasKey $timing . }}
{{- $lines = append $lines (printf "%s: %d" . (int (index $timing .))) }}
{{- end }}
{{- end }}
{{- join "\n" $lines -}}
{{- end -}}

{{- /*
Extra labels for the pod template or the Service. The four library labels
carry the selector and the chart identity, so a thin chart may add labels but
never replace one of them. Call with (dict "labels" <map> "field" "<values path>").
*/ -}}
{{- define "br-engine-service.extraLabels" -}}
{{- $owned := list "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/managed-by" "app.kubernetes.io/part-of" -}}
{{- range $key, $_ := (.labels | default dict) }}
{{- if has $key $owned }}
{{- fail (printf "%s must not set %s: the library owns that label" $.field $key) }}
{{- end }}
{{- end }}
{{- with .labels }}{{ toYaml . }}{{ end -}}
{{- end -}}
