{{- define "br-engine-service.deployment" -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "br-engine-service.fullname" . }}
  labels:
    {{- include "br-engine-service.labels" . | nindent 4 }}
  {{- with .Values.deploymentAnnotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  replicas: {{ .Values.replicaCount | default 1 }}
  strategy:
    type: Recreate
  selector:
    matchLabels:
      {{- include "br-engine-service.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "br-engine-service.labels" . | nindent 8 }}
        {{- with include "br-engine-service.extraLabels" (dict "labels" .Values.podLabels "field" "podLabels") }}
        {{- . | nindent 8 }}
        {{- end }}
      {{- with .Values.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
    spec:
      serviceAccountName: {{ include "br-engine-service.serviceAccountName" . }}
      automountServiceAccountToken: {{ .Values.automountServiceAccountToken | default false }}
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with include "br-engine-service.podSecurityContext" . }}
      securityContext:
        {{- . | nindent 8 }}
      {{- end }}
      {{- if .Values.topologySpreadEnabled }}
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              {{- include "br-engine-service.selectorLabels" . | nindent 14 }}
      {{- end }}
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.extraVolumes }}
      volumes:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      initContainers:
        - name: migrate
          image: "{{ required "image.repository is required" .Values.image.repository }}:{{ required "image.tag is required" .Values.image.tag }}"
          args: ["migrate"]
          env:
            - name: DATABASE_URL_OWNER
              valueFrom:
                secretKeyRef:
                  name: {{ required "postgres.ownerSecret.name is required" .Values.postgres.ownerSecret.name }}
                  key: {{ .Values.postgres.ownerSecret.key | default "DATABASE_URL_OWNER" }}
            - name: APP_ROLE
              value: {{ required "postgres.appRole is required" .Values.postgres.appRole | quote }}
            {{- with include "br-engine-service.trustedNetworkEnv" . | trim }}
            {{- . | nindent 12 }}
            {{- end }}
          {{- with (.Values.migrate | default dict).resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with include "br-engine-service.containerSecurityContext" . }}
          securityContext:
            {{- . | nindent 12 }}
          {{- end }}
      containers:
        - name: serve
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          args: ["serve"]
          ports:
            - name: http
              containerPort: {{ include "br-engine-service.port" . }}
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: {{ required "postgres.appSecret.name is required" .Values.postgres.appSecret.name }}
                  key: {{ .Values.postgres.appSecret.key | default "DATABASE_URL" }}
            - name: APP_ROLE
              value: {{ .Values.postgres.appRole | quote }}
            - name: NATS_URL
              value: {{ required "nats.url is required" .Values.nats.url | quote }}
            - name: ENGINE_CHANNEL
              value: {{ required "engine.channel is required" .Values.engine.channel | quote }}
            - name: HOSTNAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: PORT
              value: {{ include "br-engine-service.port" . | quote }}
            {{- with include "br-engine-service.trustedNetworkEnv" . | trim }}
            {{- . | nindent 12 }}
            {{- end }}
            {{- with include "br-engine-service.objectStoreEnv" . | trim }}
            {{- . | nindent 12 }}
            {{- end }}
            {{- with .Values.env }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          {{- with .Values.extraVolumeMounts }}
          volumeMounts:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- $probes := .Values.probes | default dict }}
          {{- /* `serve` binds its listener last (after the app pool, the migration check, NATS, Engine::boot), so /livez answering is the end of boot. /readyz can stay DOWN for long on a healthy pod, so it never gates startup. */}}
          startupProbe:
            httpGet:
              path: /livez
              port: http
            {{- include "br-engine-service.probeTiming" (dict "given" $probes.startup "defaults" (dict "periodSeconds" 5 "failureThreshold" 30) "field" "probes.startup") | nindent 12 }}
          readinessProbe:
            httpGet:
              path: /readyz
              port: http
            {{- with include "br-engine-service.probeTiming" (dict "given" $probes.readiness "field" "probes.readiness") }}
            {{- . | nindent 12 }}
            {{- end }}
          livenessProbe:
            httpGet:
              path: /livez
              port: http
            {{- with include "br-engine-service.probeTiming" (dict "given" $probes.liveness "field" "probes.liveness") }}
            {{- . | nindent 12 }}
            {{- end }}
          {{- with .Values.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with include "br-engine-service.containerSecurityContext" . }}
          securityContext:
            {{- . | nindent 12 }}
          {{- end }}
{{- end -}}
