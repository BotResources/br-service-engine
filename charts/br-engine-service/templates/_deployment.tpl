{{- define "br-engine-service.deployment" -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "br-engine-service.fullname" . }}
  labels:
    {{- include "br-engine-service.labels" . | nindent 4 }}
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
    spec:
      serviceAccountName: {{ include "br-engine-service.serviceAccountName" . }}
      {{- if .Values.topologySpreadEnabled }}
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              {{- include "br-engine-service.selectorLabels" . | nindent 14 }}
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
            {{- include "br-engine-service.trustedNetworkEnv" . | nindent 12 }}
            {{- with .Values.env }}
            {{- toYaml . | nindent 12 }}
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
            {{- include "br-engine-service.trustedNetworkEnv" . | nindent 12 }}
            {{- include "br-engine-service.objectStoreEnv" . | nindent 12 }}
            {{- with .Values.env }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          readinessProbe:
            httpGet:
              path: /readyz
              port: http
          livenessProbe:
            httpGet:
              path: /livez
              port: http
          {{- with .Values.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
{{- end -}}
