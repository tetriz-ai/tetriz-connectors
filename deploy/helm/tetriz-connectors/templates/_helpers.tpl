{{/*
Copyright 2026 Workloom Solutions Private Limited
SPDX-License-Identifier: MIT
*/}}

{{/* The chart name, overridable with nameOverride. */}}
{{- define "tetriz.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* The release-qualified name every object is named from. */}}
{{- define "tetriz.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "tetriz.simulatorName" -}}
{{- printf "%s-simulator" (include "tetriz.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tetriz.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tetriz.labels" -}}
app.kubernetes.io/name: {{ include "tetriz.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{ include "tetriz.commonLabels" . }}
{{- end }}

{{/* The labels besides name and instance, for pod templates beside selectorLabels. */}}
{{- define "tetriz.commonLabels" -}}
helm.sh/chart: {{ include "tetriz.chart" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: tetriz-connectors
{{- end }}

{{/* Selector labels; pass a dict with "ctx" and "component". */}}
{{- define "tetriz.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tetriz.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "tetriz.image" -}}
{{- $tag := default (printf "v%s" .Chart.AppVersion) .Values.image.tag }}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}
{{- end }}

{{- define "tetriz.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "tetriz.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* The Secret holding the TETRIZ_* secret variables. */}}
{{- define "tetriz.secretName" -}}
{{- default (include "tetriz.fullname" .) .Values.secrets.existingSecret }}
{{- end }}

{{/* The vendor key of an oauthClientIds entry, as the variable spells it. */}}
{{- define "tetriz.oauthVar" -}}
{{- printf "TETRIZ_%s_OAUTH_" (. | snakecase | upper) }}
{{- end }}

{{/* Whether the runner mounts a gateway config, and from which ConfigMap. */}}
{{- define "tetriz.gatewayEnabled" -}}
{{- if or .Values.gateway.config .Values.gateway.existingConfigMap .Values.simulator.enabled }}true{{ end }}
{{- end }}

{{- define "tetriz.gatewayConfigMap" -}}
{{- default (printf "%s-gateway" (include "tetriz.fullname" .)) .Values.gateway.existingConfigMap }}
{{- end }}

{{/*
The gateway YAML the chart writes: gateway.config as given, or, with the
simulator on, deploy/gateway.demo.yaml routed to the in-cluster simulator.
*/}}
{{- define "tetriz.gatewayConfig" -}}
{{- if .Values.gateway.config }}
{{- .Values.gateway.config }}
{{- else if .Values.simulator.enabled }}
{{- $demo := .Files.Get "files/gateway.demo.yaml" }}
{{- $demo = regexReplaceAll "(?m)^SimulatorBaseURL:.*\\n?" $demo "" }}
{{- $demo = regexReplaceAll "(?m)^SimulatorOrgIDs:\\n(?:[ \\t]+- .*\\n?)*" $demo "" }}
{{- trimSuffix "\n" $demo }}
SimulatorOrgIDs:
{{- range .Values.simulator.routeOrgIDs }}
  - {{ . | quote }}
{{- end }}
SimulatorBaseURL: {{ printf "http://%s.%s.svc:%d" (include "tetriz.simulatorName" .) .Release.Namespace (int .Values.simulator.port) }}
{{- end }}
{{- end }}

{{/* Fails the render when a required setting is missing. */}}
{{- define "tetriz.validate" -}}
{{- /* An existingSecret may carry TETRIZ_REDIS_URL, which the chart cannot see. */}}
{{- if and (not .Values.config.redisAddr) (not .Values.secrets.redisUrl) (not .Values.secrets.existingSecret) }}
{{- fail "config.redisAddr is required: the host:port of your Redis or Valkey (or set secrets.redisUrl, or put TETRIZ_REDIS_URL in secrets.existingSecret)" }}
{{- end }}
{{- if not .Values.secrets.existingSecret }}
{{- if not .Values.secrets.postgresDsn }}
{{- fail "set secrets.postgresDsn, or secrets.existingSecret naming a Secret with TETRIZ_POSTGRES_DSN" }}
{{- end }}
{{- if and (not .Values.secrets.credentialsKey) (not .Values.config.allowPlaintextCredentials) }}
{{- fail "set secrets.credentialsKey (openssl rand -base64 32), or config.allowPlaintextCredentials=true for a throwaway demo" }}
{{- end }}
{{- end }}
{{- if and .Values.podDisruptionBudget.enabled .Values.podDisruptionBudget.minAvailable .Values.podDisruptionBudget.maxUnavailable }}
{{- fail "podDisruptionBudget: set minAvailable or maxUnavailable, not both" }}
{{- end }}
{{- end }}

{{/* The env the runner and the migrate init container share. */}}
{{- define "tetriz.env" -}}
{{- with .Values.config.redisAddr }}
- name: TETRIZ_REDIS_ADDR
  value: {{ . | quote }}
{{- end }}
{{- with .Values.config.namespace }}
- name: TETRIZ_NAMESPACE
  value: {{ . | quote }}
{{- end }}
- name: TETRIZ_SINK
  value: {{ .Values.config.sink | quote }}
- name: TETRIZ_BLOB_DIR
  value: {{ .Values.config.blobDir | quote }}
- name: TETRIZ_HTTP_ADDR
  value: {{ printf ":%d" (int .Values.httpPort) | quote }}
- name: TETRIZ_METRICS_PREFIX
  value: {{ .Values.config.metricsPrefix | quote }}
- name: TETRIZ_CONCURRENCY
  value: {{ .Values.config.concurrency | quote }}
- name: TETRIZ_DEFAULT_TENANT_KIND
  value: {{ .Values.config.defaultTenantKind | quote }}
- name: TETRIZ_ENV
  value: {{ .Values.config.env | quote }}
- name: TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS
  value: {{ ternary "1" "0" (.Values.config.allowPlaintextCredentials | default false) | quote }}
{{- with .Values.config.githubAppId }}
- name: TETRIZ_GITHUB_APP_ID
  value: {{ . | quote }}
{{- end }}
{{- range $vendor, $id := .Values.config.oauthClientIds }}
{{- if $id }}
- name: {{ include "tetriz.oauthVar" $vendor }}CLIENT_ID
  value: {{ $id | quote }}
{{- end }}
{{- end }}
{{- if include "tetriz.gatewayEnabled" . }}
- name: TETRIZ_GATEWAY_CONFIG
  value: {{ .Values.gateway.mountPath | quote }}
{{- end }}
{{- if .Values.tenantKinds }}
- name: TETRIZ_TENANT_KINDS_FILE
  value: /etc/tetriz/tenant-kinds/tenant-kinds
{{- end }}
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "tetriz.envFrom" -}}
- secretRef:
    name: {{ include "tetriz.secretName" . }}
{{- with .Values.extraEnvFrom }}
{{ toYaml . }}
{{- end }}
{{- end }}
