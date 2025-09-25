{{/*
Expand the name of the chart.
*/}}
{{- define "stac-fastapi.name" -}}
{{- default .Chart.Name .Values.app.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "stac-fastapi.fullname" -}}
{{- if .Values.app.fullnameOverride }}
{{- .Values.app.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.app.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "stac-fastapi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "stac-fastapi.labels" -}}
helm.sh/chart: {{ include "stac-fastapi.chart" . }}
{{ include "stac-fastapi.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "stac-fastapi.selectorLabels" -}}
app.kubernetes.io/name: {{ include "stac-fastapi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "stac-fastapi.serviceAccountName" -}}
{{- if .Values.app.serviceAccount.create }}
{{- default (include "stac-fastapi.fullname" .) .Values.app.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.app.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the database host based on backend selection
*/}}
{{- define "stac-fastapi.databaseHost" -}}
{{- if .Values.externalDatabase.enabled }}
{{- .Values.externalDatabase.host }}
{{- else if eq .Values.backend "elasticsearch" }}
{{- if .Values.elasticsearch.enabled }}
{{- printf "%s-%s" .Release.Name "elasticsearch-master" }}
{{- else }}
{{- fail "Elasticsearch is not enabled but backend is set to elasticsearch" }}
{{- end }}
{{- else if eq .Values.backend "opensearch" }}
{{- if .Values.opensearch.enabled }}
{{- printf "%s-%s" .Release.Name "opensearch-cluster-master" }}
{{- else }}
{{- fail "OpenSearch is not enabled but backend is set to opensearch" }}
{{- end }}
{{- else }}
{{- fail "Invalid backend specified. Must be 'elasticsearch' or 'opensearch'" }}
{{- end }}
{{- end }}

{{/*
Create the database port based on backend selection
*/}}
{{- define "stac-fastapi.databasePort" -}}
{{- if .Values.externalDatabase.enabled }}
{{- .Values.externalDatabase.port }}
{{- else if eq .Values.backend "elasticsearch" }}
{{- .Values.elasticsearch.service.httpPort | default 9200 }}
{{- else if eq .Values.backend "opensearch" }}
{{- .Values.opensearch.service.httpPort | default 9200 }}
{{- end }}
{{- end }}

{{/*
Create the image repository with tag
*/}}
{{- define "stac-fastapi.image" -}}
{{- $registry := .Values.global.imageRegistry | default .Values.app.image.repository }}
{{- $tag := .Values.app.image.tag | default .Chart.AppVersion }}
{{- if eq .Values.backend "elasticsearch" }}
{{- printf "%s-es:%s" $registry $tag }}
{{- else if eq .Values.backend "opensearch" }}
{{- printf "%s-os:%s" $registry $tag }}
{{- end }}
{{- end }}

{{/*
Create environment variables for the application
*/}}
{{- define "stac-fastapi.environment" -}}
- name: BACKEND
  value: {{ .Values.backend | quote }}
- name: ES_HOST
  value: {{ include "stac-fastapi.databaseHost" . | quote }}
- name: ES_PORT
  value: {{ include "stac-fastapi.databasePort" . | quote }}
{{- if .Values.externalDatabase.enabled }}
- name: ES_USE_SSL
  value: {{ .Values.externalDatabase.ssl | quote }}
- name: ES_VERIFY_CERTS
  value: {{ .Values.externalDatabase.verifyCerts | quote }}
- name: ES_TIMEOUT
  value: {{ .Values.externalDatabase.timeout | quote }}
{{- if .Values.externalDatabase.apiKeySecret }}
- name: ES_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.externalDatabase.apiKeySecret }}
      key: {{ .Values.externalDatabase.apiKeySecretKey }}
{{- end }}
{{- end }}
{{- range $key, $value := .Values.app.env }}
- name: {{ $key }}
  value: {{ $value | quote }}
{{- end }}
{{- range $key, $secretName := .Values.app.envFromSecret }}
- name: {{ $key }}
  valueFrom:
    secretKeyRef:
      name: {{ $secretName }}
      key: {{ $key }}
{{- end }}
{{- end }}

{{/*
Determine if Elasticsearch should be enabled based on backend selection
*/}}
{{- define "stac-fastapi.elasticsearch.enabled" -}}
{{- if eq .Values.backend "elasticsearch" }}
{{- true }}
{{- else }}
{{- false }}
{{- end }}
{{- end }}

{{/*
Determine if OpenSearch should be enabled based on backend selection
*/}}
{{- define "stac-fastapi.opensearch.enabled" -}}
{{- if eq .Values.backend "opensearch" }}
{{- true }}
{{- else }}
{{- false }}
{{- end }}
{{- end }}