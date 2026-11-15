{{/* release 名就是服务名。 */}}
{{- define "service.name" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "service.selectorLabels" -}}
app.kubernetes.io/name: {{ .Release.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "service.labels" -}}
{{ include "service.selectorLabels" . }}
app.kubernetes.io/part-of: oppenheimer
app.kubernetes.io/version: {{ .Values.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/* 边车开着才有边车配置；开着但没给配置时渲染失败，不要起一个空配置的 Envoy。 */}}
{{- define "service.sidecarConfig" -}}
{{- required "sidecar.config is empty: pass the rendered deploy/envoy/<service>.yaml with --set-file (scripts/cluster/services.sh does), or set sidecar.enabled=false" .Values.sidecar.config -}}
{{- end -}}
