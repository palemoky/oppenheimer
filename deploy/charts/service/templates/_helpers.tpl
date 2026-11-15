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

{{/* Istio 的边车要注入：istio.inject。 */}}
{{- define "service.istioInject" -}}
{{- if .Values.istio.inject }}true{{ end -}}
{{- end -}}
