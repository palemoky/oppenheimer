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

{{/* mesh 只能是 envoy 或 istio，写错了渲染失败，不要悄悄落到某一种形态。 */}}
{{- define "service.istio" -}}
{{- if not (has .Values.mesh (list "envoy" "istio")) -}}
{{- fail (printf "mesh must be envoy or istio, got %v" .Values.mesh) -}}
{{- end -}}
{{- if eq .Values.mesh "istio" }}true{{ end -}}
{{- end -}}

{{/* 手写的 Envoy 边车开着：mesh: envoy 并且 sidecar.enabled。mesh: istio 时手写边车的东西一概不渲染。 */}}
{{- define "service.envoySidecar" -}}
{{- if and (not (include "service.istio" .)) .Values.sidecar.enabled }}true{{ end -}}
{{- end -}}

{{/* Istio 的边车要注入：mesh: istio 并且 istio.inject。 */}}
{{- define "service.istioInject" -}}
{{- if and (include "service.istio" .) .Values.istio.inject }}true{{ end -}}
{{- end -}}
