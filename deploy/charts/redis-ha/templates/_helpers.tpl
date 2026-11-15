{{- define "redis-ha.name" -}}{{ .Release.Name }}{{- end -}}
{{- define "redis-ha.sentinelName" -}}{{ .Release.Name }}-sentinel{{- end -}}
{{- define "redis-ha.sentinelHeadless" -}}{{ .Release.Name }}-sentinel-headless{{- end -}}
{{- define "redis-ha.suffix" -}}{{ .Release.Namespace }}.svc.{{ .Values.clusterDomain }}{{- end -}}
{{- define "redis-ha.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end -}}
{{- define "redis-ha.labels" -}}
app.kubernetes.io/name: redis-ha
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
{{- define "redis-ha.redisSelector" -}}
app.kubernetes.io/name: redis-ha
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: redis
{{- end -}}
{{- define "redis-ha.sentinelSelector" -}}
app.kubernetes.io/name: redis-ha
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: sentinel
{{- end -}}
