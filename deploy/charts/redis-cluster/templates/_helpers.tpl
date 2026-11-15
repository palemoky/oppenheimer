{{- define "redis-cluster.name" -}}{{ .Release.Name }}{{- end -}}
{{- define "redis-cluster.suffix" -}}{{ .Release.Namespace }}.svc.{{ .Values.clusterDomain }}{{- end -}}
{{- define "redis-cluster.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end -}}
{{- define "redis-cluster.labels" -}}
app.kubernetes.io/name: redis-cluster
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
{{- define "redis-cluster.selector" -}}
app.kubernetes.io/name: redis-cluster
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
