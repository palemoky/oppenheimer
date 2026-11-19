{{/* 每个实例一个 release，release 名就是实例名（mysql-order-0 等），所有资源名都从它来。 */}}
{{- define "mysql.name" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "mysql.selectorLabels" -}}
app.kubernetes.io/name: mysql
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "mysql.labels" -}}
{{ include "mysql.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "mysql.secretName" -}}
{{- .Release.Name }}-auth
{{- end -}}

{{/* Pod 里的脚本（files/lib.sh）靠这些变量拼出各序号的稳定名字。 */}}
{{- define "mysql.peerEnv" -}}
{{- if not (has .Values.replicaDurability (list "strict" "relaxed")) }}
{{- fail (printf "replicaDurability must be strict or relaxed, got %q" .Values.replicaDurability) }}
{{- end }}
- name: RELEASE
  value: {{ .Release.Name | quote }}
- name: POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: CLUSTER_DOMAIN
  value: {{ .Values.clusterDomain | quote }}
- name: REPLICAS
  value: {{ .Values.replicas | quote }}
- name: SERVER_ID_BASE
  value: {{ .Values.serverIdBase | quote }}
- name: REPLICA_DURABILITY
  value: {{ .Values.replicaDurability | quote }}
{{- end -}}

{{- define "mysql.secretEnv" -}}
{{- $s := include "mysql.secretName" . -}}
- name: MYSQL_ROOT_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $s }}
      key: root-password
- name: REPL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $s }}
      key: repl-password
{{- end -}}

{{/* MySQL 只放 agent：它的刷盘比 Kafka 频繁得多，不和 etcd 抢 server 的盘（设计文档 2b 的 6.2）。 */}}
{{- define "mysql.nodeAffinityRequired" -}}
requiredDuringSchedulingIgnoredDuringExecution:
  nodeSelectorTerms:
    - matchExpressions:
        - key: node-role.kubernetes.io/control-plane
          operator: DoesNotExist
{{- end -}}
