package templates

var (
	ConnectedNodes = `          Node ID                            Cluster ID                   Node Type                   Node Name 
------------------------------------   ------------------------   --------------------------   ------------------------------		
{{range .}}
{{- .Id | printf "%-36s"}} - {{.ClusterId | printf "%-24s"}} - {{.NodeType | printf "%-26s"}} - {{.NodeName | printf "%-30s"}}
{{end}}
	`
	ConnectedNodesVerbose = `          Node ID                            Cluster ID                   Node Type                   Node Name                     Replicaset ID
------------------------------------   ------------------------   --------------------------   ------------------------------   ------------------------------------
{{range .}}
{{- .Id | printf "%-36s"}} - {{.ClusterId | printf "%-24s"}} - {{.NodeType | printf "%-26s"}} - {{.NodeName | printf "%-30s"}} - {{.ReplicasetId | printf "%-36s"}}
{{end}}`

	AvailableBackups = `        Metadata file name     -         Description
------------------------------ - ---------------------------------------------------------------------------
{{range $name, $backup := .}}
{{- $name | printf "%-30s"}} - {{$backup.Description}}
{{end}}`
)
