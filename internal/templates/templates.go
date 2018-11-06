package templates

var (
	ConnectedNodes = `          Node ID                          Cluster ID              Node Type              Node Name
------------------------------------   --------------------   --------------------   ------------------------------		
{{range .}}
{{- .Id | printf "%-36s"}} - {{.ClusterId | printf "%-20s"}} - {{.NodeType | printf "%-20s"}} - {{.NodeName | printf "%-30s"}}
{{end}}
	`
	ConnectedNodesVerbose = `          Node ID                          Cluster ID              Node Type              Node Name                     Replicaset ID
------------------------------------   --------------------   --------------------   ------------------------------   ------------------------------------
{{range .}}
{{- .Id | printf "%-36s"}} - {{.ClusterId | printf "%-20s"}} - {{.NodeType | printf "%-20s"}} - {{.NodeName | printf "%-30s"}} - {{.ReplicasetId | printf "%-36s"}}
{{end}}`
)
