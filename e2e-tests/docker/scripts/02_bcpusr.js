var mbcpuser = "backupUser"
var mpwd = "test1234"

db.getSiblingDB("admin").createUser({ user: mbcpuser, pwd: mpwd, roles: [ { db: "admin", role: "root" }, { db: "admin", role: "backup" }, { db: "admin", role: "clusterMonitor" }, { db: "admin", role: "restore" } ] })