var muser = "dba"
var mpwd = "test1234"

db.getSiblingDB("admin").createUser({ user: muser, pwd: mpwd, roles: [ "root" ] })