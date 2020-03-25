#!/usr/bin/env bash

set -o xtrace

MONGO_USER="dba"
BACKUP_USER="bcp"
MONGO_PASS="test1234"
CONFIGSVR=${CONFIGSVR:-"false"}

mongo <<EOF
rs.initiate(
    {
        _id: '$REPLSET_NAME',
        configsvr: $CONFIGSVR,
        version: 1,
        members: [
            { _id: 0, host: "${REPLSET_NAME}01:27017" },
            { _id: 1, host: "${REPLSET_NAME}02:27017" },
            { _id: 2, host: "${REPLSET_NAME}03:27017" }
        ]
    }
)
EOF

sleep 15

mongo <<EOF
db.getSiblingDB("admin").createUser({ user: "${MONGO_USER}", pwd: "${MONGO_PASS}", roles: [ "root", "userAdminAnyDatabase", "clusterAdmin" ] })
EOF


mongo "mongodb://${MONGO_USER}:${MONGO_PASS}@localhost/?replicaSet=${REPLSET_NAME}" <<EOF
db.getSiblingDB("admin").createRole({ "role": "pbmAnyAction",
"privileges": [
   { "resource": { "anyResource": true },
	 "actions": [ "anyAction" ]
   }
],
"roles": []
});

db.getSiblingDB("admin").createUser(
	{
		user: "${BACKUP_USER}",
		pwd: "${MONGO_PASS}",
		"roles" : [
			{ "db" : "admin", "role" : "readWrite", "collection": "" },
			{ "db" : "admin", "role" : "backup" },
			{ "db" : "admin", "role" : "clusterMonitor" },
			{ "db" : "admin", "role" : "restore" },
			{ "db" : "admin", "role" : "pbmAnyAction" }
		 ]
	}
);

EOF
