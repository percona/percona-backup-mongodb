#!/usr/bin/env bash

set -o xtrace

sleep 25

MONGO_USER="dba"
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
