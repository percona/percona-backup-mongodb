#!/usr/bin/env bash


mongo <<EOF
rs.initiate(
    {
        _id: '$REPLSET_NAME',
        configsvr: true,
        version: 1,
        members: [
            { _id: 0, host: "${REPLSET_NAME}01:27017" },
            { _id: 1, host: "${REPLSET_NAME}02:27017" },
            { _id: 2, host: "${REPLSET_NAME}03:27017" }
        ]
    }
)
EOF