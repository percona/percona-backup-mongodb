#!/bin/bash

export PBM_MONGODB_URI="mongodb://${PBM_AGENT_MONGODB_USERNAME}:${PBM_AGENT_MONGODB_PASSWORD}@localhost:${PBM_MONGODB_PORT}/?replicaSet=${PBM_MONGODB_REPLSET}"

OUT="$(mktemp)"
timeout=5

for i in {1..10}; do
    mongo ${PBM_MONGODB_URI} --eval="db.isMaster().ok" --quiet | egrep "^(0|1)$" 1>"$OUT"
    exit_status=$?
    ok=$(cat $OUT)
    if [[ ${exit_status} != 0 ]] || [ $ok != 1 ]; then
        sleep "$((timeout * i))"
    else
        break
    fi
done

rm "$OUT"

pbm-agent
