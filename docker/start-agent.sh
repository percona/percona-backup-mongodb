#!/bin/bash

export PBM_MONGODB_URI="mongodb://${PBM_AGENT_MONGODB_USERNAME}:${PBM_AGENT_MONGODB_PASSWORD}@localhost:${PBM_MONGODB_PORT}/?replicaSet=${PBM_MONGODB_REPLSET}"

set -o xtrace

if [ "${1:0:9}" = "pbm-agent" ]; then
	OUT="$(mktemp)"
	timeout=5

	for i in {1..10}; do
		mongo "${PBM_MONGODB_URI}" --eval="(db.isMaster().hosts).length" --quiet | tee "$OUT"
		exit_status=$?
		rs_size=$(grep -E '^([0-9]+)$' "$OUT")
		if [[ "${exit_status}" == 0 ]] && [[ $rs_size -ge 1 ]]; then
			break
		else
			sleep "$((timeout * i))"
		fi
	done

	rm "$OUT"
fi

exec "$@"
