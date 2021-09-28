#!/bin/bash

function percent_encode() {
    local length="${#1}"
    for (( i = 0; i < length; i++ )); do
        local c="${1:$i:1}"
        case $c in
            [a-zA-Z0-9]) printf '%s' "$c" ;;
            *) printf '%%%02X' "'$c" ;;
        esac
    done
}

export PBM_MONGODB_URI="mongodb://${PBM_AGENT_MONGODB_USERNAME}:$(percent_encode ${PBM_AGENT_MONGODB_PASSWORD})@localhost:${PBM_MONGODB_PORT}/?replicaSet=${PBM_MONGODB_REPLSET}"

set -o xtrace

if [ "${1:0:9}" = "pbm-agent" ]; then
	OUT="$(mktemp)"
	OUT_CFG="$(mktemp)"
	timeout=5
	for i in {1..10}; do
		if [ "${SHARDED}" ]; then
			echo "waiting for sharded scluster"

			# check in case if shard has role 'shardsrv'
			mongo "${PBM_MONGODB_URI}" --eval="db.isMaster().\$configServerState.opTime.ts" --quiet | tee "$OUT"
			exit_status=$?

			# check in case if shard has role 'configsrv'
			mongo "${PBM_MONGODB_URI}" --eval="db.isMaster().configsvr" --quiet | tail -n 1 | tee "$OUT_CFG"
			exit_status_cfg=$?

			ts=$(grep -E '^Timestamp\([0-9]+, [0-9]+\)$' "$OUT")
			isCfg=$(grep -E '^2$' "$OUT_CFG")

			if [[ "${exit_status}" == 0 && "${ts}" ]] || [[ "${exit_status_cfg}" == 0 && "${isCfg}" ]]; then
				break
			else
				sleep "$((timeout * i))"
			fi
		else
			mongo "${PBM_MONGODB_URI}" --eval="(db.isMaster().hosts).length" --quiet | tee "$OUT"
			exit_status=$?
			rs_size=$(grep -E '^([0-9]+)$' "$OUT")
			if [[ "${exit_status}" == 0 ]] && [[ $rs_size -ge 1 ]]; then
				break
			else
				sleep "$((timeout * i))"
			fi
		fi
	done

	rm "$OUT"
fi

exec "$@"
