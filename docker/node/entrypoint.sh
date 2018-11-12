#!/bin/bash
set -e

# if command starts with an option, prepend mongod
if [ "${1:0:1}" = '-' ]; then
	set -- mongod "$@"
fi

# Start pmb-agent
/start-pmb-agent.sh &

exec "$@"
