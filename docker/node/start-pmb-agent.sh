#!/bin/bash
set -e

sleep 10

# (re)start pmb-agent
while true; do
    pmb-agent \
	    --backup-dir=/data/pmb \
	    --mongodb-host=127.0.0.1 \
	    --mongodb-port=27017 \
	    --debug
    exitCode=$?
    if [ $exitCode = 0 ]; then
	    exit 0
    fi
    echo "Got exit code $exitCode from pmb-agent, restarting"
    sleep 1
done
