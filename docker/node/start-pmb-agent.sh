#!/bin/bash
set -e

sleep 10

# (re)start pmb-agent
while true; do
    /usr/local/bin/pmb-agent --mongodb-host=127.0.0.1 --mongodb-port=27017
    sleep 1
done
