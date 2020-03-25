#!/usr/bin/env bash

set -o xtrace

export MONGODB_VERSION='3.6'
docker-compose -f ./docker/docker-compose.yaml up --quiet-pull --no-color -d
sleep 25
export COMPOSE_INTERACTIVE_NO_CLI=1
docker-compose -f ./docker/docker-compose.yaml exec -T cfg01 /opt/start.sh
docker-compose -f ./docker/docker-compose.yaml exec -T rs101 /opt/start.sh
docker-compose -f ./docker/docker-compose.yaml exec -T rs201 /opt/start.sh
docker-compose -f ./docker/docker-compose.yaml exec -T mongos mongo mongodb://dba:test1234@localhost /opt/mongos_init.js

docker-compose -f ./docker/docker-compose.yaml stop \
        agent-cfg01 agent-cfg02 agent-cfg03  agent-rs101 agent-rs102 agent-rs103 agent-rs201 agent-rs202 agent-rs203
docker-compose -f ./docker/docker-compose.yaml up -d --build \
        agent-cfg01 agent-cfg02 agent-cfg03  agent-rs101 agent-rs102 agent-rs103 agent-rs201 agent-rs202 agent-rs203
