#!/usr/bin/env bash

./spinup.sh

docker-compose -f ./docker/docker-compose.yaml start tests
docker-compose -f ./docker/docker-compose.yaml logs -f --no-color tests
EXIT_CODE=$(docker-compose -f ./docker/docker-compose.yaml ps -q tests | xargs docker inspect -f '{{ .State.ExitCode }}')
docker-compose -f ./docker/docker-compose.yaml ps
docker-compose -f ./docker/docker-compose.yaml down

exit $EXIT_CODE