# PBM tests

## Run tests
Run all tests
```
$ MONGODB_VERSION=3.6 ./run-all
```
`MONGODB_VERSION` is a PSMDB version (e.g. 3.6/4.0/4.2). Default is `3.6`

## Start test cluster
To start a tests with running pbm-agent and minio store:
```
$ MONGODB_VERSION=3.6 ./start-cluster
```
`MONGODB_VERSION` is a PSMDB version (e.g. 3.6/4.0/4.2). Default is `3.6`

You need to setup PBM though:
```
$ docker-compose -f ./docker/docker-compose.yaml exec agent-rs101 pbm config --file=/etc/pbm/minio.yaml
```
Run pbm commands:
```
$ docker-compose -f ./docker/docker-compose.yaml exec agent-rs101 pbm list
```