# MongoDB backup tool
[![codecov](https://codecov.io/gh/percona/mongodb-backup/branch/master/graph/badge.svg?token=TiuOmTfp2p)](https://codecov.io/gh/percona/mongodb-backup)

Progress:
- [x] Oplog tailer
- [ ] S3 streamer [WIP]

## Testing

Integration tests can be ran by running:
```
make test-full
```

The tests launch a test MongoDB cluster in Docker, *'docker'* and *'docker-compose'* is required.

Use the following to tear-down the test:
```
make test-full-clean
```
