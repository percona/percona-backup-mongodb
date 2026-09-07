# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository overview

Percona Backup for MongoDB (PBM): a distributed backup/restore tool for MongoDB sharded clusters and replica sets.
It uses Go, vendored dependencies and shell for tests and build.

Two production binaries:
- `pbm` — CLI that issues commands (`cmd/pbm`)
- `pbm-agent` — daemon that runs alongside every `mongod` and executes operations (`cmd/pbm-agent`)

Plus `pbm-speed-test` and `pbm-agent-entrypoint` (used in containers).

There is **no Go API**: external callers must use the `pbm` CLI. Exported Go types/funcs are not a stable surface — see `README.md` "API" section.

## Common commands

All builds use `-mod=vendor -tags gssapi` (Kerberos auth requires `krb5-devel` / `libkrb5-dev` at build time).

```sh
make build              # build pbm, pbm-agent, pbm-speed-test → ./bin/
make build-pbm          # just the CLI
make build-agent        # just the agent
make install            # go install equivalents
make build-race         # with -race
make build-static       # CGO_ENABLED=0, statically linked
make build-cover        # with -cover (used by CI)
```

Tests:

```sh
make test                                    # runs e2e-tests/run-all (heavy; spins MongoDB)
go test ./...                                # unit tests
go test ./pbm/oplog/... -run TestX -v        # single package / single test
MONGODB_VERSION=8.0 ./e2e-tests/run-sharded  # quicker e2e subset (sharded only)
MONGODB_VERSION=8.0 ./e2e-tests/run-rs       # quicker e2e subset (replset only)
MONGODB_VERSION=8.0 ./e2e-tests/start-cluster # bring up a docker-compose dev cluster
```

`MONGODB_VERSION` selects PSMDB version (default `8.0`). The e2e harness lives in `e2e-tests/docker/` and `e2e-tests/pkg/`.

Lint: `golangci-lint run` — config in `.golangci.yml` (v2 schema, custom enabled set including `lll`, `nilnil`, `errname`, `gochecknoinits`, `nonamedreturns`).

CI is GitHub Actions (`.github/workflows/ci.yml`) but the actual functional tests live in the external `Percona-QA/psmdb-testing` repo and run via pytest against a docker-compose stack — `make test` is the local equivalent.

## Architecture

### Coordination model

PBM has no central coordinator process. Coordination is done **through MongoDB itself**: the agents on each node read/write "PBM Control collections" to elect leaders, distribute work, and report status. Locking, command queue, and op log capture all flow through these collections. When tracing a flow, expect to read code that polls or writes BSON documents rather than RPC.

### Layered packages under `pbm/`

`pbm/` holds the domain logic, organized by concern:

- `ctrl/` — the command protocol (CmdBackup, CmdRestore, CmdPITR, …) and send/recv helpers. Start here when tracing how a CLI invocation reaches an agent.
- `connect/` — MongoDB client wrapper used everywhere
- `lock/` — distributed locks via the control collection
- `topo/` — cluster topology discovery (shards, replset members, primary detection)
- `config/` — PBM configuration (storage, PITR, etc.) persisted in MongoDB
- `backup/`, `restore/`, `oplog/`, `slicer/`, `resync/` — the operations themselves
- `storage/` — pluggable backup destinations: `s3/`, `gcs/`, `azure/`, `oci/` (Oracle Cloud), `oss/` (Alibaba), `fs/`, `mio/` (MinIO), `blackhole/` (testing). All implement the `storage.Storage` interface in `storage.go`.
- `compress/`, `archive/`, `snapshot/` — data pipeline for streaming backups
- `prio/`, `defs/`, `errors/`, `log/`, `version/`, `util/` — cross-cutting

The `pbm-agent` binary wires these together; the `pbm` CLI uses `sdk/` (a thin in-process wrapper around the same packages) to inject commands.

### Errors

Use the project's `pbm/errors` package (it wraps `pkg/errors`); it also defines sentinel errors like `ErrNotFound` that callers compare against. Don't switch to stdlib `errors` ad hoc.

## The `x/` directory — pbm-x (experimental)

`x/` is a **separate Go module** (`github.com/percona/percona-backup-mongodb/x`) and is a proof-of-concept for a future major version of PBM.

- Self-contained — **no code is shared between `pbm` and `pbm-x`** in either direction. Don't add cross-imports.
- **Not vendored** — `x/` builds in normal module mode (no `vendor/`, no `-mod=vendor`, no `gssapi` tag, no Makefile target). Build/test from inside `x/`: `cd x && go build ./...` (binary lands at `x/cmd/pbmx/pbmx`, which is gitignored) and `go test ./...`. Run `go mod tidy` after touching `x/go.mod`.

**`x/` has its own `x/CLAUDE.md`** — read it before doing pbm-x work. It carries the prototype's design notes plus working preferences that override defaults here: follow the Google + Uber Go style guides, never auto-create commits/PRs, and never modify anything outside `x/` without asking first.

If a task is scoped to "pbm-x", work only inside `x/`. If it's scoped to PBM proper, ignore `x/`.

## Conventions worth knowing

- Vendored deps under `vendor/` — run `go mod vendor` after touching `go.mod`. Builds will fail otherwise (the `-mod=vendor` flag is hardcoded in the Makefile).
- The `gssapi` build tag is mandatory for the production binaries; bare `go build ./...` works for unit tests but won't reproduce production behavior for auth code.
- Indent: tabs in Go (per `.editorconfig`), 2-space in YAML/JSON, 4-space elsewhere.
- Branch naming: include the Jira ticket, e.g. `PBM-1750-pbm-x-base-setup`. Commits and PR titles start with `PBM-XXXX. …`.
- Bug tracker: Percona Jira project `PBM` (https://jira.percona.com/projects/PBM), not GitHub Issues.
