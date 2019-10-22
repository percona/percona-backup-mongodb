.PHONY: build-pbm build-agent build install install-pbm install-agent

GOOS?=linux
VERSION ?=$(shell git describe --tags --abbrev=0)
GITCOMMIT?=$(shell git rev-parse HEAD 2>/dev/null)
GITBRANCH?=$(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)
BUILDTIME?=$(shell TZ=UTC date "+%Y-%m-%d_%H:%M_UTC")

versionpath?=github.com/percona/percona-backup-mongodb/version
LDFLAGS= -X $(versionpath).version=$(VERSION) -X $(versionpath).gitCommit=$(GITCOMMIT) -X $(versionpath).gitBranch=$(GITBRANCH) -X $(versionpath).buildTime=$(BUILDTIME) -X $(versionpath).version=$(VERSION)

build-pbm:
	GOOS=$(GOOS) go build -ldflags="$(LDFLAGS)" -mod=vendor -o ./bin/pbm ./cmd/pbm
build-agent:
	GOOS=$(GOOS) go build -ldflags="$(LDFLAGS)" -mod=vendor -o ./bin/pbm-agent ./cmd/pbm-agent
build: build-pbm build-agent

install-pbm:
	GOOS=$(GOOS) go install -ldflags="$(LDFLAGS)" -mod=vendor ./cmd/pbm
install-agent:
	GOOS=$(GOOS) go install -ldflags="$(LDFLAGS)" -mod=vendor ./cmd/pbm-agent
install: install-pbm install-agent
