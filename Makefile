.PHONY: build-pbm build-agent build install install-pbm install-agent test

GOOS?=linux
GOMOD?=on
CGO_ENABLED?=0
VERSION ?=$(shell git describe --tags --abbrev=0)
GITCOMMIT?=$(shell git rev-parse HEAD 2>/dev/null)
GITBRANCH?=$(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)
BUILDTIME?=$(shell TZ=UTC date "+%Y-%m-%d_%H:%M_UTC")
MONGO_TEST_VERSION?=3.6

define ENVS
	GO111MODULE=$(GOMOD) \
	GOOS=$(GOOS)
endef

define ENVS_STATIC
	$(ENVS) \
	CGO_ENABLED=$(CGO_ENABLED)
endef

versionpath?=github.com/percona/percona-backup-mongodb/version
LDFLAGS= -X $(versionpath).version=$(VERSION) -X $(versionpath).gitCommit=$(GITCOMMIT) -X $(versionpath).gitBranch=$(GITBRANCH) -X $(versionpath).buildTime=$(BUILDTIME) -X $(versionpath).version=$(VERSION)
LDFLAGS_STATIC=$(LDFLAGS) -extldflags "-static"


test:
	MONGODB_VERSION=$(MONGO_TEST_VERSION) e2e-tests/run-all

build: build-pbm build-agent
build-pbm:
	$(ENVS) go build -ldflags="$(LDFLAGS)" -mod=vendor -o ./bin/pbm ./cmd/pbm
build-agent:
	$(ENVS) go build -ldflags="$(LDFLAGS)" -mod=vendor -o ./bin/pbm-agent ./cmd/pbm-agent

install: install-pbm install-agent
install-pbm:
	$(ENVS) go install -ldflags="$(LDFLAGS)" -mod=vendor ./cmd/pbm
install-agent:
	$(ENVS) go install -ldflags="$(LDFLAGS)" -mod=vendor ./cmd/pbm-agent

# RACE DETECTOR ON
build-race: build-pbm-race build-agent-race
build-pbm-race:
	$(ENVS) go build -race -ldflags="$(LDFLAGS)" -mod=vendor -o ./bin/pbm ./cmd/pbm
build-agent-race:
	$(ENVS) go build -race -ldflags="$(LDFLAGS)" -mod=vendor -o ./bin/pbm-agent ./cmd/pbm-agent

install-race: install-pbm-race install-agent-race
install-pbm-race:
	$(ENVS) go install -race -ldflags="$(LDFLAGS)" -mod=vendor ./cmd/pbm
install-agent-race:
	$(ENVS) go install -race -ldflags="$(LDFLAGS)" -mod=vendor ./cmd/pbm-agent

# STATIC BUILDS
build-static: build-pbm-static build-agent-static
build-pbm-static:
	$(ENVS_STATIC) go build -ldflags="$(LDFLAGS_STATIC)" -mod=vendor -o ./bin/pbm ./cmd/pbm
build-agent-static:
	$(ENVS_STATIC) go build -ldflags="$(LDFLAGS_STATIC)" -mod=vendor -o ./bin/pbm-agent ./cmd/pbm-agent

install-static: install-pbm-static install-agent-static
install-pbm-static:
	$(ENVS_STATIC) go install -ldflags="$(LDFLAGS_STATIC)" -mod=vendor ./cmd/pbm
install-agent-static:
	$(ENVS_STATIC) go install -ldflags="$(LDFLAGS_STATIC)" -mod=vendor ./cmd/pbm-agent