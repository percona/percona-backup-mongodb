GOCACHE?=off
GOLANG_DOCKERHUB_TAG?=1.11-stretch
GO_TEST_PATH?=./...
GO_TEST_EXTRA?=
GO_TEST_COVER_PROFILE?=cover.out
GO_TEST_CODECOV?=

VERSION ?=$(shell git describe --abbrev=0)
BUILD ?=$(shell date +%FT%T%z)
GOVERSION ?=$(shell go version | cut -d " " -f3)
COMMIT ?=$(shell git rev-parse HEAD)
BRANCH ?=$(shell git rev-parse --abbrev-ref HEAD)

GO_BUILD_LDFLAGS=-X main.Version=${VERSION} -X main.Build=${BUILD} -X main.Commit=${COMMIT} -X main.Branch=${BRANCH} -X main.GoVersion=${GOVERSION} -s -w
GOSEC_VERSION?=1.2.0

NAME?=percona-backup-mongodb
REPO?=percona/$(NAME)
GORELEASER_FLAGS?=

UID?=$(shell id -u)
DEST_DIR?=/usr/local/bin

TEST_PSMDB_VERSION?=3.6
TEST_MONGODB_FLAVOR?=percona/percona-server-mongodb
TEST_MONGODB_ADMIN_USERNAME?=admin
TEST_MONGODB_ADMIN_PASSWORD?=admin123456
TEST_MONGODB_USERNAME?=test
TEST_MONGODB_PASSWORD?=123456
TEST_MONGODB_S1_RS?=rs1
TEST_MONGODB_STANDALONE_PORT?=27017
TEST_MONGODB_S1_PRIMARY_PORT?=17001
TEST_MONGODB_S1_SECONDARY1_PORT?=17002
TEST_MONGODB_S1_SECONDARY2_PORT?=17003
TEST_MONGODB_S2_RS?=rs2
TEST_MONGODB_S2_PRIMARY_PORT?=17004
TEST_MONGODB_S2_SECONDARY1_PORT?=17005
TEST_MONGODB_S2_SECONDARY2_PORT?=17006
TEST_MONGODB_CONFIGSVR_RS?=csReplSet
TEST_MONGODB_CONFIGSVR1_PORT?=17007
TEST_MONGODB_CONFIGSVR2_PORT?=17008
TEST_MONGODB_CONFIGSVR3_PORT?=17009
TEST_MONGODB_MONGOS_PORT?=17000

AWS_ACCESS_KEY_ID?=
AWS_SECRET_ACCESS_KEY?=

MINIO_PORT=9000
MINIO_ACCESS_KEY_ID=example00000
MINIO_SECRET_ACCESS_KEY=secret00000
export MINIO_ACCESS_KEY_ID
export MINIO_SECRET_ACCESS_KEY

all: pbmctl pbm-agent pbm-coordinator

$(GOPATH)/bin/dep:
	go get -ldflags="-w -s" github.com/golang/dep/cmd/dep

$(GOPATH)/bin/gosec:
	curl -sfL https://raw.githubusercontent.com/securego/gosec/master/install.sh | sh -s -- -b $(GOPATH)/bin $(GOSEC_VERSION)

vendor: $(GOPATH)/bin/dep Gopkg.lock Gopkg.toml
	$(GOPATH)/bin/dep ensure

define TEST_ENV
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	GOCACHE=$(GOCACHE) \
	GOLANG_DOCKERHUB_TAG=$(GOLANG_DOCKERHUB_TAG) \
	TEST_MONGODB_ADMIN_USERNAME=$(TEST_MONGODB_ADMIN_USERNAME) \
	TEST_MONGODB_ADMIN_PASSWORD=$(TEST_MONGODB_ADMIN_PASSWORD) \
	TEST_MONGODB_USERNAME=$(TEST_MONGODB_USERNAME) \
	TEST_MONGODB_PASSWORD=$(TEST_MONGODB_PASSWORD) \
	TEST_MONGODB_S1_RS=$(TEST_MONGODB_S1_RS) \
	TEST_MONGODB_STANDALONE_PORT=$(TEST_MONGODB_STANDALONE_PORT) \
	TEST_MONGODB_S1_PRIMARY_PORT=$(TEST_MONGODB_S1_PRIMARY_PORT) \
	TEST_MONGODB_S1_SECONDARY1_PORT=$(TEST_MONGODB_S1_SECONDARY1_PORT) \
	TEST_MONGODB_S1_SECONDARY2_PORT=$(TEST_MONGODB_S1_SECONDARY2_PORT) \
	TEST_MONGODB_S2_RS=$(TEST_MONGODB_S2_RS) \
	TEST_MONGODB_S2_PRIMARY_PORT=$(TEST_MONGODB_S2_PRIMARY_PORT) \
	TEST_MONGODB_S2_SECONDARY1_PORT=$(TEST_MONGODB_S2_SECONDARY1_PORT) \
	TEST_MONGODB_S2_SECONDARY2_PORT=$(TEST_MONGODB_S2_SECONDARY2_PORT) \
	TEST_MONGODB_CONFIGSVR_RS=$(TEST_MONGODB_CONFIGSVR_RS) \
	TEST_MONGODB_CONFIGSVR1_PORT=$(TEST_MONGODB_CONFIGSVR1_PORT) \
	TEST_MONGODB_CONFIGSVR2_PORT=$(TEST_MONGODB_CONFIGSVR2_PORT) \
	TEST_MONGODB_CONFIGSVR3_PORT=$(TEST_MONGODB_CONFIGSVR3_PORT) \
	TEST_MONGODB_MONGOS_PORT=$(TEST_MONGODB_MONGOS_PORT) \
	TEST_PSMDB_VERSION=$(TEST_PSMDB_VERSION) \
	TEST_MONGODB_FLAVOR=$(TEST_MONGODB_FLAVOR)
endef

env:
	@echo $(TEST_ENV) | tr ' ' '\n' >.env

test-race: env vendor
ifeq ($(GO_TEST_CODECOV), true)
	$(shell cat .env) \
	go test -v -race -coverprofile=$(GO_TEST_COVER_PROFILE) -covermode=atomic $(GO_TEST_EXTRA) $(GO_TEST_PATH)
else
	$(shell cat .env) \
	go test -v -race -covermode=atomic $(GO_TEST_EXTRA) $(GO_TEST_PATH)
endif
	
test: env vendor
	$(shell cat .env) \
	go test -covermode=atomic -count 1 -race -timeout 2m $(GO_TEST_EXTRA) $(GO_TEST_PATH)

test-cluster: env
	TEST_PSMDB_VERSION=$(TEST_PSMDB_VERSION) \
	docker-compose up \
	--detach \
	--force-recreate \
	--always-recreate-deps \
	--renew-anon-volumes \
	init 
	docker/test/init-cluster-wait.sh

test-cluster-clean: env
	docker-compose down -v

test-gosec: $(GOPATH)/bin/gosec
	$(GOPATH)/bin/gosec --exclude=G104 $(shell go list ./... | egrep -v "(mocks|proto|test(util)?s|vendor)")

test-full: env test-cluster-clean test-cluster
	docker-compose up \
	--build \
	--no-deps \
	--force-recreate \
	--renew-anon-volumes \
	--abort-on-container-exit \
	test

test-clean: test-cluster-clean
	rm -rf test-out 2>/dev/null || true

test-data: env test-cluster-clean test-cluster
	go get -u "github.com/feliixx/mgodatagen"
	mgodatagen --host 127.0.0.1 --port ${TEST_MONGODB_S1_PRIMARY_PORT} --username ${TEST_MONGODB_ADMIN_USERNAME} --password ${TEST_MONGODB_ADMIN_PASSWORD} -f testdata/big.json
	mgodatagen --host 127.0.0.1 --port ${TEST_MONGODB_S2_PRIMARY_PORT} --username ${TEST_MONGODB_ADMIN_USERNAME} --password ${TEST_MONGODB_ADMIN_PASSWORD} -f testdata/big.json

pbm-agent: vendor cli/pbm-agent/main.go grpc/api/*.go grpc/client/*.go internal/*/*.go mdbstructs/*.go proto/*/*.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags="$(GO_BUILD_LDFLAGS)" -o pbm-agent cli/pbm-agent/main.go

pbmctl: vendor cli/pbmctl/main.go grpc/api/*.go grpc/client/*.go internal/*/*.go proto/*/*.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags="$(GO_BUILD_LDFLAGS)" -o pbmctl cli/pbmctl/main.go

pbm-coordinator: vendor cli/pbm-coordinator/main.go grpc/*/*.go internal/*/*.go mdbstructs/*.go proto/*/*.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags="$(GO_BUILD_LDFLAGS)" -o pbm-coordinator cli/pbm-coordinator/main.go

build-all: vendor
	CGO_ENABLED=0 GOOS=linux go build -ldflags="$(GO_BUILD_LDFLAGS)" -o pbm-coordinator cli/pbm-coordinator/main.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags="$(GO_BUILD_LDFLAGS)" -o pbmctl cli/pbmctl/main.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags="$(GO_BUILD_LDFLAGS)" -o pbm-agent cli/pbm-agent/main.go

install: pbmctl pbm-agent pbm-coordinator
	install pbmctl $(DEST_DIR)/pbmctl
	install pbm-agent $(DEST_DIR)/pbm-agent
	install pbm-coordinator $(DEST_DIR)/pbm-coordinator

release: vendor
	docker build -t $(NAME)-release -f docker/Dockerfile.release .
	docker run --rm --privileged \
	-e GITHUB_TOKEN=$(GITHUB_TOKEN) \
	-e DOCKER_USERNAME=$(DOCKER_USERNAME) \
	-e DOCKER_PASSWORD=$(DOCKER_PASSWORD) \
	-v /var/run/docker.sock:/var/run/docker.sock \
	-v $(CURDIR)/dist:/go/src/github.com/percona/percona-backup-mongodb/dist/ \
	-it $(NAME)-release $(GORELEASER_FLAGS)
	docker rmi -f $(NAME)-release

docker-build: pbmctl pbm-agent pbm-coordinator
	docker build -t $(REPO):latest -f docker/Dockerfile.common .

clean:
	rm -rf vendor 2>/dev/null || true
	rm -f pbm-agent pbmctl pbm-coordinator test-out *.000 2>/dev/null || true
