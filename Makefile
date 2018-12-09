GOCACHE?=off
GOLANG_DOCKERHUB_TAG?=1.11-stretch
GO_TEST_PATH?=./...
GO_TEST_EXTRA?=
GO_TEST_COVER_PROFILE?=cover.out
GO_TEST_CODECOV?=
GO_BUILD_LDFLAGS?=-w -s

UID?=$(shell id -u)
DEST_DIR?=/usr/local/bin
UPX_BIN?=$(shell whereis -b upx 2>/dev/null | awk '{print $$(NF-0)}')

TEST_PSMDB_VERSION?=latest
TEST_MONGODB_ADMIN_USERNAME?=admin
TEST_MONGODB_ADMIN_PASSWORD?=admin123456
TEST_MONGODB_USERNAME?=test
TEST_MONGODB_PASSWORD?=123456
TEST_MONGODB_S1_RS?=rs1
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

all: pmbctl pmb-agent pmb-coordinator

$(GOPATH)/bin/dep:
	go get -ldflags="-w -s" github.com/golang/dep/cmd/dep

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
	TEST_PSMDB_VERSION=$(TEST_PSMDB_VERSION)
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
	docker-compose up \
	--detach \
	--force-recreate \
	--always-recreate-deps \
	--renew-anon-volumes \
	init
	docker/test/init-cluster-wait.sh

test-cluster-clean: env
	docker-compose down -v

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

pmb-agent: vendor cli/pmb-agent/main.go grpc/api/*.go grpc/client/*.go internal/*/*.go mdbstructs/*.go proto/*/*.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags="$(GO_BUILD_LDFLAGS)" -o pmb-agent cli/pmb-agent/main.go
	if [ -x $(UPX_BIN) ]; then upx -q pmb-agent; fi

pmbctl: vendor cli/pmbctl/main.go grpc/api/*.go grpc/client/*.go internal/*/*.go proto/*/*.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags="$(GO_BUILD_LDFLAGS)" -o pmbctl cli/pmbctl/main.go
	if [ -x $(UPX_BIN) ]; then upx -q pmbctl; fi

pmb-coordinator: vendor cli/pmb-coordinator/main.go grpc/*/*.go internal/*/*.go mdbstructs/*.go proto/*/*.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags="$(GO_BUILD_LDFLAGS)" -o pmb-coordinator cli/pmb-coordinator/main.go
	if [ -x $(UPX_BIN) ]; then upx -q pmb-coordinator; fi

install: pmbctl pmb-agent pmb-coordinator
	install pmbctl $(DEST_DIR)/pmbctl
	install pmb-agent $(DEST_DIR)/pmb-agent
	install pmb-coordinator $(DEST_DIR)/pmb-coordinator

release: vendor
	docker build -t mongodb-backup-release -f docker/Dockerfile.release .
	docker run --rm --privileged -v /var/run/docker.sock:/var/run/docker.sock -it mongodb-backup-release
	docker rmi -f mongodb-backup-release

docker-build: pmb-agent pmb-coordinator
	docker build -t mongodb-backup-agent -f docker/agent/Dockerfile .
	docker build -t mongodb-backup-coordinator -f docker/coordinator/Dockerfile .

clean:
	rm -rf pmb-agent pmbctl pmb-coordinator test-out vendor 2>/dev/null || true
