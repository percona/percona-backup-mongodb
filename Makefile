GOCACHE?=
GO_TEST_EXTRA?=
GOLANG_DOCKERHUB_TAG?=1.10-stretch
TEST_PSMDB_VERSION?=latest

AWS_ACCESS_KEY_ID?=
AWS_SECRET_ACCESS_KEY?=

all: test

test:
	GOCACHE=$(GOCACHE) go test -v $(GO_TEST_EXTRA) ./...

test-race:
	GOCACHE=$(GOCACHE) go test -v -race $(GO_TEST_EXTRA) ./...

test-replset:
	TEST_PSMDB_VERSION=$(TEST_PSMDB_VERSION) \
	docker-compose up \
	--detach \
	--force-recreate \
	--renew-anon-volumes \
	init
	test/init-replset-wait.sh

test-replset-clean:
	docker-compose down -v

test-full: test-replset
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	GOLANG_DOCKERHUB_TAG=$(GOLANG_DOCKERHUB_TAG) \
	docker-compose up \
	--build \
	--no-deps \
	--force-recreate \
	--renew-anon-volumes \
	--abort-on-container-exit \
	test

test-clean: test-replset-clean
	rm -rf out 2>/dev/null || true

clean: test-clean
