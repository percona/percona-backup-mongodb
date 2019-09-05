.PHONY: build-pbm build-agent build install install-pbm install-agent

GOOS?=linux

build-pbm:
	GOOS=$(GOOS) go build -o ./bin/ ./cmd/pbm
build-agent:
	GOOS=$(GOOS) go build -o ./bin/ ./cmd/pbm-agent
build: build-pbm build-agent

install-pbm:
	GOOS=$(GOOS) go install ./cmd/pbm
install-agent:
	GOOS=$(GOOS) go install ./cmd/pbm-agent
install: install-pbm install-agent
