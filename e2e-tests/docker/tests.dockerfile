FROM oraclelinux:8 AS base-build
WORKDIR /build
RUN dnf update && dnf install golang

ARG TESTS_BCP_TYPE
ENV TESTS_BCP_TYPE=${TESTS_BCP_TYPE}

COPY . .
RUN go build -o /bin/pbm-test ./e2e-tests/cmd/pbm-test

CMD ["pbm-test"]
