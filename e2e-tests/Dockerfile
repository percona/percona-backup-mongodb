FROM oraclelinux:8 AS base-build
WORKDIR /build
RUN dnf update && dnf install git make golang

FROM pbm:build-base AS pbm-build
ARG TESTS_BCP_TYPE
ENV TESTS_BCP_TYPE=${TESTS_BCP_TYPE}

COPY . .
RUN go build -o /bin/pbm-test ./e2e-tests/cmd/pbm-test

CMD ["pbm-test"]
