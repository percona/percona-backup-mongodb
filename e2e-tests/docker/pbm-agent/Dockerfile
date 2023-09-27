ARG MONGODB_VERSION=4.4
ARG MONGODB_IMAGE=percona/percona-server-mongodb

FROM ${MONGODB_IMAGE}:${MONGODB_VERSION} as mongo_image

FROM oraclelinux:8 AS base-build
WORKDIR /build
RUN dnf update && dnf install git make golang

FROM pbm:build-base AS pbm-build
COPY . .
RUN make build-tests

FROM oraclelinux:8
ARG MONGODB_VERSION=4.4
ARG MONGODB_IMAGE=percona/percona-server-mongodb

RUN mkdir -p /data/db
COPY --from=mongo_image /bin/mongod /bin/
COPY --from=pbm-build /build/bin/pbm* /bin/
