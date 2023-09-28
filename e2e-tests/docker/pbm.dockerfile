ARG MONGODB_VERSION=4.4
ARG MONGODB_IMAGE=percona/percona-server-mongodb

FROM ${MONGODB_IMAGE}:${MONGODB_VERSION} as mongo_image

FROM oraclelinux:8 as base-build
WORKDIR /build

RUN mkdir -p /data/db

COPY --from=mongo_image /bin/mongod /bin/

RUN dnf update && dnf install make golang tc

FROM base-build
COPY . .

RUN make build-tests && cp /build/bin/* /bin/
