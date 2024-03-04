ARG MONGODB_VERSION=4.4
ARG MONGODB_IMAGE=percona/percona-server-mongodb

FROM ${MONGODB_IMAGE}:${MONGODB_VERSION} as mongo_image

FROM oraclelinux:9 as base-build
WORKDIR /build

RUN mkdir -p /data/db

COPY --from=mongo_image /bin/mongod /bin/
RUN dnf install epel-release && dnf update && dnf install make gcc krb5-devel iproute-tc libfaketime

RUN curl -sL -o /tmp/golang.tar.gz https://go.dev/dl/go1.22.0.linux-amd64.tar.gz && \
rm -rf /usr/local/go && tar -C /usr/local -xzf /tmp/golang.tar.gz && rm /tmp/golang.tar.gz
ENV PATH=$PATH:/usr/local/go/bin


FROM base-build
COPY . .

RUN make build-tests && cp /build/bin/* /bin/
