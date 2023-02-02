<<<<<<< HEAD
FROM golang:1.19 as goimg

FROM ubuntu:20.04
ARG MONGODB_VERSION=4.2
ENV VERSION=${MONGODB_VERSION:-6.0}
COPY --from=goimg /usr/local/go /usr/local/go
ENV PATH=$PATH:/usr/local/go/bin
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN mkdir /opt/backups
RUN apt-get update && apt install -y --no-install-recommends lsb-release gnupg wget ca-certificates
RUN wget -qO - https://www.mongodb.org/static/pgp/server-${VERSION}.asc | apt-key add - && \
	echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/$VERSION multiverse" | tee /etc/apt/sources.list.d/mongodb-org-$VERSION.list && \
	apt-get update && apt-get install -y --no-install-recommends \
=======
ARG MONGODB_VERSION=4.2
ARG MONGODB_IMAGE=percona/percona-server-mongodb

FROM golang:1.19 as goimg

FROM ${MONGODB_IMAGE}:${MONGODB_VERSION}

USER 0

RUN dnf config-manager --enable ol8_u4_security_validation; \
	dnf config-manager --enable ol8_codeready_builder; \
	dnf -y install epel-release; \
	dnf -y update; \
	dnf -y install \
>>>>>>> main
	libfaketime \
	iproute \
	krb5-devel \
	vim \
	make \
	gcc \
	nload \
<<<<<<< HEAD
	htop \
	&& rm -rf /var/lib/apt/lists/*
=======
	htop

COPY --from=goimg /usr/local/go /usr/local/go
ENV PATH=$PATH:/usr/local/go/bin
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN mkdir /opt/backups
>>>>>>> main

WORKDIR /opt/pbm
COPY . .
RUN make install-tests

USER 1001