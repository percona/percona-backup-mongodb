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
	libfaketime \
	iproute \
	krb5-devel \
	vim \
	make \
	gcc \
	nload \
	htop

COPY --from=goimg /usr/local/go /usr/local/go
ENV PATH=$PATH:/usr/local/go/bin
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN mkdir /opt/backups

WORKDIR /opt/pbm
COPY . .
RUN make install-tests

USER 1001