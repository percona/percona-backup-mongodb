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
	libfaketime \
	iproute2 \
	libkrb5-dev \
	mongodb-org \
	vim \
	make \
	gcc \
	nload \
	htop \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /opt/pbm
COPY . .
RUN make install-tests

USER 1001
