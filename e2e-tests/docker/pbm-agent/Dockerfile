FROM golang:1.19
ARG MONGODB_VERSION=4.2
ENV VERSION=${MONGODB_VERSION:-4.2}
ENV PSMDB_VERSION 5.0.14-12
RUN mkdir /opt/backups
RUN apt-get update && apt install -y --no-install-recommends lsb-release
RUN export RELEASE="bullseye"; \
	wget https://repo.percona.com/apt/percona-release_latest.${RELEASE}_all.deb && \
	dpkg -i percona-release_latest.${RELEASE}_all.deb && \
	percona-release enable psmdb-50 release && \
	apt-get update && apt-get install -y --no-install-recommends \
	libfaketime \
	iproute2 \
	libkrb5-dev \
	# percona-server-mongodb-shell-${FULL_PERCONA_VERSION} \
	vim \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /opt/pbm
COPY . .
RUN make install-tests

USER 1001
