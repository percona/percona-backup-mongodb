FROM golang:1.16
RUN apt-get update && apt-get install -y --no-install-recommends \
	libfaketime \
	iproute2 \
	libkrb5-dev \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /opt/pbm
COPY . .
RUN make install-tests

USER 1001
