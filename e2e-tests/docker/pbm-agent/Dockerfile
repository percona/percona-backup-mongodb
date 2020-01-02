FROM golang:1.12
RUN apt-get update && apt-get install -y --no-install-recommends \
		libfaketime \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /opt/pbm
COPY . .
RUN make install-race

USER nobody
