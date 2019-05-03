FROM alpine

RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

ADD pbmctl /usr/local/bin/pbmctl

ENV DEBUG=1

RUN adduser -S pbmctl
USER pbmctl

ENTRYPOINT ["pbmctl"]
CMD []
