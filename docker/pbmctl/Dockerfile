FROM alpine

RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

ADD pbmctl /usr/local/bin/pbmctl

RUN adduser -S pbmctl
USER pbmctl

ENTRYPOINT ["pbmctl"]
CMD []
