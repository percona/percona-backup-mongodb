FROM alpine

ADD pbmctl /usr/local/bin/pbmctl

RUN adduser -h /data -S pbmctl
USER pbmctl

ENTRYPOINT ["pbmctl"]
CMD []
