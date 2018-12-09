FROM alpine

ADD pmb-agent /usr/local/bin/pmb-agent

RUN adduser -h /data -S pmb-agent
USER pmb-agent

ENTRYPOINT ["pmb-agent"]
CMD []
