FROM alpine

ADD pbm-agent /usr/local/bin/pbm-agent

RUN adduser -h /data -S pbm-agent
USER pbm-agent

ENTRYPOINT ["pbm-agent"]
CMD []
