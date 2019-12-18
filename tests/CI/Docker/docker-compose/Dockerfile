#
#   Extension docker image to contain Docker-compose, curl and bash
#       to run the tests scripts in.
#
#.......................................................................
FROM docker:latest

RUN apk add --update py-pip python-dev libffi-dev openssl-dev gcc libc-dev make curl bash git && \
    pip install docker-compose && \
    rm -rf /var/cache/apk/*

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["sh"]
