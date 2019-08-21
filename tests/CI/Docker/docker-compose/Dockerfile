#
#   Extension docker image to contain Docker-compose, curl and bash
#       to run the tests scripts in.
#
#.......................................................................


FROM docker:latest
MAINTAINER "Anton Lu" anton.lu@cern.ch

RUN apk add --update py-pip python-dev libffi-dev openssl-dev gcc libc-dev make curl bash git
RUN pip install docker-compose

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["sh"]
