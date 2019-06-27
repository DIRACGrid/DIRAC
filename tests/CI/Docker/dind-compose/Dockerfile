FROM docker:dind
MAINTAINER "Anton Lu" anton.lu@cern.ch

RUN apk add --update py-pip python-dev libffi-dev openssl-dev gcc libc-dev make curl
RUN pip install docker-compose

VOLUME /var/lib/docker
EXPOSE 2375

ENTRYPOINT ["dockerd-entrypoint.sh"]