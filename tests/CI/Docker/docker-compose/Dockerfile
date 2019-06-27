FROM docker:latest
MAINTAINER "Anton Lu" anton.lu@cern.ch

RUN apk add --update py-pip python-dev libffi-dev openssl-dev gcc libc-dev make curl bash
RUN pip install docker-compose

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["sh"]