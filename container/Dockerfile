# This dockerfile creates a container of a truly basic DIRAC installation which contains only the DIRAC externals.
# The sole purpose of this container is for developing (and testing) DIRAC "stuff that run",
# meaning DIRAC components like agents and services. Instructions in ...

# THIS IS A WORK IN PROGRESS
# TODO:
#   verify about certificates
#   correct openssl_config URL location
#   lcgBundle now fix version
#   start mySQL + install/start ElasticSearch (?) + install/start ActiveMQ (?) or better use docker-compose
#


# CC7 as base (the latest tag doesn't work!)
FROM cern/cc7-base:20170113
MAINTAINER Federico Stagni <federico.stagni@cern.ch>

# Create DIRAC dirs
RUN mkdir -p /opt/dirac/DIRAC && \
    mkdir -p /opt/dirac/etc/grid-security/certificates && \
    mkdir -p /opt/dirac/user && \
    cd /opt/dirac

# Installing DIRAC in /opt/dirac
RUN \
    cd /opt/dirac && \
    curl -L -o dirac-install https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py && \
    chmod +x dirac-install && \
    ./dirac-install -r v6r20 -t server -i 27 -g v14r1 && \
    rm -rf /opt/dirac/.installCache && \
    rm dirac-install

# Copying in ENTRYPOINT script (for running DIRAC scripts directly, e.g. dirac-service)
COPY dockerEntrypoint.sh /opt/dirac/dockerEntrypoint.sh
RUN chmod 755 /opt/dirac/dockerEntrypoint.sh
ENTRYPOINT [ "/opt/dirac/dockerEntrypoint.sh" ]

# Create self-signed host certificate from auto-generated CA
WORKDIR /opt/dirac/etc/grid-security
RUN source /opt/dirac/bashrc && openssl genrsa -out hostkey.pem 2048
RUN curl -L -o openssl_config https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/openssl_config_DIRACDockerDevBox && \
    source /opt/dirac/bashrc && \
    openssl req -new -x509 -key hostkey.pem -out hostcert.pem -days 365 -config openssl_config && \
    cp hostcert.pem certificates/ && \
    cp hostkey.pem certificates/

# Now generating the user credentials, which would need to be used for connecting to DIRAC components running here
WORKDIR /opt/dirac/user
RUN curl -L -o openssl_config_usr https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/openssl_config_DIRACDockerDevBox_usr
RUN sed -i 's/DIRACDockerDevBox/ciuser/g' openssl_config_usr
RUN source /opt/dirac/bashrc && \
    openssl genrsa -out client.key 1024 && \
    openssl req -key client.key -new -out client.req -config openssl_config_usr
# This is a little hack to make OpenSSL happy...
RUN echo 00 > file.srl
RUN source /opt/dirac/bashrc && \
    openssl x509 -req -in client.req \
    -CA /opt/dirac/etc/grid-security/hostcert.pem \
    -CAkey /opt/dirac/etc/grid-security/hostkey.pem \
    -CAserial /opt/dirac/user/file.srl \
    -out /opt/dirac/user/client.pem

# Copy the script so that when logging interactively the environment is correct
RUN cp /opt/dirac/bashrc /root/.bashrc

# Just standard working dir
WORKDIR /opt/dirac/
