# This dockerfile creates a container of a truly basic DIRAC installation which contains only the DIRAC externals.
# The sole purpose of this container is for developing (and testing) DIRAC "stuff that run", 
# meaning DIRAC components like agents and services. Instructions in ...


# CC7 as base (the latest tag doesn't work!)
FROM cern/cc7-base:20170113
MAINTAINER Federico Stagni <federico.stagni@cern.ch>

# Create DIRAC dirs
RUN mkdir -p /opt/dirac/DIRAC
RUN mkdir -p /opt/dirac/etc/grid-security/certificates

# Installing DIRAC in /opt/dirac
WORKDIR /opt/dirac
RUN curl -L -o dirac-install https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py && chmod +x dirac-install && ./dirac-install -r v6r17 -t server -i 27 -g 2017-01-27 
RUN rm -rf /opt/dirac/.installCache && rm dirac-install

# Copying in ENTRYPOINT script (for running DIRAC scripts directly, e.g. dirac-service)
COPY dockerEntrypoint.sh /opt/dirac/dockerEntrypoint.sh
RUN chmod 755 /opt/dirac/dockerEntrypoint.sh
ENTRYPOINT [ "/opt/dirac/dockerEntrypoint.sh" ]

# Create self-signed host certificate from auto-generated CA
WORKDIR /opt/dirac/etc/grid-security
RUN source /opt/dirac/bashrc && openssl genrsa -out hostkey.pem 2048 2&>1 /dev/null
RUN curl -L -o openssl_config https://raw.githubusercontent.com/fstagni/DIRAC/devDocsDocker-1/tests/openssl_config_DIRACDockerDevBox && source /opt/dirac/bashrc && openssl req -new -x509 -key hostkey.pem -out hostcert.pem -days 365 -config openssl_config && cp hostcert.pem certificates/ && cp hostkey.pem certificates/
#RUN cp /opt/dirac/etc/grid-security/hostcert.pem /opt/dirac/etc/grid-security/certificates/hostcert.pem 
#RUN cp /opt/dirac/etc/grid-security/hostkey.pem /opt/dirac/etc/grid-security/certificates/hostkey.pem

# Now generating the user credentials, which would need to be used for connecting to DIRAC components running here
RUN mkdir -p /opt/dirac/user
WORKDIR /opt/dirac/user
RUN cp /opt/dirac/etc/grid-security/openssl_config .
RUN sed -i 's/DIRACDockerDevBox/ciuser/g' openssl_config
RUN source /opt/dirac/bashrc && openssl genrsa -out client.key 1024 2&>1 /dev/null && openssl req -key client.key -new -out client.req -config openssl_config
# This is a little hack to make OpenSSL happy...
RUN echo 00 > file.srl 
RUN source /opt/dirac/bashrc && openssl x509 -req -in client.req -CA /opt/dirac/etc/grid-security/hostcert.pem -CAkey /opt/dirac/etc/grid-security/hostkey.pem -CAserial /opt/dirac/user/file.srl -out /opt/dirac/user/client.pem
#RUN cp /opt/dirac/etc/grid-security/hostcert.pem /opt/dirac/etc/grid-security/certificates/ && cp /opt/dirac/etc/grid-security/hostkey.pem /opt/dirac/etc/grid-security/certificates/

# Copy the script so that when logging interactively the environment is correct
RUN cp /opt/dirac/bashrc /root/.bashrc

# Just standard working dir
WORKDIR /opt/dirac/
