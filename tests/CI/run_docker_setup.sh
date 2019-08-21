#!/bin/sh
#
#    Executable script to set up DIRAC server and client instances with
#        ElasticSearch and MySQL services, all in docker containers.
#
#    The following software is required on top of Cern Centos 7 (CC7):
#      * Docker v18+
#      * Docker-Compose v2.4+
#
#    For the script to run, the shell must be logged into the CERN
#    container registry at gitlab-registry.cern.ch using
#    `docker login gitlab-registry.cern.ch` and following the prompts
#
#    Edit environment variables (settings) in the CONFIG file
#
#........................................................................

set -e

CWD=$PWD
SCRIPT_DIR="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"

if [ -z $TMP ]; then
    TMP=/tmp/DIRAC_CI_`date +"%Y%m%d%I%M%p"`
    mkdir -p $TMP
fi
if [ -z $CLIENTCONFIG ]; then
    CLIENTCONFIG=$TMP/CLIENTCONFIG
fi
if [ -z $SERVERCONFIG ]; then
    SERVERCONFIG=$TMP/SERVERCONFIG
fi

source $SCRIPT_DIR/CONFIG
source $SCRIPT_DIR/utils.sh
cd $SCRIPT_DIR

parseArguments

docker-compose -f ./docker-compose.yml up -d

echo -e "\n****" $(date -u) "Creating user and copying scripts ****"

# DIRAC server user and scripts
docker exec server adduser -s /bin/bash -d $USER_HOME $USER
docker exec client adduser -s /bin/bash -d $USER_HOME $USER

docker cp ./install_server.sh server:$WORKSPACE
docker cp ./install_client.sh client:$WORKSPACE

copyLocalSource

docker cp $SERVERCONFIG server:$WORKSPACE/CONFIG
docker cp $CLIENTCONFIG client:$WORKSPACE/CONFIG


echo -e "\n****" $(date -u) "Installing DIRAC server ****"
docker exec -u $USER \
       -w $WORKSPACE \
       server \
       bash -c "./install_server.sh > log.txt 2>&1" 

echo -e "\n****" $(date -u) "Copying user certificates and installing client ****"

# Copy generated user credentials from server to temp folder
docker cp server:$WORKSPACE/ServerInstallDIR/user/client.pem $TMP/usercert.pem
docker cp server:$WORKSPACE/ServerInstallDIR/user/client.key $TMP/userkey.pem
docker cp server:$WORKSPACE/ServerInstallDIR/etc/grid-security $TMP/

# install client
docker exec -u $USER \
       -w $WORKSPACE \
       client \
       bash -c "./install_client.sh  > log.txt 2>&1"

# copy credentials to client
docker exec client bash -c "mkdir /home/dirac/.globus"
docker cp $TMP/usercert.pem client:$USER_HOME/.globus/
docker cp $TMP/userkey.pem client:$USER_HOME/.globus/
docker cp $TMP/grid-security client:$WORKSPACE/ClientInstallDIR/etc/

# copy credentials to location required by all_integration_client_tests.sh
docker exec client bash -c "mkdir -p ${WORKSPACE}/ServerInstallDIR/user"
docker cp $TMP/usercert.pem client:$WORKSPACE/ServerInstallDIR/user/client.pem
docker cp $TMP/userkey.pem client:$WORKSPACE/ServerInstallDIR/user/client.key

docker exec client bash -c "chown -R dirac:dirac /home/dirac/"


set +e

echo -e "\n****" $(date -u) "Starting server tests ****"
docker exec -u $USER \
       -w $WORKSPACE \
       -e INSTALLROOT=$WORKSPACE \
       -e INSTALLTYPE=server \
       server \
       bash TestCode/DIRAC/tests/CI/run_tests.sh
SERVER_ERR=$?

echo -e "\n****" $(date -u) "Starting client tests ****"
docker exec -u $USER \
       -w $WORKSPACE \
       -e INSTALLROOT=$WORKSPACE \
       -e INSTALLTYPE=client \
       client \
       bash TestCode/DIRAC/tests/CI/run_tests.sh
CLIENT_ERR=$?

echo -e "\n****" $(date -u) "ALL DONE ****"


if [ $SERVER_ERR -eq 0 ] && [ $CLIENT_ERR -eq 0 ]; then
    echo "SUCCESS: All tests succeded" 
    exit 0
else
    echo "At least one unit test failed. Check the logs for more info. "
    exit 1
fi
