#!/bin/bash
#
#    Executable script to set up DIRAC server and client instances with
#        ElasticSearch and MySQL services, all in docker containers.
#
#    The following software is required on top of Cern Centos 7 (CC7):
#      * Docker v18+
#      * Docker-Compose v3.3+
#
#    For the script to run, the shell must be logged into the CERN
#    container registry at gitlab-registry.cern.ch using
#    `docker login gitlab-registry.cern.ch` and following the prompts
#
#    Edit environment variables (settings) in the CONFIG file
#
#........................................................................

set -e

SCRIPT_DIR="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"

source $SCRIPT_DIR/CONFIG
source $SCRIPT_DIR/utils.sh
mkdir  $SCRIPT_DIR/tmp || ( echo "Did you run cleanup.sh before trying again?" && exit 1 )
export CONFIGFILE=$SCRIPT_DIR/tmp/CONFIG

parseCommandLine

docker-compose -f $SCRIPT_DIR/docker-compose.yml up -d


echo -e "\n****" $(date -u) "Creating user and copying scripts ****"
USER=dirac
USER_HOME=/home/${USER}

# DIRAC server user and scripts
docker exec server adduser -s /bin/bash -d $USER_HOME $USER

if [[ ! -z $ALTERNATIVE_MODULES && -d $ALTERNATIVE_MODULES ]]; then
    echo "export ALTERNATIVE_MODULES=$(basename $ALTERNATIVE_MODULES)" > $CONFIGFILE
    docker cp $ALTERNATIVE_MODULES server:$USER_HOME/$(basename $ALTERATIVE_MODULES)
fi

docker cp $SCRIPT_DIR/install_server.sh server:$USER_HOME
docker cp $CONFIGFILE server:$USER_HOME

# DIRAC client user and scripts
docker exec client adduser -s /bin/bash -d $USER_HOME $USER

docker cp $CONFIGFILE client:$USER_HOME
docker cp $SCRIPT_DIR/install_client.sh client:$USER_HOME


echo -e "\n****" $(date -u) "Configuring MySQL admin user ****"

cat > $SCRIPT_DIR/tmp/mysql.sql <<EOF
CREATE USER 'admin'@'%' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
quit
EOF

# Wait for mysql to boot up
sleep 20
docker cp $SCRIPT_DIR/tmp/mysql.sql mysql:/mysql.sql
docker exec mysql bash -c "mysql -u root --password=password < mysql.sql"


echo -e "\n****" $(date -u) "Installing DIRAC server ****"
docker exec -u $USER \
       -w $USER_HOME \
       server \
       bash -c "./install_server.sh > log.txt 2>&1" 

echo -e "\n****" $(date -u) "Copying user certificates and installing client ****"

# Copy generated user credentials from server to temp folder
docker cp server:$USER_HOME/ServerInstallDIR/user/client.pem $SCRIPT_DIR/tmp/usercert.pem
docker cp server:$USER_HOME/ServerInstallDIR/user/client.key $SCRIPT_DIR/tmp/userkey.pem
docker cp server:$USER_HOME/ServerInstallDIR/etc/grid-security $SCRIPT_DIR/tmp/

# install client
docker exec -u $USER \
       -w $USER_HOME \
       client \
       bash -c "./install_client.sh > log.txt 2>&1"

# copy credentials to client
docker exec client bash -c "mkdir /home/dirac/.globus"
docker cp $SCRIPT_DIR/tmp/usercert.pem client:$USER_HOME/.globus/
docker cp $SCRIPT_DIR/tmp/userkey.pem client:$USER_HOME/.globus/
docker cp $SCRIPT_DIR/tmp/grid-security client:$USER_HOME/ClientInstallDIR/etc/

docker exec client bash -c "chown -R dirac:dirac /home/dirac/"

# copy tests to server and client
docker cp $SCRIPT_DIR/run_tests.sh server:$USER_HOME/
docker cp $SCRIPT_DIR/run_tests.sh client:$USER_HOME/

set +e

echo -e "\n****" $(date -u) "Starting server tests ****"
docker exec -u $USER \
       -w $USER_HOME \
       -e INSTALLROOT=$USER_HOME \
       -e AGENT=server \
       server \
       bash run_tests.sh

echo -e "\n****" $(date -u) "Starting client tests ****"
docker exec -u $USER \
       -w $USER_HOME \
       -e INSTALLROOT=$USER_HOME \
       -e AGENT=client \
       client \
       bash run_tests.sh


echo -e "\n****" $(date -u) "ALL DONE ****"
