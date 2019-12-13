#!/usr/bin/env bash
# set -euo pipefail
set -eo pipefail
IFS=$'\n\t'
#........................................................................
#    Executable script to set up DIRAC server and client instances with
#        ElasticSearch and MySQL services, all in docker containers.
#
#    The following software is required on top of Cern Centos 7 (CC7):
#      * Docker v18+
#      * Docker-Compose that understands v2.4+ format
#
#    For the script to run, the shell must be logged into the CERN
#    container registry at gitlab-registry.cern.ch using
#    `docker login gitlab-registry.cern.ch` and following the prompts
#
#    Edit environment variables (settings) in the CONFIG file
#........................................................................

BUILD_DIR=$PWD/integration_test_results
mkdir -p "${BUILD_DIR}"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export HOST_OS=${HOST_OS:-cc7}

# shellcheck source=tests/CI/CONFIG
source "$SCRIPT_DIR/CONFIG"
# shellcheck source=tests/CI/utils.sh
source "$SCRIPT_DIR/utils.sh"
cd "$SCRIPT_DIR"

function prepareEnvironment() {
  if [ -z "$TMP" ]; then
      TMP=/tmp/DIRAC_CI_$(date +"%Y%m%d%I%M%p")
      mkdir -p "$TMP"
  fi
  if [ -z "$CLIENTCONFIG" ]; then
      CLIENTCONFIG=$PWD/CLIENTCONFIG
  fi
  if [ -z "$SERVERCONFIG" ]; then
      SERVERCONFIG=$PWD/SERVERCONFIG
  fi

  # GitLab variables
  cp ./CONFIG "${SERVERCONFIG}"
  if [[ -n $CI_PROJECT_DIR ]]; then
      echo "I guess we're in GitLab CI, using local repository in branch ${CI_COMMIT_REF_NAME}"
      export TESTREPO=$CI_PROJECT_DIR
      export ALTERNATIVE_MODULES=$CI_PROJECT_DIR

      # find the latest version
      if [ "${CI_COMMIT_REF_NAME}" = 'refs/heads/integration' ]; then
          export DIRACBRANCH=integration
      else
          majorVersion=$(grep "majorVersion =" "${TESTREPO}/__init__.py" | cut -d "=" -f 2)
          minorVersion=$(grep "minorVersion =" "${TESTREPO}/__init__.py" | cut -d "=" -f 2)
          export DIRACBRANCH=v${majorVersion// }r${minorVersion// }
          echo "Deduced DIRACBRANCH ${DIRACBRANCH} from __init__.py"
      fi

      {
        echo "export TESTREPO=${TESTREPO}"
        echo "export ALTERNATIVE_MODULES=${ALTERNATIVE_MODULES}"
        echo "export DIRACBRANCH=${DIRACBRANCH}"
      } >> "${SERVERCONFIG}"
  fi
  cp "${SERVERCONFIG}" "${CLIENTCONFIG}"

  docker-compose -f ./docker-compose.yml up -d

  echo -e "\n**** $(date -u) Creating user and copying scripts ****"

  # DIRAC server user and scripts
  docker exec server adduser -s /bin/bash -d "$USER_HOME" "$DOCKER_USER"
  docker exec client adduser -s /bin/bash -d "$USER_HOME" "$DOCKER_USER"

  # Create database user
  docker exec mysql mysql --password=password -e "CREATE USER '${DB_USER}'@'%' IDENTIFIED BY '${DB_PASSWORD}';"
  docker exec mysql mysql --password=password -e "CREATE USER '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';"
  docker exec mysql mysql --password=password -e "CREATE USER '${DB_USER}'@'mysql' IDENTIFIED BY '${DB_PASSWORD}';"

  docker cp ./install_server.sh server:"$WORKSPACE"
  docker cp ./install_client.sh client:"$WORKSPACE"

  copyLocalSource server "${SERVERCONFIG}"
  copyLocalSource client "${CLIENTCONFIG}"

  docker cp "$SERVERCONFIG" server:"$WORKSPACE/CONFIG"
  docker cp "$CLIENTCONFIG" client:"$WORKSPACE/CONFIG"
}

function installServer() {
  docker exec -e TERM=xterm-color -u "$DOCKER_USER" -w "$WORKSPACE" server bash ./install_server.sh 2>&1 | tee "${BUILD_DIR}/log_server_install.txt"

  echo -e "\n**** $(date -u) Copying credentials and certificates ****"
  docker exec client bash -c "mkdir -p $WORKSPACE/ServerInstallDIR/user $WORKSPACE/ClientInstallDIR/etc /home/dirac/.globus"
  docker cp server:"$WORKSPACE/ServerInstallDIR/etc/grid-security" - | docker cp - client:"$WORKSPACE/ClientInstallDIR/etc/"
  docker cp server:"$WORKSPACE/ServerInstallDIR/user/client.pem" - | docker cp - client:"$WORKSPACE/ServerInstallDIR/user/"
  docker cp server:"$WORKSPACE/ServerInstallDIR/user/client.key" - | docker cp - client:"$WORKSPACE/ServerInstallDIR/user/"
  docker exec client bash -c "cp $WORKSPACE/ServerInstallDIR/user/client.* $USER_HOME/.globus/"
  docker cp server:/tmp/x509up_u1000 - | docker cp - client:/tmp/
  docker exec client bash -c "chown -R dirac:dirac /home/dirac"
  docker exec client bash -c "chown -R dirac:dirac /tmp/x509up_u1000"
}

function installClient() {
  docker exec -e TERM=xterm-color -u "$DOCKER_USER" -w "$WORKSPACE" client bash ./install_client.sh 2>&1 | tee "${BUILD_DIR}/log_client_install.txt"
}

function testServer() {
  docker exec -e TERM=xterm-color -u "$DOCKER_USER" -w "$WORKSPACE" -e INSTALLROOT="$WORKSPACE" -e INSTALLTYPE=server server \
      bash TestCode/DIRAC/tests/CI/run_tests.sh || SERVER_CODE=$?
  echo ${SERVER_CODE:-0} > "${BUILD_DIR}/server_test_status"
}

function testClient() {
  docker exec -e TERM=xterm-color -u "$DOCKER_USER" -w "$WORKSPACE" -e INSTALLROOT="$WORKSPACE" -e INSTALLTYPE=client client \
      bash TestCode/DIRAC/tests/CI/run_tests.sh || CLIENT_CODE=$?
  echo ${CLIENT_CODE:-0} > "${BUILD_DIR}/client_test_status"
}

function checkErrors() {
  set +x
  EXIT_CODE=0

  # Server
  if [ ! -f "${BUILD_DIR}/server_test_status" ]; then
    echo "ERROR: Server integration have not been ran"
    EXIT_CODE=1
  elif [ "$(cat "${BUILD_DIR}/server_test_status")" = "0" ]; then
    echo "Server integration tests passed"
  else
    echo "ERROR: Server integration tests failed with $(cat "${BUILD_DIR}/server_test_status")"
    EXIT_CODE=1
  fi

  # Client
  if [ ! -f "${BUILD_DIR}/client_test_status" ]; then
    echo "ERROR: Client integration have not been ran"
    EXIT_CODE=1
  elif [ "$(cat "${BUILD_DIR}/client_test_status")" = "0" ]; then
    echo "Client integration tests passed"
  else
    echo "ERROR: Client integration tests failed with $(cat "${BUILD_DIR}/client_test_status")"
    EXIT_CODE=1
  fi

  exit $EXIT_CODE
}

if [ "${0}" = "${BASH_SOURCE[0]}" ]; then
  prepareEnvironment
  installServer
  installClient
  testServer
  testClient
  checkErrors
else
  echo "Skipping setup, installation and testing as ${BASH_SOURCE[0]} is being sourced"
fi
