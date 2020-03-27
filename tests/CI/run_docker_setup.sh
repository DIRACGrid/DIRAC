#!/usr/bin/env bash
# set -euo pipefail
set -eo pipefail
IFS=$'\n\t'
set -x

BUILD_DIR=$PWD/integration_test_results
mkdir -p "${BUILD_DIR}"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DIRAC_BASE_DIR=$(realpath "${SCRIPT_DIR}/../..")

export CI_REGISTRY_IMAGE=${CI_REGISTRY_IMAGE:-diracgrid}
export HOST_OS=${HOST_OS:-cc7}
export MYSQL_VER=${MYSQL_VER:-5.7}
export ES_VER=${ES_VER:-6.6.0}

export DOCKER_USER=dirac
export USER_HOME=/home/${DOCKER_USER}
export WORKSPACE=$USER_HOME

function copyLocalSource() {
  # Copies local source and test code to docker containers, if they are directories
  CONTAINER_NAME=$1
  CONFIG_PATH=$2

  # Drop into a subshell to avoid poluting the environment
  (
    source "$CONFIG_PATH"

    docker exec "${CONTAINER_NAME}" mkdir -p "$WORKSPACE/LocalRepo/TestCode"
    for repo_path in "${TESTREPO[@]}"; do
      if [ -n "${repo_path}" ] && [ -d "${repo_path}" ]; then
        docker cp "${repo_path}" "${CONTAINER_NAME}:$WORKSPACE/LocalRepo/TestCode/$(basename "${repo_path}")"
        sed -i "s@\(TESTREPO+=..\)$(dirname "${repo_path}")\(/$(basename "${repo_path}")..\)@\1${WORKSPACE}/LocalRepo/TestCode\2@" "$CONFIG_PATH"
      fi
    done

    docker exec "${CONTAINER_NAME}" mkdir -p "$WORKSPACE/LocalRepo/ALTERNATIVE_MODULES"
    for module_path in "${ALTERNATIVE_MODULES[@]}"; do
      if [ -n "${module_path}" ] && [ -d "${module_path}" ]; then
        docker cp "${module_path}" "${CONTAINER_NAME}:$WORKSPACE/LocalRepo/ALTERNATIVE_MODULES/$(basename "${module_path}")"
        sed -i "s@\(ALTERNATIVE_MODULES+=..\)$(dirname "${module_path}")\(/$(basename "${module_path}")..\)@\1${WORKSPACE}/LocalRepo/ALTERNATIVE_MODULES\2@" "$CONFIG_PATH"
      fi
    done
  )
}
cd "$SCRIPT_DIR"

function prepareEnvironment() {
  if [ -z "$TMP" ]; then
      TMP=/tmp/DIRAC_CI_$(date +"%Y%m%d%I%M%p")
      mkdir -p "$TMP"
  fi
  if [ -z "$CLIENTCONFIG" ]; then
      CLIENTCONFIG=${BUILD_DIR}/CLIENTCONFIG
  fi
  if [ -z "$SERVERCONFIG" ]; then
      SERVERCONFIG=${BUILD_DIR}/SERVERCONFIG
  fi

  # GitLab variables
  {
    echo "#!/usr/bin/env bash"
    echo "# This file contains all the environment variables necessary to run a full integration test"
    echo ""
    echo "export DEBUG=${DEBUG:-True}"
    echo ""
    echo "# Settings for external services"
    echo ""
    echo "# MYSQL Settings"
    echo "export DB_USER=${DB_USER:-Dirac}"
    echo "export DB_PASSWORD=${DB_PASSWORD:-Dirac}"
    echo "export DB_ROOTUSER=${DB_ROOTUSER:-root}"
    echo "export DB_ROOTPWD=${DB_ROOTPWD:-password}"
    echo "export DB_HOST=${DB_HOST:-mysql}"
    echo "export DB_PORT=${DB_PORT:-3306}"
    echo ""
    echo "# ElasticSearch settings"
    echo "export NoSQLDB_HOST=${NoSQLDB_HOST:-elasticsearch}"
    echo "export NoSQLDB_PORT=${NoSQLDB_PORT:-9200}"
    echo ""
    echo "# Hostnames"
    echo "export SERVER_HOST=${SERVER_HOST:-server}"
    echo "export CLIENT_HOST=${CLIENT_HOST:-client}"
    echo ""
    echo "# Settings for DIRAC installation"
    echo "export PRERELEASE=${PRERELEASE:-}"
    echo "export DIRAC_RELEASE=${DIRAC_RELEASE:-}"
    echo "export DIRACBRANCH=${DIRACBRANCH:-}"
    echo ""
    echo "# repository to get tests and install scripts from"
    echo "export TESTBRANCH=${TESTBRANCH:-ci}"
    echo "export DIRAC_CI_SETUP_SCRIPT=${WORKSPACE}/TestCode/${DIRAC_CI_SETUP_SCRIPT:-DIRAC/tests/Jenkins/dirac_ci.sh}"
    echo ""
    echo "export DIRACOSVER=${DIRACOSVER:-master}"
    echo "export DIRACOS_TARBALL_PATH=${DIRACOS_TARBALL_PATH:-}"
    echo ""
    echo "# Test specific variables"
    echo "export WORKSPACE=${WORKSPACE}"
    echo ""
    echo "# Optional parameters"

    echo "declare -a TESTREPO"
    if [ -n "${TESTREPO+x}" ]; then
      for repo_path in "${TESTREPO[@]}"; do
        echo "TESTREPO+=(\"${repo_path}\")"
      done
    else
      echo "TESTREPO+=(\"${DIRAC_BASE_DIR}\")"
    fi

    echo "declare -a ALTERNATIVE_MODULES"
    if [ -n "${ALTERNATIVE_MODULES+x}" ]; then
      for module_path in "${ALTERNATIVE_MODULES[@]}"; do
        echo "ALTERNATIVE_MODULES+=(\"${module_path}\")"
      done
    else
      echo "ALTERNATIVE_MODULES+=(\"${DIRAC_BASE_DIR}\")"
    fi

    echo "declare -a INSTALLOPTIONS"
    if [ -n "${INSTALLOPTIONS+x}" ]; then
      for option in "${INSTALLOPTIONS[@]}"; do
        echo "INSTALLOPTIONS+=(\"${option}\")"
      done
    fi
  } > "${SERVERCONFIG}"

  # find the latest version, unless it's integration
  if [ "${CI_COMMIT_REF_NAME}" = 'refs/heads/integration' ]; then
      {
        echo "export DIRAC_RELEASE=integration"
      } >> "${SERVERCONFIG}"

  elif [ "${CI_MERGE_REQUEST_TARGET_BRANCH_NAME}" = 'integration' ]; then
      {
        echo "export DIRAC_RELEASE=integration"
      } >> "${SERVERCONFIG}"
  else
      majorVersion=$(grep "majorVersion =" "${DIRAC_BASE_DIR}/__init__.py" | cut -d "=" -f 2)
      minorVersion=$(grep "minorVersion =" "${DIRAC_BASE_DIR}/__init__.py" | cut -d "=" -f 2)
      {
        echo "export DIRACBRANCH=${DIRACBRANCH:-v${majorVersion// }r${minorVersion// }}"
      } >> "${SERVERCONFIG}"
  fi

  if [ -n "${EXTRA_ENVIRONMENT_CONFIG+x}" ]; then
    for line in "${EXTRA_ENVIRONMENT_CONFIG[@]}"; do
      echo "${line}" >> "${SERVERCONFIG}"
    done
  fi

  echo "Generated config file is:"
  cat "${SERVERCONFIG}"
  cp "${SERVERCONFIG}" "${CLIENTCONFIG}"

  if [ -n "${SERVER_USE_M2CRYPTO+x}" ]; then
    echo "export DIRAC_USE_M2CRYPTO=${SERVER_USE_M2CRYPTO}" >> "${SERVERCONFIG}"
  fi

  if [ -n "${CLIENT_USE_M2CRYPTO+x}" ]; then
    echo "export DIRAC_USE_M2CRYPTO=${CLIENT_USE_M2CRYPTO}" >> "${CLIENTCONFIG}"
  fi

  docker-compose -f ./docker-compose.yml up -d

  echo -e "\n**** $(date -u) Creating user and copying scripts ****"

  # DIRAC server user and scripts
  docker exec server adduser -s /bin/bash -d "$USER_HOME" "$DOCKER_USER"
  docker exec client adduser -s /bin/bash -d "$USER_HOME" "$DOCKER_USER"

  # Create database user
  # Run in a subshell so we can safely source ${SERVERCONFIG}
  (
    source "${SERVERCONFIG}"
    docker exec mysql mysql --password=password -e "CREATE USER '${DB_USER}'@'%' IDENTIFIED BY '${DB_PASSWORD}';"
    docker exec mysql mysql --password=password -e "CREATE USER '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';"
    docker exec mysql mysql --password=password -e "CREATE USER '${DB_USER}'@'mysql' IDENTIFIED BY '${DB_PASSWORD}';"
  )

  docker cp ./install_server.sh server:"$WORKSPACE"
  docker cp ./install_client.sh client:"$WORKSPACE"

  copyLocalSource server "${SERVERCONFIG}"
  copyLocalSource client "${CLIENTCONFIG}"

  docker cp "$SERVERCONFIG" server:"$WORKSPACE/CONFIG"
  docker cp "$CLIENTCONFIG" client:"$WORKSPACE/CONFIG"

  # if the DIRACOS_TARBALL_PATH is a local directory, copy it over
  if [ -d "${DIRACOS_TARBALL_PATH}" ]; then
    docker cp ${DIRACOS_TARBALL_PATH} server:${DIRACOS_TARBALL_PATH}
    docker cp ${DIRACOS_TARBALL_PATH} client:${DIRACOS_TARBALL_PATH}
  fi
    
}

function installServer() {
  docker exec -e TERM=xterm-color -u "$DOCKER_USER" -w "$WORKSPACE" server bash ./install_server.sh 2>&1 | tee "${BUILD_DIR}/log_server_install.txt"

  echo -e "\n**** $(date -u) Copying credentials and certificates ****"
  docker exec client bash -c "mkdir -p $WORKSPACE/ServerInstallDIR/user $WORKSPACE/ClientInstallDIR/etc /home/dirac/.globus"
  docker cp server:"$WORKSPACE/ServerInstallDIR/etc/grid-security" - | docker cp - client:"$WORKSPACE/ClientInstallDIR/etc/"
  docker cp server:"$WORKSPACE/ServerInstallDIR/user/client.pem" - | docker cp - client:"$WORKSPACE/ServerInstallDIR/user/"
  docker cp server:"$WORKSPACE/ServerInstallDIR/user/client.key" - | docker cp - client:"$WORKSPACE/ServerInstallDIR/user/"
  docker exec client bash -c "cp $WORKSPACE/ServerInstallDIR/user/client.* $USER_HOME/.globus/"
  server_uid=$(docker exec -u dirac server bash -c 'echo $UID')
  client_uid=$(docker exec -u dirac client bash -c 'echo $UID')
  docker cp "server:/tmp/x509up_u${server_uid}" - | docker cp - client:/tmp/
  docker exec client bash -c "chown -R dirac:dirac /home/dirac"
  docker exec client bash -c "chown -R dirac:dirac /tmp/x509up_u${client_uid}"
}

function installClient() {
  docker exec -e TERM=xterm-color -u "$DOCKER_USER" -w "$WORKSPACE" client bash ./install_client.sh 2>&1 | tee "${BUILD_DIR}/log_client_install.txt"
}

function testServer() {
  docker exec -e TERM=xterm-color -u "$DOCKER_USER" -w "$WORKSPACE" -e INSTALLROOT="$WORKSPACE" -e INSTALLTYPE=server server \
      bash TestCode/DIRAC/tests/CI/run_tests.sh || SERVER_CODE=$?
  echo ${SERVER_CODE:-0} > "${BUILD_DIR}/server_test_status"
  docker cp server:/home/dirac/serverTestOutputs.txt "${BUILD_DIR}/log_server_tests.txt"
}

function testClient() {
  docker exec -e TERM=xterm-color -u "$DOCKER_USER" -w "$WORKSPACE" -e INSTALLROOT="$WORKSPACE" -e INSTALLTYPE=client client \
      bash TestCode/DIRAC/tests/CI/run_tests.sh || CLIENT_CODE=$?
  echo ${CLIENT_CODE:-0} > "${BUILD_DIR}/client_test_status"
  docker cp client:/home/dirac/clientTestOutputs.txt "${BUILD_DIR}/log_client_tests.txt"
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
