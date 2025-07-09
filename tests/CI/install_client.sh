#!/bin/bash
#
#   Executable script to install the DIRAC client
#
#   Requires no additional external variables
#
#.....................................................
# set -euo pipefail
set -eo pipefail
# IFS=$'\n\t'
set -x

# shellcheck source=tests/CI/CONFIG
source CONFIG

# shellcheck disable=SC2034
CSURL=https://$SERVER_HOST:9135/Configuration/Server

mkdir -p /home/dirac/ClientInstallDIR/etc
mkdir -p "${HOME}/.globus"
cp /ca/certs/client.pem "${HOME}/.globus/usercert.pem"
cp /ca/certs/client.key "${HOME}/.globus/userkey.pem"

echo -e "*** $(date -u) **** Getting the tests ****\n"

mkdir -p "$PWD/TestCode"
cd "$PWD/TestCode"

if [[ -n "${INSTALLATION_BRANCH}" ]]; then
    # Use this for (e.g.) running backward-compatibility tests
    echo "Using https://github.com/DIRACGrid/DIRAC.git@${INSTALLATION_BRANCH} for the tests"
    git clone --single-branch --branch "$INSTALLATION_BRANCH" "https://github.com/DIRACGrid/DIRAC.git"
    mkdir -p /home/dirac/ServerInstallDIR/user
    cd /home/dirac/ServerInstallDIR/user
    ln -s "${HOME}/.globus/usercert.pem" client.pem
    ln -s "${HOME}/.globus/userkey.pem" client.key
else
    for repo_path in "${TESTREPO[@]}"; do
        if [[ -d "${repo_path}" ]]; then
            cp -r "${repo_path}" "$(basename "${repo_path}")"
            cd "$(basename "${repo_path}")"
            echo "Using local test repository in branch $(git branch | grep "\*" | sed -e "s/* //")"
            cd -
        else
            git clone --single-branch --branch "$TESTBRANCH" "https://github.com/$repo_path/DIRAC.git"
            echo "Using remote test repository ${repo_path} in branch ${TESTBRANCH}"
        fi
    done
fi

cd ..

echo -e "*** $(date -u) **** Got the DIRAC tests ****\n"

source "${DIRAC_CI_SETUP_SCRIPT}"

if [[ -n "${INSTALLATION_BRANCH}" ]]; then
    # shellcheck disable=SC2034
    DIRACSETUP=$(< "${INSTALL_CFG_FILE}" grep "Setup = " | cut -f5 -d " ")
fi

echo -e "*** $(date -u) **** Client INSTALLATION START ****\n"

installDIRAC

if [[ -z "${INSTALLATION_BRANCH}" ]]; then
    echo 'Generate a pilot proxy, to be used by the pilot'
    dirac-proxy-init -g pilot -C /ca/certs/pilot.pem -K /ca/certs/pilot.key "${DEBUG}"
    mv /tmp/x509up_u$UID /ca/certs/pilot_proxy

    echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a clientTestOutputs.txt
    dirac-proxy-init "${DEBUG}" |& tee -a clientTestOutputs.txt

    #-------------------------------------------------------------------------------#
    echo -e "*** $(date -u) **** Submit a job ****\n"

    echo -e '[\n    Arguments = "Hello World";\n    Executable = "echo";\n    Site = "DIRAC.Jenkins.ch";' > test.jdl
    echo "    JobName = \"${GITHUB_JOB}_$(date +"%Y-%m-%d_%T" | sed 's/://g')\"" >> test.jdl
    echo "]" >> test.jdl
    dirac-wms-job-submit test.jdl "${DEBUG}" |& tee -a clientTestOutputs.txt

    #-------------------------------------------------------------------------------#
    echo -e "*** $(date -u) **** add a file ****\n"

    echo "${CLIENT_UPLOAD_BASE64}" > b64_lfn
    base64 b64_lfn --decode > "${CLIENT_UPLOAD_FILE}"
    dirac-dms-add-file "${CLIENT_UPLOAD_LFN}" "${CLIENT_UPLOAD_FILE}" S3-DIRECT
    echo $?

    #-------------------------------------------------------------------------------#
    echo -e "*** $(date -u) **** Submit a job with an input ****\n"

    echo -e '[\n    Arguments = "Hello World";\n    Executable = "echo";\n    Site = "DIRAC.Jenkins.ch";\n    InputData = "/vo/test_lfn.txt";' > test_dl.jdl
    echo "    JobName = \"${GITHUB_JOB}_$(date +"%Y-%m-%d_%T" | sed 's/://g')\"" >> test_dl.jdl
    echo "]" >> test_dl.jdl
    dirac-wms-job-submit test_dl.jdl "${DEBUG}" |& tee -a clientTestOutputs.txt
fi
