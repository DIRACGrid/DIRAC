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

echo -e "*** $(date -u) **** Getting the tests ****\n"

mkdir -p "$PWD/TestCode"
cd "$PWD/TestCode"

if [[ -n "${INSTALLATION_BRANCH}" ]]; then
    # Use this for (e.g.) running backward-compatibility tests
    echo "Using https://github.com/DIRACGrid/DIRAC.git@${INSTALLATION_BRANCH} for the tests"
    git clone "https://github.com/DIRACGrid/DIRAC.git"
    cd DIRAC
    git checkout "$INSTALLATION_BRANCH"
    cd -
else
    for repo_path in "${TESTREPO[@]}"; do
        if [[ -d "${repo_path}" ]]; then
            cp -r "${repo_path}" "$(basename "${repo_path}")"
            cd "$(basename "${repo_path}")"
            echo "Using local test repository in branch $(git branch | grep "\*" | sed -e "s/* //")"
            cd -
        else
            git clone "https://github.com/$repo_path/DIRAC.git"
            cd "$(basename "${repo_path}")"
            git checkout "$TESTBRANCH"
            echo "Using remote test repository ${repo_path} in branch ${TESTBRANCH}"
            cd -
        fi
    done
fi

cd ..

echo -e "*** $(date -u) **** Got the DIRAC tests ****\n"

source "${DIRAC_CI_SETUP_SCRIPT}"
# shellcheck disable=SC2034
DIRACSETUP=$(< "${INSTALL_CFG_FILE}" grep "Setup = " | cut -f5 -d " ")

echo -e "*** $(date -u) **** Client INSTALLATION START ****\n"

installDIRAC

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Submit a job ****\n"

echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a clientTestOutputs.txt
dirac-login -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt

echo -e '[\n    Arguments = "Hello World";\n    Executable = "echo";\n    Site = "DIRAC.Jenkins.ch";' > test.jdl
echo "    JobName = \"${GITHUB_JOB}_$(date +"%Y-%m-%d_%T" | sed 's/://g')\"" >> test.jdl
echo "]" >> test.jdl
dirac-wms-job-submit test.jdl
