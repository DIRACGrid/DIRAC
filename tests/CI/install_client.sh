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
CSURL=dips://$SERVER_HOST:9135/Configuration/Server

echo -e "*** $(date -u) **** Getting the tests ****\n"

mkdir -p "$PWD/TestCode"
cd "$PWD/TestCode"

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

cd ..

echo -e "*** $(date -u) **** Got the DIRAC tests ****\n"

source "${DIRAC_CI_SETUP_SCRIPT}"
# shellcheck disable=SC2034
DIRACSETUP=$(< "${INSTALL_CFG_FILE}" grep "Setup = " | cut -f5 -d " ")

echo -e "*** $(date -u) **** Client INSTALLATION START ****\n"

findRelease

if [ -z "$DIRAC_RELEASE" ]; then
    export DIRAC_RELEASE=${projectVersion:?}
fi

installDIRAC
