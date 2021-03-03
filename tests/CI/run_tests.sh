#!/bin/bash
#
#   Executable script that copies tests from TestCode dir to source, and
#   starts server or client unit tests
#
#   Requires arguments
#     $INSTALLROOT - should contain directories TestCode, ServerInstallDIR,
#                    ClientInstallDIR
#     $INSTALLTYPE - client or server, deciding which tests to run
#
#.........................................................................

if [[ -z $INSTALLROOT || -z $INSTALLTYPE ]]; then
    echo "Arguments missing. " >&2
    exit 1
fi

cd "$INSTALLROOT" || exit 1

# shellcheck source=tests/CI/CONFIG
source CONFIG
source "${DIRAC_CI_SETUP_SCRIPT}"

echo -e "*** $(date -u) **** Starting integration tests on ${INSTALLTYPE} ****\n"

if [[ "$INSTALLTYPE" == "server" ]]; then
    # shellcheck source=/dev/null
    source "$WORKSPACE/ServerInstallDIR/bashrc"
    # shellcheck disable=SC2034
    SERVER_TEST_OUTPUT=serverTestOutputs.txt
    set -o pipefail
    ERR=0
    for repo_path in "${TESTREPO[@]}"; do
        # TODO: The tests should be refactored to remove the need for this
        cp -r "${repo_path}/tests" "$WORKSPACE/ServerInstallDIR/$(basename "${repo_path}")"
        if [[ "$(basename "${repo_path}")" == "DIRAC" ]]; then
            sed -i "s/\(elHost = \).*/\1'elasticsearch'/" "$WORKSPACE/ServerInstallDIR/DIRAC/tests/Integration/Core/Test_ElasticsearchDB.py"
        fi
    done
    for repo_path in "${TESTREPO[@]}"; do
        source "$WORKSPACE/ServerInstallDIR/$(basename "${repo_path}")/tests/Integration/all_integration_server_tests.sh"
    done
elif [[ "$INSTALLTYPE" == "client" ]]; then
    # shellcheck source=/dev/null
    source "$WORKSPACE/ClientInstallDIR/bashrc"
    set -o pipefail
    ERR=0
    for repo_path in "${TESTREPO[@]}"; do
        source "${repo_path}/tests/Integration/all_integration_client_tests.sh"
    done
fi

echo -e "*** $(date -u) **** TESTS OVER ****\n"

if [[ -z "$ERR" ]]; then
    echo "WARN: Variable \$ERR not defined, check the test logs for possible failed tests"
    exit 0
elif [[ "$ERR" != "0" ]]; then
   echo "ERROR: At least one unit test in ${INSTALLTYPE} failed !!!" >&2
   exit "$ERR"
else
   echo "SUCCESS: All tests succeded"
   exit 0
fi
