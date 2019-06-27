#!/bin/bash
#
#   Executable script that copies tests from TestCode dir to source, and
#   starts server or client unit tests
#
#   Requires arguments
#     $INSTALLROOT - should contain directories TestCode, ServerInstallDIR,
#                    ClientInstallDIR
#     $AGENT       - client or server, deciding which tests to run
#
#.........................................................................

if [[ -z $INSTALLROOT || -z $AGENT ]]; then
    echo "Arguments missing. "
    exit 1
fi

cd $INSTALLROOT

SERVERINSTALLDIR=$INSTALLROOT/ServerInstallDIR
CLIENTINSTALLDIR=$INSTALLROOT/ClientInstallDIR
TESTCODE=$INSTALLROOT/TestCode
WORKSPACE=$INSTALLROOT

source $TESTCODE/DIRAC/tests/Jenkins/dirac_ci.sh
source CONFIG

echo -e '***' $(date -u) "**** Starting integration tests on ${AGENT} ****\n"

if [ $AGENT == "server" ]; then
    source $SERVERINSTALLDIR/bashrc

    cp -r $TESTCODE/DIRAC/tests $SERVERINSTALLDIR/DIRAC/

    sed -i "s/\(elHost = \).*/\1'elasticsearch'/" $SERVERINSTALLDIR/DIRAC/tests/Integration/Test_ElasticsearchDB.py

    source $SERVERINSTALLDIR/DIRAC/tests/Integration/all_integration_server_tests.sh
elif [ $AGENT == "client" ]; then
    source $CLIENTINSTALLDIR/bashrc

    cp -r $TESTCODE/DIRAC/tests $CLIENTINSTALLDIR/DIRAC/

    SERVERINSTALLDIR=$CLIENTINSTALLDIR
    source $CLIENTINSTALLDIR/DIRAC/tests/Integration/all_integration_client_tests.sh
fi

echo -e '***' $(date -u) "**** TESTS OVER ****\n"
