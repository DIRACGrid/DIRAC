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
    echo "Arguments missing. "
    exit 1
fi

cd $INSTALLROOT

SERVERINSTALLDIR=$INSTALLROOT/ServerInstallDIR
CLIENTINSTALLDIR=$INSTALLROOT/ClientInstallDIR
TESTCODE=$INSTALLROOT/TestCode
WORKSPACE=$INSTALLROOT

source CONFIG
source $TESTCODE/DIRAC/tests/Jenkins/dirac_ci.sh

echo -e '***' $(date -u) "**** Starting integration tests on ${INSTALLTYPE} ****\n"

if [ $INSTALLTYPE == "server" ]; then
    source $SERVERINSTALLDIR/bashrc

    sed -i "s/\(elHost = \).*/\1'elasticsearch'/" $TESTCODE/DIRAC/tests/Integration/Core/Test_ElasticsearchDB.py

    cp -r $TESTCODE/DIRAC/tests $SERVERINSTALLDIR/DIRAC/

    source $SERVERINSTALLDIR/DIRAC/tests/Integration/all_integration_server_tests.sh
elif [ $INSTALLTYPE == "client" ]; then
    source $CLIENTINSTALLDIR/bashrc

    cp -r $TESTCODE/DIRAC/tests $CLIENTINSTALLDIR/DIRAC/

    source $CLIENTINSTALLDIR/DIRAC/tests/Integration/all_integration_client_tests.sh
fi

echo -e '***' $(date -u) "**** TESTS OVER ****\n"

if [ -z $ERR ]; then
    echo "WARN: Variable \$ERR not defined, check the test logs for possible failed tests"
    exit 0
elif [ $ERR != "0" ]; then
   echo "ERROR: At least one unit test in ${INSTALLTYPE} failed !!!"
   exit $ERR
else
   echo "SUCCESS: All tests succeded"
   exit 0
fi
   
