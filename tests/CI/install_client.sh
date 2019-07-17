#!/bin/bash
#
#   Executable script to install the DIRAC client
#
#   Requires no addidional external variables
#
#.....................................................

set -e

source CONFIG

CSURL=dips://$SERVER_HOST:9135/Configuration/Server
DIRACSETUP=dirac-JenkinsSetup


echo -e '***' $(date -u) "**** Getting the tests ****\n"

mkdir -p $PWD/TestCode
cd $PWD/TestCode


git clone https://github.com/$TESTREPO/DIRAC.git
cd DIRAC
git checkout $TESTBRANCH

cd ../..


echo -e '***' $(date -u) "**** Got the DIRAC tests ****\n"


if [ ! -z $LHCBDIRACTESTBRANCH ]; then
    echo "Detected LHCb branch ${LHCBDIRACTESTBRANCH}, getting the tests"
    
    git clone https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC.git
    cd LHCbDIRAC
    git checkout $LHCBDIRACTESTBRANCH
    cd ../..

    echo -e '***' $(date -u) "**** Got the LHCb DIRAC tests ****\n"
fi


source TestCode/DIRAC/tests/Jenkins/dirac_ci.sh


echo -e '***' $(date -u) "**** Client INSTALLATION START ****\n"

findRelease

installDIRAC


