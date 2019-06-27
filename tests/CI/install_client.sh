#!/bin/bash
#
#   Executable script to install the DIRAC client
#
#   Requires no addidional external variables
#
#.....................................................

source CONFIG

CSURL=dips://$SERVER_HOST:9135/Configuration/Server
DIRACSETUP=dirac-JenkinsSetup

export PATH=$PATH:/sbin


echo -e '***' $(date -u) "**** Getting the tests ****\n"

mkdir -p $PWD/TestCode
cd $PWD/TestCode


git clone https://github.com/$TESTREPO/DIRAC.git
cd DIRAC
git checkout $TESTBRANCH

cd ../..


echo -e '***' $(date -u) "**** Got the tests ****\n"

set -e

alias pytest='pytest -v -s'

sed -i '/installES/d' TestCode/DIRAC/tests/Jenkins/dirac_ci.sh
source TestCode/DIRAC/tests/Jenkins/dirac_ci.sh


echo -e '***' $(date -u) "**** Client INSTALLATION START ****\n"
set -x

findRelease

installDIRAC
