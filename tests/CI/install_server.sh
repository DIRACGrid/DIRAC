#!/bin/bash
#
#   Executable script to install full DIRAC server
#
#   Requires no additional external arguments
#
#.........................................................

set -ex

source CONFIG

echo -e '***' $(date -u) "**** Getting the tests ****\n"

mkdir -p $PWD/TestCode
cd $PWD/TestCode

if [ -d $TESTREPO ]; then
    cp -r $TESTREPO ./DIRAC
    cd DIRAC
    echo "Using local test repository in branch $(git branch | grep \* | sed -e "s/*^* //")"
else
    git clone https://github.com/$TESTREPO/DIRAC.git
    cd DIRAC
    git checkout $TESTBRANCH
    echo "Using remote test repository ${TESTREPO} in branch ${TESTBRANCH}"
fi

cd ../..

echo -e '***' $(date -u) "**** Got the DIRAC tests ****\n"


echo -e '***' $(date -u) "**** Server INSTALLATION START ****\n"

sed -i "0,/\(Host = \).*/s//\1$SERVER_HOST/" TestCode/DIRAC/tests/Jenkins/install.cfg
source TestCode/DIRAC/tests/Jenkins/dirac_ci.sh


X509_CERT_DIR=$SERVERINSTALLDIR/etc/grid-security/certificates/ fullInstallDIRAC

echo -e '***' $(date -u) "**** Server INSTALLATION DONE ****\n"

