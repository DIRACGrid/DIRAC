#!/bin/bash
#
#   Executable script to install full DIRAC server
#
#   Requires no additional external arguments
#
#.........................................................

set -e

source CONFIG

echo -e '***' $(date -u) "**** Getting the tests ****\n"

mkdir -p $PWD/TestCode
cd $PWD/TestCode


git clone https://github.com/$repository/DIRAC.git
cd DIRAC
git checkout $branch

cd ../..

echo -e '***' $(date -u) "**** Got the tests ****\n"

set -e

alias pytest='pytest -v -s'

sed -i '/installES/d' TestCode/DIRAC/tests/Jenkins/dirac_ci.sh
sed -i "0,/\(Host = \).*/s//\1$SERVER_HOST/" TestCode/DIRAC/tests/Jenkins/install.cfg
source TestCode/DIRAC/tests/Jenkins/dirac_ci.sh


echo -e '***' $(date -u) "**** Server INSTALLATION START ****\n"
set -x

X509_CERT_DIR=$SERVERINSTALLDIR/etc/grid-security/certificates/ fullInstallDIRAC

echo -e '***' $(date -u) "**** Server INSTALLATION DONE ****\n"

