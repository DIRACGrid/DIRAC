#!/bin/sh

echo "\n======> Test_ProxyManager <======\n"

if [ ! -z "$DEBUG" ]
then
  echo '==> Running in DEBUG mode'
  DEBUG='-ddd'
else
  echo '==> Running in non-DEBUG mode'
fi

# Go to server
source $SERVERINSTALLDIR/bashrc
runsvctrl d $SERVERINSTALLDIR/startup/Framework_ProxyManager
$DIRACSCRIPTS/dirac-service Framework/ProxyManager $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_ProxyManager.cfg > /dev/null  &

# Go to client
source $CLIENTINSTALLDIR/bashrc
python $CLIENTINSTALLDIR/DIRAC/tests/Integration/Framework/Test_ProxyManager.py $DEBUG

# Go to server
source $SERVERINSTALLDIR/bashrc
kill -9 `jobs -p`
runsvctrl u $SERVERINSTALLDIR/startup/Framework_ProxyManager
