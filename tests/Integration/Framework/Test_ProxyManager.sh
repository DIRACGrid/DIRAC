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
cd $SERVERINSTALLDIR
source bashrc
runsvctrl d /opt/dirac/startup/Framework_ProxyManager
$DIRACSCRIPTS/dirac-service Framework/ProxyManager $DIRAC/DIRAC/tests/Integration/Framework/Test_ProxyManager.cfg > /dev/null  &

# Go to client
cd $CLIENTINSTALLDIR
source bashrc
python $CLIENTINSTALLDIR/DIRAC/tests/Integration/Framework/Test_ProxyManager.py $DEBUG

# Go to server
cd $SERVERINSTALLDIR
source bashrc
kill -9 `jobs -p`
runsvctrl u /opt/dirac/startup/Framework_ProxyManager
