#!/bin/bash

# This is a generic list of commands run from a client for testing proxy manager client.
#
# Submitter should follow through the logs

echo
echo
echo " ########################## ProxyManager #############################"
echo
echo
echo "================"
echo "dirac-proxy-init -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key -U $DEBUG"
dirac-proxy-init -g prod -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key -U $DEBUG
if [ $? -ne 0 ]
then
   echo
   echo "==> ERROR: not successful"
   exit $?
fi
echo "================"
echo "dirac-proxy-info"
dirac-proxy-info -m
if [ $? -ne 0 ]
then
   echo
   echo "==> ERROR: not successful"
   exit $?
fi
echo "================"
echo "dirac-proxy-get-uploaded-info (must return uploaded proxy)"
dirac-proxy-get-uploaded-info
if [ $? -ne 0 ]
then
   echo
   echo "==> ERROR: not successful"
   exit $?
fi
echo "================"
echo "======  Get admin credentionals"
diracCredentials
echo "================"
echo "dirac-admin-get-proxy adminusername dirac-admin -v 4:00"
dirac-admin-get-proxy adminusername dirac-admin -v 4:00
if [ $? -ne 0 ]
then
   echo
   echo "==> ERROR: not successful"
   exit $?
fi
echo "================"
echo "======  dirac-proxy-destroy"
dirac-proxy-destroy -a
if [ $? -ne 0 ]
then
   echo
   echo "==> ERROR: not successful"
   exit $?
fi
echo "================"
echo "======  dirac-proxy-info (now this will fail...)"
dirac-proxy-info
if [ $? -eq 0 ]
then
   echo
   echo "==> ERROR: Must end with fail"
   exit $?
fi
echo
echo
echo "==> SUCCSSES"