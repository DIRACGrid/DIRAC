#!/bin/bash

# This is a generic list of commands run from a client for testing/certification purposes.
#
# Submitter should follow through the logs

echo
echo
echo " ########################## REAL BASICS #############################"
echo
echo

echo "________________________"
echo "===  dirac-proxy-init -U"
dirac-proxy-init -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key -U
if [ $? -ne 0 ]
then
   exit $?
fi
echo

echo "_____________________"
echo "===  dirac-proxy-info"
dirac-proxy-info
if [ $? -ne 0 ]
then
   exit $?
fi
echo

echo "__________________________________"
echo "===  dirac-proxy-get-uploaded-info"
dirac-proxy-get-uploaded-info
if [ $? -ne 0 ]
then
   exit $?
fi
echo

echo "___________________________"
echo "===  dirac-proxy-destroy -a"
dirac-proxy-destroy -a
if [ $? -ne 0 ]
then
   exit $?
fi
echo

echo "_____________________________________________"
echo "===  dirac-proxy-info (now this will fail...)"
dirac-proxy-info
if [ $? -eq 0 ]
then
   exit $?
fi
echo

echo "___________________________________"
echo "===  dirac-proxy-init -g dirac_prod"
dirac-proxy-init -g dirac_prod -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key -U
if [ $? -ne 0 ]
then
   exit $?
fi
echo

diracCredentials

echo "_______________________________________________________"
echo "dirac-admin-get-proxy adminusername dirac_prod -v 4:00"
dirac-admin-get-proxy adminusername dirac-admin -v 4:00
if [ $? -ne 0 ]
then
   exit $?
fi
echo

echo "_______________"
echo "===  dirac-info"
dirac-info
if [ $? -ne 0 ]
then
   exit $?
fi
echo

echo "__________________"
echo "===  dirac-version"
dirac-version
if [ $? -ne 0 ]
then
   exit $?
fi
echo

echo "___________________"
echo "===  dirac-platform"
dirac-platform
if [ $? -ne 0 ]
then
   exit $?
fi

echo "_________________________________________"
echo "===  dirac-configuration-dump-local-cache"
dirac-configuration-dump-local-cache
if [ $? -ne 0 ]
then
   exit $?
fi
echo



echo
echo
echo " ########################## Framework #############################"
echo
echo


echo "___________________________________________"
echo "===  dirac-monitoring-get-components-status"
dirac-monitoring-get-components-status
if [ $? -ne 0 ]
then
   exit $?
fi
echo
