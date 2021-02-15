#!/bin/bash

# This is a generic list of commands run from inside a test (Jenkins, normally) setup
# for testing/certification purposes.
#
# Submitter should follow through the logs
#

if [[ -z ${SERVERINSTALLDIR+x} ]]; then
  PARAMS=""
else  # To run in Jenkins
  PARAMS="-C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key"
fi

echo
echo
echo " ########################## REAL BASICS #############################"
echo
echo

echo "================================"
echo "===  dirac-proxy-init $PARAMS"
echo
dirac-proxy-init $PARAMS
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo

echo "====================="
echo "===  dirac-proxy-info"
echo
dirac-proxy-info
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo

echo "=================================="
echo "===  dirac-proxy-get-uploaded-info"
echo
dirac-proxy-get-uploaded-info
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo

echo "==========================="
echo "===  dirac-proxy-destroy -a"
echo
dirac-proxy-destroy -a
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo

echo "============================================="
echo "===  dirac-proxy-info (now this will fail...)"
echo
dirac-proxy-info
if [[ "${?}" -eq 0 ]]; then
   exit 1
fi
echo

echo "==============================================="
echo "===  dirac-proxy-init -g dirac_admin $PARAMS"
echo
dirac-proxy-init -g dirac_admin $PARAMS
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo

echo "====================================================="
echo "===  dirac-admin-get-proxy adminusername prod -v 4:00"
echo
dirac-admin-get-proxy adminusername prod -v 4:00
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo

echo "================================================================================="
echo "===  dirac-admin-get-proxy adminusername no_exist -v 4:00 (now this will fail...)"
echo
dirac-admin-get-proxy adminusername no_exist -v 4:00
if [[ "${?}" -eq 0 ]]; then
   exit 1
fi
echo

# Find proxy file
PROXYFILE=`pwd`/proxy.adminusername.prod

echo "================================"
echo "===  dirac-proxy-info $PROXYFILE"
echo
dirac-proxy-info $PROXYFILE
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo
