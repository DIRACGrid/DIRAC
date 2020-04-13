#!/bin/bash

# This is a generic list of commands run from a client for testing/certification purposes.
#
# Submitter should follow through the logs

echo " "
echo " "
echo " ########################## REAL BASICS #############################"
echo " "
echo " "

echo "dirac-proxy-init"
dirac-proxy-init
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "======  dirac-proxy-info"
dirac-proxy-info
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "======  dirac-proxy-get-uploaded-info"
dirac-proxy-get-uploaded-info
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "======  dirac-proxy-destroy"
dirac-proxy-destroy
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "======  dirac-info"
dirac-info
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "======  dirac-proxy-info (now this will fail...)"
dirac-proxy-info
if [[ "${?}" -eq 0 ]]; then
   exit 1
fi

echo " "
echo "dirac-proxy-init"
dirac-proxy-init
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "======  dirac-version"
dirac-version
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "======  dirac-platform"
dirac-platform
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "======  dirac-configuration-dump-local-cache"
dirac-configuration-dump-local-cache
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo " "



echo " "
echo " "
echo " ########################## Framework #############################"
echo " "
echo " "


echo " "
echo "======  dirac-monitoring-get-components-status"
dirac-monitoring-get-components-status
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo " "