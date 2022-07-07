#!/bin/bash

# This is a script that tests various scripts
# It is based on script outputs and exit codes

echo " "
echo " "
echo " ########################## Getting a proxy #############################"
echo " "
echo " "

echo "dirac-login dirac_prod"
dirac-login dirac_prod
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
echo " "
echo " ########################## RMS #############################"
echo " "
echo " "

echo "======  dirac-rms-reqdb-summary"
dirac-rms-reqdb-summary
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo " "

echo "======  dirac-rms-list-req-cache"
dirac-rms-list-req-cache
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo " "

echo " "
echo " "
echo " ########################## Resources #############################"
echo " "
echo " "

echo "======  dirac-resource-info -S"
dirac-resource-info -S
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo " "

echo "======  dirac-resource-info -C"
dirac-resource-info -C
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo " "
