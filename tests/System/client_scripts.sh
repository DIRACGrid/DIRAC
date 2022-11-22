#!/bin/bash

# This is a script that tests various scripts
# It is based on script outputs and exit codes

echo " "
echo " "
echo " ########################## Getting a proxy #############################"
echo " "
echo " "

echo "dirac-login dirac_prod"
if ! dirac-login dirac_prod; then
   exit 1
fi
echo " "
echo "======  dirac-proxy-info"
if ! dirac-proxy-info; then
   exit 1
fi

echo " "
echo " "
echo " ########################## RMS #############################"
echo " "
echo " "

echo "======  dirac-rms-reqdb-summary"
if ! dirac-rms-reqdb-summary; then
   exit 1
fi
echo " "

echo "======  dirac-rms-list-req-cache"
if ! dirac-rms-list-req-cache; then
   exit 1
fi
echo " "

echo " "
echo " "
echo " ########################## Resources #############################"
echo " "
echo " "

echo "======  dirac-resource-info -S"
if ! dirac-resource-info -S; then
   exit 1
fi
echo " "

echo "======  dirac-resource-info -C"
if ! dirac-resource-info -C; then
   exit 1
fi
echo " "
