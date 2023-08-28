#!/bin/bash

# This is a generic list of commands run from a client for testing/certification purposes.
#
# Submitter should follow through the logs

echo " "
echo " "
echo " ########################## REAL BASICS #############################"
echo " "
echo " "

echo "dirac-login"
if ! dirac-login; then
   exit 1
fi

echo " "
echo "======  dirac-login --status"
if ! dirac-login --status; then
   exit 1
fi

echo " "
echo "======  dirac-proxy-info"
if ! dirac-proxy-info; then
   exit 1
fi

echo " "
echo "======  dirac-proxy-get-uploaded-info"
if ! dirac-proxy-get-uploaded-info; then
   exit 1
fi

echo " "
echo "======  dirac-proxy-destroy"
if ! dirac-proxy-destroy; then
   exit 1
fi

echo " "
echo "======  dirac-info"
if ! dirac-info; then
   exit 1
fi

echo " "
echo "======  dirac-proxy-info (now this will fail...)"
if dirac-proxy-info; then
   exit 1
fi

echo " "
echo "dirac-login"
if ! dirac-login; then
   exit 1
fi

echo " "
echo "======  dirac-platform"
if ! dirac-platform; then
   exit 1
fi

echo " "
echo "======  dirac-configuration-dump-local-cache"
if ! dirac-configuration-dump-local-cache; then
   exit 1
fi
echo " "
