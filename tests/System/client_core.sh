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
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "======  dirac-proxy-info"
dirac-proxy-info
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "======  dirac-proxy-get-uploaded-info"
dirac-proxy-get-uploaded-info
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "======  dirac-proxy-destroy"
dirac-proxy-get-destroy
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "======  dirac-info"
dirac-info
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "======  dirac-proxy-info (now this will fail...)"
dirac-proxy-info
if [ $? -eq 0 ]
then
   exit $?
fi

echo " "
echo "dirac-proxy-init -g dirac_prod"
dirac-proxy-init -g dirac_prod
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "======  dirac-version"
dirac-version
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "======  dirac-platform"
dirac-platform
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "======  dirac-configuration-dump-local-cache"
dirac-configuration-dump-local-cache
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "




echo " "
echo " "
echo " ########################## Resources Management #############################"
echo " "
echo " "

echo "======  dirac-admin-get-banned-sites"
dirac-admin-get-banned-sites
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-admin-get-site-mask"
dirac-admin-get-site-mask
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======   dirac-admin-site-info LCG.CERN.cern"
dirac-admin-site-info LCG.CERN.cern
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-dms-show-se-status"
dirac-dms-show-se-status
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-rss-list-status --element=Site"
dirac-rss-list-status --element=Site
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-rss-list-status --element=Resource --name=CERN-USER"
dirac-rss-list-status --element=Resource --name=CERN-USER
if [ $? -ne 0 ]
then
   exit $?
fi
# echo " "
# echo "======  dirac-rss-list-status --element=Resource --name=CERN-RAW,CERN-DST"
# dirac-rss-list-status --element=Resource --name=CERN-RAW,CERN-DST
# if [ $? -ne 0 ]
# then
#    exit $?
# fi
# echo " "

# echo " "
# echo "======  dirac-rss-query-dt-cache select --element=Site"
# dirac-rss-query-dt-cache select --element=Site
# if [ $? -ne 0 ]
# then
#    exit $?
# fi
echo " "




echo " "
echo " "
echo " ########################## Framework #############################"
echo " "
echo " "


echo " "
echo "======  dirac-monitoring-get-components-status"
dirac-monitoring-get-components-status
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
