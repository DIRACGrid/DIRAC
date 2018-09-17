#!/bin/bash

: '
This is a script that tests:
dirac-rss-query-db,
dirac-rss-list-status,,
dirac-rss-set-token,
dirac-rss-query-dtcache

It is based on script outputs and exit codes
'

echo " "
echo " "
echo " ########################## Getting a proxy #############################"
echo " "
echo " "

echo "dirac-proxy-init -g dirac_prod"
dirac-proxy-init -g dirac_prod
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

echo " "
echo " "
echo " ########################## Resource Status #############################"
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


echo -e "\n\n TESTING: dirac-rss-query-db --name=test123 --status=Banned --statusType=ReadAccess --reason=test add resource status"
dirac-rss-query-db --name=test123 --status=Banned --statusType=ReadAccess --elementType=StorageElement --reason=test add resource status -dd
if [ $? -ne 0 ]
then
  echo -e "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-list-status --name=test123 --element=Resource"
dirac-rss-list-status --name=test123 --element=Resource -dd
if [ $? -ne 0 ]
then
  echo -e "Script dirac-rss-list-status did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-set-token --name=test123 --element=Resource --reason=RSStest --releaseToken"
TEST_OUT=$( dirac-rss-set-token --name=test123 --element=Resource --reason=RSStest --releaseToken -dd )
if [ $? -ne 0 ]
then
  echo -e "Script dirac-rss-set-token did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-list-status --name=test123 --element=Resource -dd"
TEST_OUT=$( dirac-rss-list-status --name=test123 --element=Resource -dd )
if [[ $TEST_OUT != *"rs_svc"* ]]
then
  echo -e "Script dirac-rss-set-token did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

echo -e "\n\n TESTING: dirac-rss-query-db --name=test123 delete resource status -dd"
TEST_OUT=$( dirac-rss-query-db --name=test123 delete resource status -dd )
if [[ $TEST_OUT != *"successfully executed"* ]]
then
  echo -e "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

echo -e "\n\n TESTING: dirac-rss-list-status --name=test123 --element=Resource -dd"
TEST_OUT=$( dirac-rss-list-status --name=test123 --element=Resource -dd )
if [[ $TEST_OUT != *"No output"* ]]
then
  echo -e "Script dirac-rss-query-db did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi



echo " "
echo " "
echo " ########################## Resource Management #############################"
echo " "
echo " "

echo -e "\n\n TESTING: dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 add"
TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 add -dd )

if [[ $TEST_OUT != *"successfully executed"* ]]
then
  echo -e "Script dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi

if [ $? -ne 0 ]
then
   exit $?
fi

echo -e "\n\n TESTING: dirac-rss-query-dtcache --name=dtest123 select"
TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 select )
if [[ $TEST_OUT != *"4354354789"* ]]
then
  echo -e "Script dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 delete -dd"
TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 delete -dd )
if [[ $TEST_OUT != *"successfully executed"* ]]
then
  echo -e "\n\nScript dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi

echo -e "\n\n TESTING: dirac-rss-query-dtcache --name=dtest123 select"
TEST_OUT=$( dirac-rss-query-dtcache --name=dtest123 select )
if [[ $TEST_OUT != *"number: 0"* ]]
then
  echo -e "\n\nScript dirac-rss-query-dtcache did not get executed successfully \n"
  exit 1
fi
