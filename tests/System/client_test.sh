#!/bin/bash

# This is a generic list of commands run from a client for testing/certification purposes.
#
# Submitter should follow through the logs


echo " "
echo " "
echo " ########################## REAL BASICS #############################"
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
echo " "
echo "======  dirac-rss-list-status --element=Resource --name=CERN-RAW,CERN-DST"
dirac-rss-list-status --element=Resource --name=CERN-RAW,CERN-DST
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

echo " "
echo "======  dirac-rss-query-dt-cache select --element=Site"
dirac-rss-query-dt-cache select --element=Site
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

echo " "
echo " "
echo " ########################## Data Management #############################"
echo " "
echo " "

echo "======  dirac-dms-lfn-replicas /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000071.raw"
dirac-dms-lfn-replicas /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000071.raw
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-lfn-metadata /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw"
dirac-dms-lfn-metadata /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-lfn-accessURL /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw"
dirac-dms-lfn-accessURL /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000044.raw
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

echo " "
echo " "
echo " ########################## BEGIN OF USER FILES TEST #############################"
echo " "
echo " "

dir=$( echo "$USER" |cut -c 1)/$USER
echo "this is a test file" >> DMS_Scripts_Test_File.txt

echo "====== dirac-dms-add-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CNAF-USER"
dirac-dms-add-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt ./DMS_Scripts_Test_File.txt CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
mv DMS_Scripts_Test_File.txt DMS_Scripts_Test_File.old
echo "======  dirac-dms-replicate-lfn /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt SARA-USER CNAF-USER"
dirac-dms-replicate-lfn /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt SARA-USER CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-catalog-metadata /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-catalog-metadata /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-get-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-get-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

echo " "
echo "====== dirac-dms-remove-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt SARA-USER"
dirac-dms-remove-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt SARA-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
ls DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
else
   echo "File downloaded properly"
fi
echo " "
echo "====== dirac-dms-remove-files /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CNAF-USER"
dirac-dms-remove-files /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo " "
echo " ########################## END OF USER FILES TEST #############################"
echo " "
echo " "
echo "======  dirac-monitoring-get-components-status"
dirac-monitoring-get-components-status
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
