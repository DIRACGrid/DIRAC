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
echo " ########################## BEGIN OF WMS scripts tests #############################"
echo " "
echo " "

# TODO: add 
# submit
# status
# logging info
# paramaters....


echo " "
echo "======  dirac-wms-select-jobs --Site=LCG.Cern.cern --Status=Running"
dirac-wms-select-jobs --Site=LCG.Cern.cern --Status=Running
if [ $? -ne 0 ]
then
   exit $?
fi




echo " "
echo " "
echo " ########################## Data Management #############################"
echo " "
echo " "

echo "======  dirac-dms-lfn-replicas /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/Test_Files_20170810_121717_002.init"
#dirac-dms-lfn-replicas /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81789/081789_0000000071.raw
dirac-dms-lfn-replicas /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/Test_Files_20170810_121717_002.init
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-lfn-metadata /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/Test_Files_20170810_121717_002.init"
dirac-dms-lfn-metadata /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/Test_Files_20170810_121717_002.init
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-lfn-accessURL /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/Test_Files_20170810_121717_002.init"
dirac-dms-lfn-accessURL /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/Test_Files_20170810_121717_002.init CNAF-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

echo " "
echo "======  dirac-dms-check-directory-integrity /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/"
dirac-dms-check-directory-integrity /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "====== dirac-dms-check-file-integrity /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/Test_Files_20170810_121717_002.init"
dirac-dms-check-file-integrity /lhcb/user/m/msoares/Dirac_Scripts_Test_Directory/Test_Files_20170810_121717_002.init
if [ $? -ne 0 ]
then
   exit $?
fi


echo " "
echo " "
echo " ########################## BEGIN OF USER FILES TEST #############################"
echo " "
echo " "

echo "====== dirac-proxy-init -g lhcb_user" #this is necesary to 
dirac-proxy-init -g lhcb_user

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

rm DMS_Scripts_Test_File.*
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
