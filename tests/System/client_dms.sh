#!/bin/bash

echo " "
echo " "
echo " ########################## Data Management #############################"
echo " "
echo " ########################## BEGIN OF USER FILES TEST #############################"
echo " "
echo " "

echo "====== dirac-login -g dteam_user" #this is necesary to upload user files
dirac-login dteam_user

dirac_user=$( dirac-proxy-info | awk '/^username / {print $3}' )
#userdir="/dteam/user/$( echo "$USER" | cut -c 1)/$USER"
userdir="/dteam/user/$( echo "$dirac_user" | cut -c 1)/$dirac_user"
echo "this is a test file" >> DMS_Scripts_Test_File.txt

echo " "
echo "====== dirac-dms-add-file $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt ./DMS_Scripts_Test_File.txt RAL-SE"
if ! dirac-dms-add-file "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt ./DMS_Scripts_Test_File.txt RAL-SE; then
   exit 1
fi

echo " "
mv DMS_Scripts_Test_File.txt DMS_Scripts_Test_File.old
echo "======  dirac-dms-replicate-lfn $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt UKI-LT2-IC-HEP-disk RAL-SE"
if ! dirac-dms-replicate-lfn "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt UKI-LT2-IC-HEP-disk RAL-SE; then
   exit 1
fi

echo " "
echo "====== dirac-dms-catalog-metadata $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
if ! dirac-dms-catalog-metadata "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt; then
   exit 1
fi

echo " "
echo "====== dirac-dms-lfn-metadata $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
if ! dirac-dms-lfn-metadata "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt; then
   exit 1
fi

echo " "
echo "====== dirac-dms-lfn-accessURL $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt RAL-SE"
if ! dirac-dms-lfn-accessURL "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt RAL-SE; then
   exit 1
fi
echo " "

echo " "
echo "====== dirac-dms-get-file $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
if ! dirac-dms-get-file "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt; then
   exit 1
fi

echo " "
if ! ls DMS_Scripts_Test_File.txt; then
   exit 1
else
   echo "File downloaded properly"
fi

echo " "
echo "====== dirac-dms-lfn-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
if ! dirac-dms-lfn-replicas "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt; then
   exit 1
fi

echo " "
echo "====== dirac-dms-user-lfns"
if ! dirac-dms-user-lfns; then
   exit 1
fi

echo " "
echo "====== dirac-dms-remove-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt UKI-LT2-IC-HEP-disk"
if ! dirac-dms-remove-replicas "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt UKI-LT2-IC-HEP-disk; then
   exit 1
fi

echo " "
echo "====== dirac-dms-lfn-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt second time: now there should be only 1 replica"
if ! dirac-dms-lfn-replicas "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt; then
   exit 1
fi

echo " "
echo "====== dirac-dms-remove-files $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
if ! dirac-dms-remove-files "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt; then
   exit 1
fi

echo " "
echo "====== dirac-dms-lfn-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt third time: now there should be no replicas"
if ! dirac-dms-lfn-replicas "$userdir"/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt; then
   exit 1
fi

rm DMS_Scripts_Test_File.*

echo " "
echo " "
echo " ########################## END OF USER FILES TEST #############################"
echo " "
