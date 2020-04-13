echo " "
echo " "
echo " ########################## Data Management #############################"
echo " "
echo " ########################## BEGIN OF USER FILES TEST #############################"
echo " "
echo " "

echo "====== dirac-proxy-init -g dteam_user" #this is necesary to upload user files
dirac-proxy-init -g dteam_user

dirac_user=$( dirac-proxy-info | awk '/^username / {print $3}' )
#userdir="/dteam/user/$( echo "$USER" |cut -c 1)/$USER"
userdir="/dteam/user/$( echo "$dirac_user" |cut -c 1)/$dirac_user"
echo "this is a test file" >> DMS_Scripts_Test_File.txt

echo " "
echo "====== dirac-dms-add-file $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt ./DMS_Scripts_Test_File.txt RAL-SE"
dirac-dms-add-file $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt ./DMS_Scripts_Test_File.txt RAL-SE
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
mv DMS_Scripts_Test_File.txt DMS_Scripts_Test_File.old
echo "======  dirac-dms-replicate-lfn $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CESNET-SE RAL-SE"
dirac-dms-replicate-lfn $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CESNET-SE RAL-SE
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "====== dirac-dms-catalog-metadata $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-catalog-metadata $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "====== dirac-dms-lfn-metadata $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-lfn-metadata $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "====== dirac-dms-lfn-accessURL $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt RAL-SE"
dirac-dms-lfn-accessURL $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt RAL-SE
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi
echo " "

echo " "
echo "====== dirac-dms-get-file $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-get-file $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
ls DMS_Scripts_Test_File.txt
if [[ "${?}" -ne 0 ]]; then
   exit 1
else
   echo "File downloaded properly"
fi

echo " "
echo "====== dirac-dms-lfn-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-lfn-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "====== dirac-dms-user-lfns"
dirac-dms-user-lfns
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "====== dirac-dms-remove-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CESNET-SE"
dirac-dms-remove-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CESNET-SE
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "====== dirac-dms-lfn-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt second time: now there should be only 1 replica"
dirac-dms-lfn-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "====== dirac-dms-remove-files $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt RAL-SE"
dirac-dms-remove-files $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt RAL-SE
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

echo " "
echo "====== dirac-dms-lfn-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt third time: now there should be no replicas"
dirac-dms-lfn-replicas $userdir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [[ "${?}" -ne 0 ]]; then
   exit 1
fi

rm DMS_Scripts_Test_File.*

echo " "
echo " "
echo " ########################## END OF USER FILES TEST #############################"
echo " "
