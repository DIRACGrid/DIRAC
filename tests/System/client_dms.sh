echo " "
echo " "
echo " ########################## Data Management #############################"
echo " "
echo " ########################## BEGIN OF USER FILES TEST #############################"
echo " "
echo " "

echo "====== dirac-proxy-init -g lhcb_user" #this is necesary to upload user files
dirac-proxy-init -g lhcb_user

dir=$( echo "$USER" |cut -c 1)/$USER
echo "this is a test file" >> DMS_Scripts_Test_File.txt

echo " "
echo "====== dirac-dms-add-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt ./DMS_Scripts_Test_File.txt CERN-USER"
dirac-dms-add-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt ./DMS_Scripts_Test_File.txt CERN-USER
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
mv DMS_Scripts_Test_File.txt DMS_Scripts_Test_File.old
echo "======  dirac-dms-replicate-lfn /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt GRIDKA-USER CERN-USER"
dirac-dms-replicate-lfn /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt GRIDKA-USER CERN-USER
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
echo "====== dirac-dms-lfn-metadata /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-lfn-metadata /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "====== dirac-dms-lfn-accessURL /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CERN-USER"
dirac-dms-lfn-accessURL /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CERN-USER
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

echo " "
echo "====== dirac-dms-get-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-get-file /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
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
echo "====== dirac-dms-lfn-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt"
dirac-dms-lfn-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "====== dirac-dms-user-lfns"
dirac-dms-user-lfns
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "====== dirac-dms-remove-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt GRIDKA-USER"
dirac-dms-remove-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt GRIDKA-USER
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "====== dirac-dms-lfn-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt second time: now there should be only 1 replica"
dirac-dms-lfn-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "====== dirac-dms-remove-files /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CERN-USER"
dirac-dms-remove-files /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt CERN-USER
if [ $? -ne 0 ]
then
   exit $?
fi

echo " "
echo "====== dirac-dms-lfn-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt third time: now there should be no replicas"
dirac-dms-lfn-replicas /lhcb/user/$dir/Dirac_Scripts_Test_Directory/DMS_Scripts_Test_File.txt
if [ $? -ne 0 ]
then
   exit $?
fi

rm DMS_Scripts_Test_File.*

echo " "
echo " "
echo " ########################## END OF USER FILES TEST #############################"
echo " "
