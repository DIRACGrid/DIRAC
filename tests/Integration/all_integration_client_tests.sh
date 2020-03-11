#!/usr/bin/env bash
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests for client -> server interaction
#
# It supposes that DIRAC client is installed in $CLIENTINSTALLDIR
# and that there's a DIRAC server running with all the services running.
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '********' "client -> server tests" '********\n'

echo -e "*** $(date -u)  Getting a non privileged user\n" 2>&1 | tee -a clientTestOutputs.txt
dirac-proxy-init -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Accounting TESTS ****\n"
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/AccountingSystem/Test_DataStoreClient.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/AccountingSystem/Test_ReportsClient.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RMS TESTS ****\n"

echo -e "*** $(date -u)  Getting a non privileged user\n" 2>&1 | tee -a clientTestOutputs.txt
dirac-proxy-init -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG 2>&1 | tee -a clientTestOutputs.txt

echo -e "*** $(date -u)  Starting RMS Client test as a non privileged user\n" 2>&1 | tee -a clientTestOutputs.txt
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/RequestManagementSystem/Test_Client_Req.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))

echo -e "*** $(date -u)  getting the prod role again\n" 2>&1 | tee -a clientTestOutputs.txt
dirac-proxy-init -g prod -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG 2>&1 | tee -a clientTestOutputs.txt
echo -e "*** $(date -u)  Starting RMS Client test as an admin user\n" 2>&1 | tee -a clientTestOutputs.txt
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/RequestManagementSystem/Test_Client_Req.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RSS TESTS ****\n"
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_ResourceManagement.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_ResourceStatus.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_SiteStatus.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_Publisher.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** WMS TESTS ****\n"
# pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_PilotsLoggingClient.py 2>&1 | tee -a clientTestOutputs.txt
python $CLIENTINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_SandboxStoreClient.py $WORKSPACE/TestCode/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobWrapper.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
python $CLIENTINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_Client_WMS.py $WORKSPACE/TestCode/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_PilotsClient.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))

## no real tests
python $CLIENTINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/createJobXMLDescriptions.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
$CLIENTINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_dirac-jobexec.sh 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
$CLIENTINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_TimeLeft.sh 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** MONITORING TESTS ****\n"
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/Monitoring/Test_MonitoringSystem.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** TS TESTS ****\n"
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/TransformationSystem/Test_Client_Transformation.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
# pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/TransformationSystem/Test_TS_DFC_Catalog.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** PS TESTS ****\n"
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/ProductionSystem/Test_Client_Production.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/ProductionSystem/Test_Client_TS_Prod.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** DataManager TESTS ****\n"

echo -e "*** $(date -u)  Getting a privileged user\n" 2>&1 | tee -a clientTestOutputs.txt
dirac-proxy-init -g jenkins_fcadmin -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG 2>&1 | tee -a clientTestOutputs.txt

cat >> dataManager_create_folders <<EOF

mkdir /Jenkins
chgrp -R jenkins_user Jenkins
chmod -R 774 Jenkins
exit

EOF

# the filecatalog-cli script sorts alphabetically all the defined catalog and takes the first one....
# which of course does not work if your catalog is called Bookkeeping... so force to use the real DFC
dirac-dms-filecatalog-cli -f FileCatalog < dataManager_create_folders

echo -e "*** $(date -u)  Getting a non privileged user\n" 2>&1 | tee -a clientTestOutputs.txt
dirac-proxy-init -g jenkins_user -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG 2>&1 | tee -a clientTestOutputs.txt

pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_DataManager.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))


echo -e "*** $(date -u) **** S3 TESTS ****\n"
pytest $CLIENTINSTALLDIR/DIRAC/tests/Integration/Resources/Storage/Test_Resources_S3.py 2>&1 | tee -a clientTestOutputs.txt; (( ERR |= $? ))
