#!/bin/sh
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests
#
# It supposes that DIRAC is installed in $SERVERINSTALLDIR
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** Core TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Test_ElasticsearchDB.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** Accounting TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/AccountingSystem/Test_DataStoreClient.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/AccountingSystem/Test_ReportsClient.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** FRAMEWORK TESTS (partially skipped) ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_InstalledComponentsDB.py >> testOutputs.txt 2>&1
#pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_LoggingDB.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** RMS TESTS ****\n"

echo -e '***' $(date -u)  "Getting a non privileged user\n" >> testOutputs.txt 2>&1
dirac-proxy-init -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG >> testOutputs.txt 2>&1

echo -e '***' $(date -u)  "Starting RMS Client test as a non privileged user\n" >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/RequestManagementSystem/Test_Client_Req.py >> testOutputs.txt 2>&1

echo -e '***' $(date -u)  "getting the prod role again\n" >> testOutputs.txt 2>&1
dirac-proxy-init -g prod -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG >> testOutputs.txt 2>&1
echo -e '***' $(date -u)  "Starting RMS Client test as an admin user\n" >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/RequestManagementSystem/Test_Client_Req.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** RSS TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_FullChain.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_Publisher.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_ResourceManagement.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_ResourceStatus.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_SiteStatus.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** WMS TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobLoggingDB.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_PilotsLoggingClient.py >> testOutputs.txt 2>&1
python $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_Client_WMS.py $WORKSPACE/TestCode/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg >> testOutputs.txt 2>&1
python $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_SandboxStoreClient.py $WORKSPACE/TestCode/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobWrapper.py >> testOutputs.txt 2>&1
## no real tests
python $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/createJobXMLDescriptions.py >> testOutputs.txt 2>&1
$SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_dirac-jobexec.sh >> testOutputs.txt 2>&1
$SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_TimeLeft.sh >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** DMS TESTS ****\n"
## DFC
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_DataIntegrityDB.py

echo "Test DFC DB" >> testOutputs.txt 2>&1
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py >> testOutputs.txt 2>&1

echo -e '***' $(date -u)  "Reinitialize the DFC DB\n" >> testOutputs.txt 2>&1
diracDFCDB >> testOutputs.txt 2>&1

echo -e '***' $(date -u)  "Run the DFC client tests as user without admin privileges" >> testOutputs.txt 2>&1
echo -e '***' $(date -u)  "Getting a non privileged user\n" >> testOutputs.txt 2>&1
dirac-proxy-init -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py >> testOutputs.txt 2>&1
diracDFCDB
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py >> testOutputs.txt 2>&1

echo "Reinitialize the DFC DB" >> testOutputs.txt 2>&1
diracDFCDB >> testOutputs.txt 2>&1

echo -e '***' $(date -u)  "Restart the DFC service\n" &>> testOutputs.txt
dirac-restart-component DataManagement FileCatalog $DEBUG &>> testOutputs.txt

echo -e '***' $(date -u)  "Run it with the admin privileges" >> testOutputs.txt 2>&1
echo -e '***' $(date -u)  "getting the prod role again\n" >> testOutputs.txt 2>&1
dirac-proxy-init -g prod -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG >> testOutputs.txt 2>&1
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py >> testOutputs.txt 2>&1
diracDFCDB
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** FTS TESTS ****\n"
pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_Client_FTS3.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** MONITORING TESTS ****\n"
pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Monitoring/Test_MonitoringReporter.py >> testOutputs.txt 2>&1
pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Monitoring/Test_MonitoringSystem.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** TS TESTS ****\n"
pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/TransformationSystem/Test_Client_Transformation.py >> testOutputs.txt 2>&1
# pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/TransformationSystem/Test_TS_DFC_Catalog.py >> testOutputs.txt 2>&1

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** Resources TESTS ****\n"
python $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/Storage/Test_Resources_GFAL2StorageBase.py ProductionSandboxSE >> testOutputs.txt 2>&1
# python $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/Storage/Test_Resources_Echo.py >> testOutputs.txt 2>&1

python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/MessageQueue/Test_ActiveClose.py >> testOutputs.txt 2>&1
