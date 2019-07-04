#!/bin/sh
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests for servers
#
# It supposes that DIRAC is installed in $SERVERINSTALLDIR
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '*******' "integration server tests" '*******\n'



#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** Core TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Core/Test_ElasticsearchDB.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** FRAMEWORK TESTS (partially skipped) ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_InstalledComponentsDB.py >> testOutputs.txt 2>&1
#pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_LoggingDB.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** RSS TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_FullChain.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** WMS TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobLoggingDB.py >> testOutputs.txt 2>&1
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py >> testOutputs.txt 2>&1


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
echo -e '***' $(date -u)  "**** MONITORING TESTS ****\n"
pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Monitoring/Test_MonitoringReporter.py >> testOutputs.txt 2>&1


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** Resources TESTS ****\n"
python $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/Storage/Test_Resources_GFAL2StorageBase.py ProductionSandboxSE >> testOutputs.txt 2>&1

# Can only run if there's a Stomp MQ local... 
# python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/MessageQueue/Test_ActiveClose.py >> testOutputs.txt 2>&1
