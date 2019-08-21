#!/bin/sh
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests for servers
#
# It supposes that DIRAC is installed in $SERVERINSTALLDIR
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '*******' "integration server tests" '*******\n'


ERR=0
#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** Core TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Core/Test_ElasticsearchDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u) "**** FRAMEWORK TESTS (partially skipped) ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_InstalledComponentsDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
python $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_ProxyDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
#pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_LoggingDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** RSS TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_FullChain.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** WMS TESTS ****\n"
python $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_Client_WMS.py $WORKSPACE/TestCode/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobLoggingDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_ElasticJobDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** DMS TESTS ****\n"
## DFC
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_DataIntegrityDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))

echo "Test DFC DB" >> serverTestOutputs.txt 2>&1
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))

echo -e '***' $(date -u)  "Reinitialize the DFC DB\n" >> serverTestOutputs.txt 2>&1
diracDFCDB >> serverTestOutputs.txt 2>&1

echo -e '***' $(date -u)  "Run the DFC client tests as user without admin privileges" >> serverTestOutputs.txt 2>&1
echo -e '***' $(date -u)  "Getting a non privileged user\n" >> serverTestOutputs.txt 2>&1
dirac-proxy-init -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
diracDFCDB >> serverTestOutputs.txt 2>&1
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))

echo "Reinitialize the DFC DB" >> serverTestOutputs.txt 2>&1
diracDFCDB >> serverTestOutputs.txt 2>&1

echo -e '***' $(date -u)  "Restart the DFC service\n" &>> serverTestOutputs.txt
dirac-restart-component DataManagement FileCatalog $DEBUG &>> serverTestOutputs.txt

echo -e '***' $(date -u)  "Run it with the admin privileges" >> serverTestOutputs.txt 2>&1
echo -e '***' $(date -u)  "getting the prod role again\n" >> serverTestOutputs.txt 2>&1
dirac-proxy-init -g prod -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG >> serverTestOutputs.txt 2>&1
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
diracDFCDB >> serverTestOutputs.txt 2>&1
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** RMS TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/RequestManagementSystem/Test_ReqDB.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** MONITORING TESTS ****\n"
pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Monitoring/Test_MonitoringReporter.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e '***' $(date -u)  "**** Resources TESTS ****\n"

python $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/Storage/Test_Resources_GFAL2StorageBase.py ProductionSandboxSE >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
python $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/ProxyProvider/Test_DIRACCAProxyProvider.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))

# Can only run if there's a Stomp MQ local... 
# python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/MessageQueue/Test_ActiveClose.py >> serverTestOutputs.txt 2>&1; (( ERR |= $? ))
