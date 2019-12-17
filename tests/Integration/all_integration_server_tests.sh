#!/usr/bin/env bash
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests for servers
#
# It supposes that DIRAC is installed in $SERVERINSTALLDIR
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '*******' "integration server tests" '*******\n'

SERVER_TEST_OUTPUT=serverTestOutputs.txt

set -o pipefail
ERR=0
#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Core TESTS ****\n"
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/Core/Test_ElasticsearchDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** FRAMEWORK TESTS (partially skipped) ****\n"
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_InstalledComponentsDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
python $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_ProxyDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
#pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_LoggingDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RSS TESTS ****\n"
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_FullChain.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** WMS TESTS ****\n"
python $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_Client_WMS.py $WORKSPACE/TestCode/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobLoggingDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_ElasticJobDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobParameters_MySQLandES.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** DMS TESTS ****\n"
## DFC
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_DataIntegrityDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))

echo "Test DFC DB" 2>&1 | tee -a $SERVER_TEST_OUTPUT
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))

echo -e "*** $(date -u)  Reinitialize the DFC DB\n" 2>&1 | tee -a $SERVER_TEST_OUTPUT
diracDFCDB 2>&1 | tee -a $SERVER_TEST_OUTPUT

echo -e "*** $(date -u)  Run the DFC client tests as user without admin privileges" 2>&1 | tee -a $SERVER_TEST_OUTPUT
echo -e "*** $(date -u)  Getting a non privileged user\n" 2>&1 | tee -a $SERVER_TEST_OUTPUT
dirac-proxy-init -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
diracDFCDB 2>&1 | tee -a $SERVER_TEST_OUTPUT
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))

echo "Reinitialize the DFC DB" 2>&1 | tee -a $SERVER_TEST_OUTPUT
diracDFCDB 2>&1 | tee -a $SERVER_TEST_OUTPUT

echo -e "*** $(date -u)  Restart the DFC service\n" 2>&1 | tee -a $SERVER_TEST_OUTPUT
dirac-restart-component DataManagement FileCatalog $DEBUG 2>&1 | tee -a $SERVER_TEST_OUTPUT

echo -e "*** $(date -u)  Run it with the admin privileges" 2>&1 | tee -a $SERVER_TEST_OUTPUT
echo -e "*** $(date -u)  getting the prod role again\n" 2>&1 | tee -a $SERVER_TEST_OUTPUT
dirac-proxy-init -g prod -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG 2>&1 | tee -a $SERVER_TEST_OUTPUT
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
diracDFCDB 2>&1 | tee -a $SERVER_TEST_OUTPUT
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RMS TESTS ****\n"
pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/RequestManagementSystem/Test_ReqDB.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** MONITORING TESTS ****\n"
pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Monitoring/Test_MonitoringReporter.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** Resources TESTS ****\n"

python $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/Storage/Test_Resources_GFAL2StorageBase.py ProductionSandboxSE 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
python $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/ProxyProvider/Test_DIRACCAProxyProvider.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))

# Can only run if there's a Stomp MQ local...
# pytest -s $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/MessageQueue/Test_ActiveClose.py 2>&1 | tee -a $SERVER_TEST_OUTPUT; (( ERR |= $? ))
