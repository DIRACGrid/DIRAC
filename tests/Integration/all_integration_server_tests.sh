#!/usr/bin/env bash
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests for servers
#
# It supposes that DIRAC is installed in $SERVERINSTALLDIR
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '*******' "integration server tests" '*******\n'

set -o pipefail
ERR=0
#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Core TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Core/Test_ElasticsearchDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** FRAMEWORK TESTS (partially skipped) ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Framework/Test_InstalledComponentsDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RSS TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/ResourceStatusSystem/Test_FullChain.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** WMS TESTS ****\n"
python $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_Client_WMS.py $WORKSPACE/TestCode/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobLoggingDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_ElasticJobDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobParameters_MySQLandES.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** DMS TESTS ****\n"
## DFC
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_DataIntegrityDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))

echo "Test DFC DB" 2>&1 | tee serverTestOutputs.txt
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))

echo -e "*** $(date -u)  Reinitialize the DFC DB\n" 2>&1 | tee serverTestOutputs.txt
diracDFCDB 2>&1 | tee serverTestOutputs.txt

echo -e "*** $(date -u)  Run the DFC client tests as user without admin privileges" 2>&1 | tee serverTestOutputs.txt
echo -e "*** $(date -u)  Getting a non privileged user\n" 2>&1 | tee serverTestOutputs.txt
dirac-proxy-init -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))
diracDFCDB 2>&1 | tee serverTestOutputs.txt
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))

echo "Reinitialize the DFC DB" 2>&1 | tee serverTestOutputs.txt
diracDFCDB 2>&1 | tee serverTestOutputs.txt

echo -e "*** $(date -u)  Restart the DFC service\n" &>> serverTestOutputs.txt
dirac-restart-component DataManagement FileCatalog $DEBUG &>> serverTestOutputs.txt

echo -e "*** $(date -u)  Run it with the admin privileges" 2>&1 | tee serverTestOutputs.txt
echo -e "*** $(date -u)  getting the prod role again\n" 2>&1 | tee serverTestOutputs.txt
dirac-proxy-init -g prod -C $WORKSPACE/ServerInstallDIR/user/client.pem -K $WORKSPACE/ServerInstallDIR/user/client.key $DEBUG 2>&1 | tee serverTestOutputs.txt
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))
diracDFCDB 2>&1 | tee serverTestOutputs.txt
python $SERVERINSTALLDIR/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RMS TESTS ****\n"
python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/RequestManagementSystem/Test_ReqDB.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** MONITORING TESTS ****\n"
pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Monitoring/Test_MonitoringReporter.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** Resources TESTS ****\n"
python $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/Storage/Test_Resources_GFAL2StorageBase.py ProductionSandboxSE 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))

# Can only run if there's a Stomp MQ local...
# python -m pytest $SERVERINSTALLDIR/DIRAC/tests/Integration/Resources/MessageQueue/Test_ActiveClose.py 2>&1 | tee serverTestOutputs.txt; (( ERR |= $? ))
