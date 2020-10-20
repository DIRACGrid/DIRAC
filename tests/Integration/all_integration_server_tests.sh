#!/usr/bin/env bash
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests for servers
#
# It supposes that DIRAC is installed in ${SERVERINSTALLDIR}
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '*******' "integration server tests" '*******\n'

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Configuration TESTS ****\n"
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/ConfigurationSystem/Test_Helpers.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Core TESTS ****\n"
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/Core/Test_ElasticsearchDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/Core/Test_MySQLDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** FRAMEWORK TESTS (partially skipped) ****\n"
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/Framework/Test_InstalledComponentsDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
python "${SERVERINSTALLDIR}/DIRAC/tests/Integration/Framework/Test_ProxyDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
#pytest ${SERVERINSTALLDIR}/DIRAC/tests/Integration/Framework/Test_LoggingDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RSS TESTS ****\n"
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/ResourceStatusSystem/Test_FullChain.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** WMS TESTS ****\n"
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobLoggingDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_ElasticJobParametersDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobParameters_MySQLandES.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
python "${SERVERINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_Client_WMS.py" --cfg "${WORKSPACE}/TestCode/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** DMS TESTS ****\n"
## DFC
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/DataManagementSystem/Test_DataIntegrityDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

echo "Test DFC DB" |& tee -a "${SERVER_TEST_OUTPUT}"
python "${SERVERINSTALLDIR}/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

echo -e "*** $(date -u)  Reinitialize the DFC DB\n" |& tee -a "${SERVER_TEST_OUTPUT}"
diracDFCDB |& tee -a "${SERVER_TEST_OUTPUT}"

echo -e "*** $(date -u)  Run the DFC client tests as user without admin privileges" |& tee -a "${SERVER_TEST_OUTPUT}"
echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a "${SERVER_TEST_OUTPUT}"
dirac-proxy-init -C "${WORKSPACE}/ServerInstallDIR/user/client.pem" -K "${WORKSPACE}/ServerInstallDIR/user/client.key" "${DEBUG}"
python "${SERVERINSTALLDIR}/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
diracDFCDB |& tee -a "${SERVER_TEST_OUTPUT}"
python "${SERVERINSTALLDIR}/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

echo 'Reinitialize the DFC DB' |& tee -a "${SERVER_TEST_OUTPUT}"
diracDFCDB |& tee -a "${SERVER_TEST_OUTPUT}"

echo -e "*** $(date -u)  Restart the DFC service\n" |& tee -a "${SERVER_TEST_OUTPUT}"
dirac-restart-component DataManagement FileCatalog "${DEBUG}" |& tee -a "${SERVER_TEST_OUTPUT}"
dirac-restart-component Tornado Tornado "${DEBUG}" |& tee -a "${SERVER_TEST_OUTPUT}"

echo -e "*** $(date -u)  Run it with the admin privileges" |& tee -a "${SERVER_TEST_OUTPUT}"
echo -e "*** $(date -u)  getting the prod role again\n" |& tee -a "${SERVER_TEST_OUTPUT}"
dirac-proxy-init -g prod -C "${WORKSPACE}/ServerInstallDIR/user/client.pem" -K "${WORKSPACE}/ServerInstallDIR/user/client.key" "${DEBUG}" |& tee -a "${SERVER_TEST_OUTPUT}"
python "${SERVERINSTALLDIR}/DIRAC/tests/Integration/DataManagementSystem/Test_Client_DFC.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
diracDFCDB |& tee -a "${SERVER_TEST_OUTPUT}"
python "${SERVERINSTALLDIR}/DIRAC/tests/Integration/DataManagementSystem/Test_FileCatalogDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** FTS TESTS ****\n"
# I know, it says Client, but it also instaciates a DB, so it needs to be here
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/DataManagementSystem/Test_Client_FTS3.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RMS TESTS ****\n"
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/RequestManagementSystem/Test_ReqDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** MONITORING TESTS ****\n"
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/Monitoring/Test_MonitoringReporter.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** Resources TESTS ****\n"

python "${SERVERINSTALLDIR}/DIRAC/tests/Integration/Resources/Storage/Test_Resources_GFAL2StorageBase.py" ProductionSandboxSE |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/Resources/Computing/Test_SingularityCE.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
python "${SERVERINSTALLDIR}/DIRAC/tests/Integration/Resources/ProxyProvider/Test_DIRACCAProxyProvider.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

# Can only run if there's a Stomp MQ local...
# TODO Enable
# pytest "${SERVERINSTALLDIR}/DIRAC/tests/Integration/Resources/MessageQueue/Test_ActiveClose.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
