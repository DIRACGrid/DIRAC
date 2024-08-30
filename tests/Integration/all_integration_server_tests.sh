#!/usr/bin/env bash
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests for servers
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '*******' "integration server tests" '*******\n'

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo -e "THIS_DIR=${THIS_DIR}" |& tee -a "${SERVER_TEST_OUTPUT}"

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Accounting TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/AccountingSystem/Test_Plots.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/AccountingSystem/Test_AccountingDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Configuration TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/ConfigurationSystem/Test_Helpers.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Core TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/Core/Test_ElasticsearchDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/Core/Test_MySQLDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** FRAMEWORK TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/Framework/Test_InstalledComponentsDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/Framework/Test_ProxyDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/Framework/Test_TokenDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/Framework/Test_AuthDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/Framework/Test_AuthServer.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** MONITORING TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/Monitoring/Test_MonitoringReporter.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/Monitoring/Test_MonitoringDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** TS TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/TransformationSystem/Test_TS_CheckPerms.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RSS TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/ResourceStatusSystem/Test_FullChain.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** WMS TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_JobDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_JobLoggingDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_TaskQueueDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_JobParametersDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_PilotAgentsDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_SandboxMetadataDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
# pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_JobCleaningAgent.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** DMS TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/DataManagementSystem/Test_DataIntegrityDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

echo 'Reinitialize the DFC DB' |& tee -a "${SERVER_TEST_OUTPUT}"
diracDFCDB |& tee -a "${SERVER_TEST_OUTPUT}"
echo "Test DFC DB" |& tee -a "${SERVER_TEST_OUTPUT}"
python "${THIS_DIR}/DataManagementSystem/Test_FileCatalogDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

echo -e "*** $(date -u)  Reinitialize the DFC DB\n" |& tee -a "${SERVER_TEST_OUTPUT}"
diracDFCDB |& tee -a "${SERVER_TEST_OUTPUT}"

echo -e "*** $(date -u)  Restart the DFC service (required for Test_Client_DFC)\n" |& tee -a "${SERVER_TEST_OUTPUT}"
if [[ "${TEST_HTTPS:-Yes}" = "No" ]]; then
  dirac-restart-component DataManagement FileCatalog "${DEBUG}" |& tee -a "${SERVER_TEST_OUTPUT}"
fi
dirac-restart-component Tornado Tornado "${DEBUG}" |& tee -a "${SERVER_TEST_OUTPUT}"

echo -e "*** $(date -u)  Run the DFC client tests as user without admin privileges" |& tee -a "${SERVER_TEST_OUTPUT}"
echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a "${SERVER_TEST_OUTPUT}"
dirac-login -C "${WORKSPACE}/ServerInstallDIR/user/client.pem" -K "${WORKSPACE}/ServerInstallDIR/user/client.key" "${DEBUG}"
python "${THIS_DIR}/DataManagementSystem/Test_Client_DFC.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
diracDFCDB |& tee -a "${SERVER_TEST_OUTPUT}"
python "${THIS_DIR}/DataManagementSystem/Test_FileCatalogDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

echo 'Reinitialize the DFC DB' |& tee -a "${SERVER_TEST_OUTPUT}"
diracDFCDB |& tee -a "${SERVER_TEST_OUTPUT}"

echo -e "*** $(date -u)  Restart the DFC service (required for Test_Client_DFC)\n" |& tee -a "${SERVER_TEST_OUTPUT}"
if [[ "${TEST_HTTPS:-Yes}" = "No" ]]; then
  dirac-restart-component DataManagement FileCatalog "${DEBUG}" |& tee -a "${SERVER_TEST_OUTPUT}"
fi
dirac-restart-component Tornado Tornado "${DEBUG}" |& tee -a "${SERVER_TEST_OUTPUT}"

echo -e "*** $(date -u)  Run it with the admin privileges" |& tee -a "${SERVER_TEST_OUTPUT}"
echo -e "*** $(date -u)  getting the prod role again\n" |& tee -a "${SERVER_TEST_OUTPUT}"
dirac-login prod -C "${WORKSPACE}/ServerInstallDIR/user/client.pem" -K "${WORKSPACE}/ServerInstallDIR/user/client.key" "${DEBUG}" |& tee -a "${SERVER_TEST_OUTPUT}"
python "${THIS_DIR}/DataManagementSystem/Test_Client_DFC.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
diracDFCDB |& tee -a "${SERVER_TEST_OUTPUT}"
python "${THIS_DIR}/DataManagementSystem/Test_FileCatalogDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** FTS TESTS ****\n"
# I know, it says Client, but it also instantiates a DB, so it needs to be here
pytest --no-check-dirac-environment "${THIS_DIR}/DataManagementSystem/Test_Client_FTS3.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RMS TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/RequestManagementSystem/Test_ReqDB.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** Resources TESTS ****\n"

python "${THIS_DIR}/Resources/Storage/Test_Resources_GFAL2StorageBase.py" ProductionSandboxSE |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/Resources/ProxyProvider/Test_DIRACCAProxyProvider.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))

# Can only run if there's a Stomp MQ local...
# TODO Enable
# pytest --no-check-dirac-environment "${THIS_DIR}/Resources/MessageQueue/Test_ActiveClose.py" |& tee -a "${SERVER_TEST_OUTPUT}"; (( ERR |= "${?}" ))
