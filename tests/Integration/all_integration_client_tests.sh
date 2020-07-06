#!/usr/bin/env bash
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests for client -> server interaction
#
# It supposes that DIRAC client is installed in "${CLIENTINSTALLDIR}"
# and that there's a DIRAC server running with all the services running.
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '********' "client -> server tests" '********\n'

echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a clientTestOutputs.txt
dirac-proxy-init -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Accounting TESTS ****\n"
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/AccountingSystem/Test_DataStoreClient.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/AccountingSystem/Test_ReportsClient.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RMS TESTS ****\n"

echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a clientTestOutputs.txt
dirac-proxy-init -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt

echo -e "*** $(date -u)  Starting RMS Client test as a non privileged user\n" |& tee -a clientTestOutputs.txt
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/RequestManagementSystem/Test_Client_Req.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

echo -e "*** $(date -u)  getting the prod role again\n" |& tee -a clientTestOutputs.txt
dirac-proxy-init -g prod -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt
echo -e "*** $(date -u)  Starting RMS Client test as an admin user\n" |& tee -a clientTestOutputs.txt
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/RequestManagementSystem/Test_Client_Req.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RSS TESTS ****\n"
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/ResourceStatusSystem/Test_ResourceManagement.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/ResourceStatusSystem/Test_ResourceStatus.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/ResourceStatusSystem/Test_SiteStatus.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/ResourceStatusSystem/Test_Publisher.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** WMS TESTS ****\n"
# pytest "${CLIENTINSTALLDIR}"/DIRAC/tests/Integration/WorkloadManagementSystem/Test_PilotsLoggingClient.py |& tee -a clientTestOutputs.txt
python "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_SandboxStoreClient.py" "$WORKSPACE/TestCode/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobWrapper.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_PilotsClient.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

## no real tests
python "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/createJobXMLDescriptions.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
"${CLIENTINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_dirac-jobexec.sh" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
"${CLIENTINSTALLDIR}/DIRAC/tests/Integration/WorkloadManagementSystem/Test_TimeLeft.sh" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** MONITORING TESTS ****\n"
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/Monitoring/Test_MonitoringSystem.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
cat "${SERVERINSTALLDIR}/runit/Monitoring/Monitoring/log/current"


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** TS TESTS ****\n"
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/TransformationSystem/Test_Client_Transformation.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
# pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/TransformationSystem/Test_TS_DFC_Catalog.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** PS TESTS ****\n"
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/ProductionSystem/Test_Client_Production.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/ProductionSystem/Test_Client_TS_Prod.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** DataManager TESTS ****\n"


echo -e "*** $(date -u)  Getting a non privileged user to find its VO dynamically\n" |& tee -a clientTestOutputs.txt
dirac-proxy-init -g jenkins_user -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG |& tee -a clientTestOutputs.txt

userVO=$(python -c "from DIRAC.Core.Base.Script import parseCommandLine; parseCommandLine(); from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup; print getVOfromProxyGroup().get('Value','')")
userVO="${userVO:-Jenkins}"
echo -e "*** $(date -u) VO is "${userVO}"\n" |& tee -a clientTestOutputs.txt

echo -e "*** $(date -u)  Getting a privileged user\n" |& tee -a clientTestOutputs.txt
dirac-proxy-init -g jenkins_fcadmin -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt

cat >> dataManager_create_folders <<EOF

mkdir /${userVO}
chgrp -R jenkins_user ${userVO}
chmod -R 774 ${userVO}
exit

EOF

# the filecatalog-cli script sorts alphabetically all the defined catalog and takes the first one....
# which of course does not work if your catalog is called Bookkeeping... so force to use the real DFC
dirac-dms-filecatalog-cli -f FileCatalog < dataManager_create_folders

echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a clientTestOutputs.txt
dirac-proxy-init -g jenkins_user -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt

pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/DataManagementSystem/Test_DataManager.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
# MultiVO File Catalog tests are configured to use MultiVOFileCatalog module with a separate DB.
# FileMetadata and DirectoryMetadata options are set to MultiVOFileMetadata and  MultiVODirectoryMetadata
# respectively. The default FileCatalog Master option is set to False before running the tests.

# need the dirac_admin user, so we can make a change in the CS
echo -e "*** $(date -u)  Getting a dirac_admin user in order to modify the configuration \n" |& tee -a clientTestOutputs.txt
if ! dirac-proxy-init -g dirac_admin -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" --rfc; then
  echo 'ERROR: dirac-proxy-init for dirac_admin failed' >&2
  exit 1
fi
python "${CLIENTINSTALLDIR}/DIRAC/tests/Jenkins/dirac-cfg-update-filecatalog.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
# now back to a normal user
dirac-proxy-init -g jenkins_user -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt
echo -e "*** $(date -u) **** MultiVO User Metadata TESTS ****\n"
python -m pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/DataManagementSystem/Test_UserMetadata.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

echo -e "*** $(date -u) **** S3 TESTS ****\n"
pytest "${CLIENTINSTALLDIR}/DIRAC/tests/Integration/Resources/Storage/Test_Resources_S3.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
