#!/usr/bin/env bash
#-------------------------------------------------------------------------------
# A convenient way to run all the integration tests for client -> server interaction
#
# It supposes that there's a DIRAC server running with all the services running.
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '********' "client -> server tests" '********\n'

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo -e "THIS_DIR=${THIS_DIR}" |& tee -a clientTestOutputs.txt

echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a clientTestOutputs.txt
dirac-login -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Accounting TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/AccountingSystem/Test_DataStoreClient.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/AccountingSystem/Test_ReportsClient.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RMS TESTS ****\n"

echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a clientTestOutputs.txt
dirac-login -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt

echo -e "*** $(date -u)  Starting RMS Client test as a non privileged user\n" |& tee -a clientTestOutputs.txt
pytest --no-check-dirac-environment "${THIS_DIR}/RequestManagementSystem/Test_Client_Req.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

echo -e "*** $(date -u)  getting the prod role again\n" |& tee -a clientTestOutputs.txt
dirac-login prod -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt
echo -e "*** $(date -u)  Starting RMS Client test as an admin user\n" |& tee -a clientTestOutputs.txt
pytest --no-check-dirac-environment "${THIS_DIR}/RequestManagementSystem/Test_Client_Req.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Framework TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/Framework/Test_UserProfileClient.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** RSS TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/ResourceStatusSystem/Test_ResourceManagement.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/ResourceStatusSystem/Test_ResourceStatus.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/ResourceStatusSystem/Test_SiteStatus.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/ResourceStatusSystem/Test_Publisher.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/ResourceStatusSystem/Test_EmailActionAgent.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** WMS TESTS ****\n"

pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_SandboxStoreClient.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_JobWrapper.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_PilotsClient.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_WMSAdministratorClient.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/WorkloadManagementSystem/Test_Client_WMS.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

# Make sure we have the prod role for these tests to get the VmRpcOperator permission
dirac-login prod -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt

## no real tests
python "${THIS_DIR}/WorkloadManagementSystem/createJobXMLDescriptions.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
"${THIS_DIR}/WorkloadManagementSystem/Test_dirac-jobexec.sh" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
"${THIS_DIR}/WorkloadManagementSystem/Test_TimeLeft.sh" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** MONITORING TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/Monitoring/Test_MonitoringSystem.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))


#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** TS TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/TransformationSystem/Test_Client_Transformation.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
# pytest --no-check-dirac-environment "${THIS_DIR}/TransformationSystem/Test_TS_DFC_Catalog.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** PS TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/ProductionSystem/Test_Client_Production.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest --no-check-dirac-environment "${THIS_DIR}/ProductionSystem/Test_Client_TS_Prod.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u)  **** Resources TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/Resources/Computing/Test_SingularityCE.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** DataManager TESTS ****\n"


echo -e "*** $(date -u)  Getting a non privileged user to find its VO dynamically\n" |& tee -a clientTestOutputs.txt
dirac-login jenkins_user -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG |& tee -a clientTestOutputs.txt

userVO=$(python -c "import DIRAC; DIRAC.initialize(); from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup; print(getVOfromProxyGroup().get('Value',''))")
userVO="${userVO:-Jenkins}"
echo -e "*** $(date -u) VO is "${userVO}"\n" |& tee -a clientTestOutputs.txt

echo -e "*** $(date -u)  Getting a privileged user\n" |& tee -a clientTestOutputs.txt
dirac-login jenkins_fcadmin -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt

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
dirac-login jenkins_user -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt

pytest --no-check-dirac-environment "${THIS_DIR}/DataManagementSystem/Test_DataManager.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
# MultiVO File Catalog tests are configured to use MultiVOFileCatalog module with a separate DB.
# FileMetadata and DirectoryMetadata options are set to MultiVOFileMetadata and  MultiVODirectoryMetadata
# respectively.

# normal user proxy
dirac-login jenkins_user -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" "${DEBUG}" |& tee -a clientTestOutputs.txt
echo -e "*** $(date -u) **** MultiVO User Metadata TESTS ****\n"
python -m pytest --no-check-dirac-environment "${THIS_DIR}/DataManagementSystem/Test_UserMetadata.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

echo -e "*** $(date -u) **** S3 TESTS ****\n"
pytest --no-check-dirac-environment "${THIS_DIR}/Resources/Storage/Test_Resources_S3.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
