#!/usr/bin/env bash
#-------------------------------------------------------------------------------
# A convenient way to run all the workflow tests
#
# It supposes that there's a DIRAC pilot is installed
#-------------------------------------------------------------------------------

echo -e '****************************************'
echo -e '********' "workflow tests" '********\n'

source PilotInstallDIR/bashrc

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo -e "THIS_DIR=${THIS_DIR}" |& tee -a pilotTestOutputs.txt

cp PilotInstallDIR/pilot.cfg "${THIS_DIR}"

echo -e "*** $(date -u)  Getting a non privileged user\n" |& tee -a pilotTestOutputs.txt
dirac-admin-get-proxy "/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser" dirac_user -o /DIRAC/Security/UseServerCertificate=True --cfg pilot.cfg --out=/tmp/x509up_u${UID} -ddd
dirac-configure -FDMH -o /DIRAC/Security/UseServerCertificate=False -O pilot.cfg pilot.cfg -ddd

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Integration TESTS ****\n"
python "${THIS_DIR}/../Workflow/Integration/Test_UserJobs.py" pilot.cfg -o /DIRAC/Security/UseServerCertificate=no -ddd |& tee -a pilotTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** Regression TESTS ****\n"
python "${THIS_DIR}/../Workflow/Regression/Test_RegressionUserJobs.py" -o /DIRAC/Security/UseServerCertificate=no pilot.cfg -ddd |& tee -a pilotTestOutputs.txt; (( ERR |= "${?}" ))
