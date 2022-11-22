#!/bin/bash

testdir=$(pwd)

JDLFILE="${testdir}/multiVO.jdl"
cat > "${JDLFILE}" <<EOF
[
Executable = "multiVOexe.sh";
StdOutput = "job.log";
StdError = "job.log";
InputSandbox = "multiVOexe.sh";
OutputSandbox = "job.log";
Site = {"LCG.UKI-LT2-IC-HEP.uk", "LCG.UKI-SOUTHGRID-RALPP.uk", "LCG.UKI-SCOTGRID-GLASGOW.uk"};
JobName = "MultiVOTest";
]
EOF


EXEFILE="${testdir}/multiVOexe.sh"
cat > "${EXEFILE}" <<EOF
echo "\\\$DIRACSITE: \$DIRACSITE"
IPADDRESS=\$(hostname --ip-address 2>&1)
echo "\$(hostname) has address " \${IPADDRESS}
echo "Operating System:"
uname -a
cat /etc/redhat-release
env | sort
EOF

MYDATE=$(date +%s)
env > "testfile.${MYDATE}.txt"


echo -e "\nTesting first VO: gridpp"
# just writing the proxy to a different file causes this command to fail
# need to tell DIRAC beforehand where the user proxy will be
export X509_USER_PROXY="${testdir}/gridpp.proxy"
echo "dirac-login gridpp_user --out ${testdir}/gridpp.proxy"
if ! dirac-login gridpp_user --out "${testdir}/gridpp.proxy"; then
   echo "Could not acquire gridpp proxy. Giving up."
   exit 1
fi

echo -e "\nSubmitting job as a gridpp user"
dirac-wms-job-submit -f gridpp_jobid.log multiVO.jdl
sleep 30
dirac-wms-job-status -f gridpp_jobid.log

echo -e "\nUploading file as a gridpp_user"
if ! dirac-dms-add-file "/gridpp/diraccert/testfile.${MYDATE}.txt" "testfile.${MYDATE}.txt" UKI-LT2-IC-HEP-disk; then
   echo "That didn't go well, please check the error."
fi
echo -e "\nRemoving file"
dirac-dms-remove-files "/gridpp/diraccert/testfile.${MYDATE}.txt"


export X509_USER_PROXY="${testdir}/dteam.proxy"
echo -e "\nChanging VO to dteam."
echo "dirac-login dteam_user --out ${testdir}/dteam.proxy"
if ! dirac-login dteam_user --out "${testdir}/dteam.proxy"; then
   echo "Could not acquire dteam proxy. Giving up."
   exit 1
fi

echo -e "\nSubmitting job as a dteam user"
dirac-wms-job-submit -f dteam_jobid.log multiVO.jdl
sleep 30
dirac-wms-job-status -f dteam_jobid.log

echo -e "\nUploading file as a dteam_user"
if ! dirac-dms-add-file "/dteam/diraccert/testfile.${MYDATE}.txt" "testfile.${MYDATE}.txt" UKI-LT2-IC-HEP-disk; then
   echo "That didn't go well, please check the error."
fi
echo -e "\nRemoving file"
dirac-dms-remove-files "/dteam/diraccert/testfile.${MYDATE}.txt"

echo -e "\nBonus test: Try to add file to gridpp storage as a dteam user."
echo "This should fail."
dirac-dms-add-file "/gridpp/diraccert/testfile.${MYDATE}.txt" "testfile.${MYDATE}.txt" UKI-LT2-IC-HEP-disk

echo -e "\nThe client_multivo script has finished. Please check that the jobs run and the output looks OK before considering this test to be passing."
