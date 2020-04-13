#!/usr/bin/env bash
#-------------------------------------------------------------------------------

# Tests that require a $DIRACSCRIPTS pointing to DIRAC deployed scripts location,
# and a $DIRAC variable pointing to an installed DIRAC
# It also assumes that pilot.cfg contains all the necessary for running

echo "\n======> Test_dirac-jobexec <======\n"

if [[ ! -z "$DEBUG" ]]; then
  echo '==> Running in DEBUG mode'
  DEBUG='-ddd'
else
  echo '==> Running in non-DEBUG mode'
fi

# Creating the XML job description files
python $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/createJobXMLDescriptions.py $DEBUG

###############################################################################
# Running the real tests

# OK
$DIRACSCRIPTS/dirac-jobexec jobDescription-OK.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [[ $? -eq 0 ]]; then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi

# OK2
$DIRACSCRIPTS/dirac-jobexec jobDescription-OK-multiSteps.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [[ $? -eq 0 ]]; then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi


# FAIL
$DIRACSCRIPTS/dirac-jobexec jobDescription-FAIL.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [[ $? -eq 111 ]]; then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi

# FAIL2
$DIRACSCRIPTS/dirac-jobexec jobDescription-FAIL-multiSteps.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [[ $? -eq 111 ]]; then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi


# FAIL with exit code > 255
$DIRACSCRIPTS/dirac-jobexec jobDescription-FAIL1502.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [[ $? -eq 222 ]]; then # This is 1502 & 255 (0xDE)
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi

# Removals
rm jobDescription-OK.xml
rm jobDescription-OK-multiSteps.xml
rm jobDescription-FAIL.xml
rm jobDescription-FAIL-multiSteps.xml
rm jobDescription-FAIL1502.xml
rm Script1_CodeOutput.log
