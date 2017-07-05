#!/bin/sh
#-------------------------------------------------------------------------------

# Tests that require a $DIRACSCRIPTS pointing to DIRAC deployed scripts location,
# and a $DIRAC variable pointing to an installed DIRAC
# It also assumes that pilot.cfg contains all the necessary for running

if [ ! -z "$DEBUG" ]
then
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
if [ $? -eq 0 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi


# FAIL
$DIRACSCRIPTS/dirac-jobexec jobDescription-FAIL.xml $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG
if [ $? -eq 111 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi

# Removals
rm jobDescription-OK.xml
rm jobDescription-FAIL.xml
rm Script1_CodeOutput.log
