#!/usr/bin/env bash
#-------------------------------------------------------------------------------

# Tests that require a $DIRACSCRIPTS pointing to DIRAC deployed scripts location,
# and a $DIRAC variable pointing to an installed DIRAC
# It also assumes that pilot.cfg contains all the necessary for running

echo "\n======> Test_TimeLeft <======\n"

if [[ ! -z "$DEBUG" ]]; then
  echo '==> Running in DEBUG mode'
  DEBUG='-ddd'
else
  echo '==> Running in non-DEBUG mode'
fi



###############################################################################
# Can't find anywhere a batch plugin, not even MJF

$DIRACSCRIPTS/dirac-wms-get-queue-cpu-time $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG

if [[ "${?}" -eq 0 ]]; then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n" >&2
  exit 1
fi


###############################################################################
# Found MJF, not reading it (not a directory)

export MACHINEFEATURES=$DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg
export JOBFEATURES=$DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/sb.cfg

$DIRACSCRIPTS/dirac-wms-get-queue-cpu-time $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG

if [[ "${?}" -eq 0 ]]; then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n" >&2
  exit 1
fi


###############################################################################
# Found MJF, gave proper values

export MACHINEFEATURES=$DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/MJF/
export JOBFEATURES=$DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/MJF/

$DIRACSCRIPTS/dirac-wms-get-queue-cpu-time $DIRAC/DIRAC/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG

if [[ "${?}" -eq 0 ]]; then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n" >&2
  exit 1
fi

exit 0
