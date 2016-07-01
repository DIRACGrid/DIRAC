#!/bin/sh
#-------------------------------------------------------------------------------

# Tests that require a $DIRACROOT pointing to DIRAC code
# It also assumes that pilot.cfg (in this directory contains all the necessary for running)

if [ ! -z "$DEBUG" ]
then
	echo '==> Running in DEBUG mode'
	DEBUG='-ddd'
else
	echo '==> Running in non-DEBUG mode'
fi



###############################################################################
# Can't find anywhere a batch plugin, not even MJF

python $DIRACROOT/WorkloadManagementSystem/scripts/dirac-wms-get-queue-cpu-time.py $DIRACROOT/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG

if [ $? -eq 0 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi


###############################################################################
# Found MJF, not reading it (not a directory)

export MACHINEFEATURES=$DIRACROOT/tests/Integration/WorkloadManagementSystem/sb.cfg
export JOBFEATURES=$DIRACROOT/tests/Integration/WorkloadManagementSystem/sb.cfg

python $DIRACROOT/WorkloadManagementSystem/scripts/dirac-wms-get-queue-cpu-time.py $DIRACROOT/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG

if [ $? -eq 0 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi


###############################################################################
# Found MJF, gave proper values

export MACHINEFEATURES=$DIRACROOT/tests/Integration/WorkloadManagementSystem/MJF/
export JOBFEATURES=$DIRACROOT/tests/Integration/WorkloadManagementSystem/MJF/

python $DIRACROOT/WorkloadManagementSystem/scripts/dirac-wms-get-queue-cpu-time.py $DIRACROOT/tests/Integration/WorkloadManagementSystem/pilot.cfg $DEBUG

if [ $? -eq 0 ]
then
  echo -e "\nSuccess\n\n"
else
  echo -e "\nSomething wrong!\n\n"
  exit 1
fi

exit 0
