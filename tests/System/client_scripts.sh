#!/bin/bash

# This is a script that tests various scripts
# It is based on script outputs and exit codes

echo " "
echo " "
echo " ########################## Getting a proxy #############################"
echo " "
echo " "

echo "dirac-proxy-init -g dirac_prod"
dirac-proxy-init -g dirac_prod
if [[ "${?}" -ne 0 ]]; then
   exit "${?}"
fi
echo " "
echo "======  dirac-proxy-info"
dirac-proxy-info
if [[ "${?}" -ne 0 ]]; then
   exit "${?}"
fi
echo " "

echo " "
echo " "
echo " ########################## WMS #############################"
echo " "
echo " "

echo "======  dirac-admin-show-task-queues"
dirac-admin-show-task-queues
if [[ "${?}" -ne 0 ]]; then
   exit "${?}"
fi
echo " "

echo "======  dirac-wms-get-wn-parameters --Site=LCG.CERN.cern --Name=ce503.cern.ch --Queue=condor"
dirac-wms-get-wn-parameters --Site=LCG.CERN.cern --Name=ce503.cern.ch --Queue=condor
if [[ "${?}" -ne 0 ]]; then
   exit "${?}"
fi
echo " "

echo " "
echo " "
echo " ########################## RMS #############################"
echo " "
echo " "

echo "======  dirac-rms-reqdb-summary"
dirac-rms-reqdb-summary
if [[ "${?}" -ne 0 ]]; then
   exit "${?}"
fi
echo " "

echo "======  dirac-rms-list-req-cache"
dirac-rms-list-req-cache
if [[ "${?}" -ne 0 ]]; then
   exit "${?}"
fi
echo " "

