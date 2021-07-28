#!/usr/bin/env bash

# This is a script that tests various WMS scripts
# It is based on script outputs and exit codes

declare -a commands=(
'dirac-admin-show-task-queues'
'dirac-wms-get-wn-parameters --Site=LCG.CERN.cern --Name=ce503.cern.ch --Queue=condor'
'dirac-admin-sync-pilot'
'dirac-wms-cpu-normalization'
'dirac-wms-get-queue-cpu-time 10 -o /LocalSite/GridCE=ce503.cern.ch -o /LocalSite/CEQueue=condor'
'dirac-wms-get-wn --Site=LCG.CERN.cern'
'dirac-wms-get-wn-parameters --Site=LCG.CERN.cern --Name=ce503.cern.ch --Queue=condor'
'dirac-admin-site-info LCG.CERN.cern'
'dirac-wms-get-normalized-queue-length ce503.cern.ch/condor'
'dirac-wms-get-queue-normalization ce503.cern.ch/condor'
'dirac-wms-select-jobs --Status=Running'
)

for command in "${commands[@]}"
do
  echo "************************************************"
  echo " "
  if ! bash -c "${command}"; then
    echo "${command}" gives error
    exit 1
  fi
done
