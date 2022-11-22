#!/usr/bin/env bash

# This is a script that tests various RSS scripts
# It is based on script outputs and exit codes

# It should be run with a production proxy

declare -a commands=(
'dirac-admin-get-banned-sites'
'dirac-admin-get-site-mask'
'dirac-admin-site-info LCG.CERN.cern'
'dirac-dms-show-se-status'
'dirac-rss-list-status --element=Site'
'dirac-rss-list-status --element=Resource --name=RAL-SE'
'dirac-rss-list-status --element=Resource --name=CertificationSandboxSE'
'dirac-rss-query-db --name=test123 --status=Banned --statusType=ReadAccess --elementType=StorageElement --reason=test add resource status -dd'
'dirac-rss-list-status --name=test123 --element=Resource -dd'
'dirac-rss-set-token --name=test123 --element=Resource --reason=RSStest --releaseToken -dd'
'dirac-rss-query-db --name=test123 delete resource status -dd'
'dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 --severity=OUTAGE --description="just a test DT" add --startDate="2019-06-12 15:00:00" --endDate="2020-06-12 15:00:00" -dd'
'test $( dirac-rss-query-dtcache --name=dtest123 select | grep dtest123 | wc -l ) = 1'
'test $( dirac-rss-query-dtcache --name=dtest123 --element=Site --downtimeID=4354354789 delete -dd | grep "successfully executed" | wc -l ) = 1'
'test $( dirac-rss-query-dtcache --name=dtest123 select | grep "request successfully executed" | grep "number: 0" | wc -l ) = 1'
)

for command in "${commands[@]}"
do
  echo "************************************************"
  echo " "
  echo "executing ${command}"
  echo " "
  if ! bash -c "${command}"; then
    echo "${command}" gives error
    exit 1
  fi
done
