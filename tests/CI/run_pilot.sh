#!/bin/bash
#
#   Executable script to run the DIRAC Pilot
#
#.....................................................
# set -euo pipefail
set -eo pipefail
# IFS=$'\n\t'
set -x

source CONFIG

# Creating "the worker node"
mkdir -p /home/dirac/PilotInstallDIR/etc/grid-security/certificates
mkdir -p /home/dirac/PilotInstallDIR/etc/grid-security/vomsdir
mkdir -p /home/dirac/PilotInstallDIR/etc/grid-security/vomses

cp /ca/certs/pilot.pem /home/dirac/PilotInstallDIR/etc/grid-security/hostcert.pem
cp /ca/certs/pilot.key /home/dirac/PilotInstallDIR/etc/grid-security/hostkey.pem
cp /ca/certs/ca.cert.pem /home/dirac/PilotInstallDIR/etc/grid-security/certificates

touch /home/dirac/PilotInstallDIR/etc/grid-security/vomsdir/vomsdir
touch /home/dirac/PilotInstallDIR/etc/grid-security/vomses/vomses

cd /home/dirac/PilotInstallDIR

eval "${PILOT_DOWNLOAD_COMMAND}"

echo "${PILOT_JSON}" > pilot.json

if command -v python &> /dev/null; then
  py='python'
elif command -v python3 &> /dev/null; then
  py='python3'
elif command -v python2 &> /dev/null; then
  py='python2'
fi

more pilot.json | jq

# shellcheck disable=SC2086
$py ${PILOT_INSTALLATION_COMMAND}
