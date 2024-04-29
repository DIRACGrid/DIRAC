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
cd /home/dirac/PilotInstallDIR
mkdir -p etc/grid-security/vomsdir
mkdir -p etc/grid-security/vomses
touch etc/grid-security/vomsdir/vomsdir
touch etc/grid-security/vomses/vomses

eval "${PILOT_DOWNLOAD_COMMAND}"

echo "${PILOT_JSON}" > pilot.json

if command -v python &> /dev/null; then
  py='python'
elif command -v python3 &> /dev/null; then
  py='python3'
elif command -v python2 &> /dev/null; then
  py='python2'
fi

more pilot.json

# shellcheck disable=SC2086
$py ${PILOT_INSTALLATION_COMMAND}
