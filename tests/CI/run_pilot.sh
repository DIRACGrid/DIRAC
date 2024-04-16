#!/bin/bash
#
#   Executable script to run the DIRAC Pilot
#
#.....................................................
# set -euo pipefail
set -eo pipefail
# IFS=$'\n\t'
set -x

TESTCODE=$PWD/LocalRepo/ALTERNATIVE_MODULES/
PILOTINSTALLDIR=$PWD/PilotInstallDIR/

source CONFIG

# Creating "the worker node"
cd "${PILOTINSTALLDIR}"
mkdir -p etc/grid-security/vomsdir
mkdir -p etc/grid-security/vomses
touch etc/grid-security/vomsdir/vomsdir
touch etc/grid-security/vomses/vomses

# Get the pilot code
git clone --single-branch -b master https://github.com/DIRACGrid/Pilot.git
mv Pilot/Pilot/*.py .
rm -rf Pilot

# shellcheck disable=SC2034
CSURL=https://$SERVER_HOST:9135/Configuration/Server

echo "
{
	\"timestamp\": \"2023-02-13T14:34:26.725499\",
	\"CEs\": {
		\"jenkins.cern.ch\": {
			\"Site\": \"DIRAC.Jenkins.ch\",
			\"GridCEType\": \"TEST-FULL\"
		}
	},
	\"Defaults\": {
		\"Pilot\": {
			\"RemoteLogging\": \"False\",
			\"Version\": \"integration\",
			\"Commands\": {
				\"TEST-FULL\": \"CheckWorkerNode, InstallDIRAC, ConfigureBasics, RegisterPilot, CheckCECapabilities, CheckWNCapabilities, ConfigureSite, ConfigureArchitecture, ConfigureCPURequirements, LaunchAgent\"
			}
		}
	},
	\"vo\": {
		\"Pilot\": {
			\"CheckVersion\": \"False\",
			\"pilotFileServer\": \"should_not_matter\",
			\"pilotRepoBranch\": \"should_not_matter\",
			\"pilotRepo\": \"https://github.com/should_not_matter/Pilot.git\",
			\"GenericPilotGroup\": \"dirac_user\",
			\"GenericPilotDN\": \"/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser\",
			\"PilotLogLevel\": \"DEBUG\"
		}
	},
	\"ConfigurationServers\": [
		\"${CSURL}\"
	]
}
" > pilot.json

if command -v python &> /dev/null; then
  py='python'
elif command -v python3 &> /dev/null; then
  py='python3'
elif command -v python2 &> /dev/null; then
  py='python2'
fi

pilotUUID="${GITHUB_JOB//-/}""$(shuf -i 2000-65000 -n 1)"
pilotUUID=$(echo "$pilotUUID" | rev | cut -c 1-32 | rev)

$py dirac-pilot.py --modules "${TESTCODE}/DIRAC" -M 1 -S dirac-JenkinsSetup -N jenkins.cern.ch -Q jenkins-queue_not_important -n DIRAC.Jenkins.ch --pilotUUID="${pilotUUID}" --cert --certLocation="${PILOTINSTALLDIR}"/etc/grid-security --CVMFS_locations="${PILOTINSTALLDIR}" -o diracInstallOnly --wnVO=vo --debug
