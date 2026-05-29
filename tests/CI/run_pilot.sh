#!/bin/bash
#
#   Executable script to run the DIRAC Pilot
#
#.....................................................
# set -euo pipefail
set -eo pipefail
# IFS=$'\n\t'
set -x

echo "Starting run_pilot.sh"
source CONFIG

if [[ -n "${INSTALLATION_BRANCH}" ]]; then
  # Do not run this
  echo "Not running the DIRAC Pilot"
  exit
fi

# Creating "the worker node"
mkdir -p /home/dirac/etc/grid-security/certificates
mkdir -p /home/dirac/etc/grid-security/vomsdir
mkdir -p /home/dirac/etc/grid-security/vomses

cp /ca/certs/ca.cert.pem /home/dirac/etc/grid-security/certificates
cp /ca/certs/ca.crl.pem /home/dirac/etc/grid-security/certificates
touch /home/dirac/etc/grid-security/vomsdir/vomsdir
touch /home/dirac/etc/grid-security/vomses/vomses
# Generate the hash link file required by openSSL to index CA certificates
caHash=$(openssl x509 -in /home/dirac/etc/grid-security/certificates/ca.cert.pem -noout -hash)
ln -s ca.cert.pem "/home/dirac/etc/grid-security/certificates/$caHash.0"
tar --create --file "/home/dirac/etc/grid-security/certificates/$caHash.r0" --gzip /home/dirac/etc/grid-security/certificates/ca.crl.pem

# Copy over the pilot proxy
cp /ca/certs/pilot_proxy /tmp/x509up_u$UID

eval "${PILOT_DOWNLOAD_COMMAND}"

echo "${PILOT_JSON}" >pilot.json
jq <pilot.json

if command -v python &>/dev/null; then
  py='python'
elif command -v python3 &>/dev/null; then
  py='python3'
elif command -v python2 &>/dev/null; then
  py='python2'
fi

# shellcheck disable=SC2086
$py ${PILOT_INSTALLATION_COMMAND}
