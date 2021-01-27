#!/bin/bash
##################################################################################
#
# $Id$
#
# dirac-cert-convert.sh script converts the user certificate in the p12 format
# into a standard .globus usercert.pem and userkey.pem files
#
# Author: Vanessa Hamar
# Last modified: 25.04.2010
#
##################################################################################

usage() {
  echo Usage:
  echo "    " dirac-cert-convert.sh CERT_FILE_NAME.p12
  exit 1
}

if [[ $# = 0 ]]; then
  echo User Certificate P12 file is not given.
  usage
fi

export OPENSSL_CONF="${HOME}"/.globus
GLOBUS="${HOME}"/.globus
USERCERT_P12_ORIG=$1
USERCERT_P12="${GLOBUS}"/$(basename "${USERCERT_P12_ORIG}")
USERCERT_PEM="${GLOBUS}"/usercert.pem
USERKEY_PEM="${GLOBUS}"/userkey.pem
OPENSSL=$(command -v openssl)
DATE=$(/bin/date +%F-%H:%M)

if [[ ! -f "${USERCERT_P12_ORIG}" ]]; then
  echo file "${USERCERT_P12_ORIG}" does not exist
  usage
fi

if [[ ! -d "${GLOBUS}" ]]; then
  echo "Creating globus directory"
  mkdir "${GLOBUS}"
fi
if [[ -f "${USERCERT_P12}" ]]; then
  echo "Back up ${USERCERT_P12} file"
  cp "${USERCERT_P12}" "${USERCERT_P12}"."$DATE"
fi
cp "${USERCERT_P12_ORIG}" "${USERCERT_P12}"

echo 'Converting p12 key to pem format'
if [[ -f "${USERKEY_PEM}" ]]; then
  echo "Back up ${USERKEY_PEM} file"
  mv "${USERKEY_PEM}" "${USERKEY_PEM}"."$DATE"
fi
while [[ ! -s "${USERKEY_PEM}" ]]; do
 $OPENSSL pkcs12 -nocerts -in "${USERCERT_P12}" -out "${USERKEY_PEM}"
done

echo 'Converting p12 certificate to pem format'
if [[ -f "${USERCERT_PEM}" ]]; then
  echo "Back up ${USERCERT_PEM} file"
  mv "${USERCERT_PEM}" "${USERCERT_PEM}"."$DATE"
fi
while [[ ! -s "${USERCERT_PEM}" ]]; do
  $OPENSSL pkcs12 -clcerts -nokeys -in "${USERCERT_P12}" -out "${USERCERT_PEM}"
done

chmod 400 "${USERKEY_PEM}"
chmod 644 "${USERCERT_PEM}"
echo 'Information about your certificate: '
$OPENSSL x509 -in "${USERCERT_PEM}" -noout -subject
$OPENSSL x509 -in "${USERCERT_PEM}" -noout -issuer
echo 'Done'
