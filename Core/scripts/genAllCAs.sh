#!/bin/bash

gsCerts="/etc/grid-security/certificates"

allF="/opt/dirac/etc/grid-security/cas.pem"
 
narg="${#}"
nar=0
while [[ ${nar} -lt "${narg}" ]]; do
  case "${1}" in
    -h )
      echo "Usage : $0 [-h] [-i] [-o] "
      echo "      -h : provide this help"
      echo "      -i : to use the directory as input"
      echo "      -o : to use the directory as output"      
      exit 0
      ;;
    -i )
      shift
      gsCerts=${1}
      if [[ ! -d "${gsCerts}" ]]; then
        echo "${gsCerts} does not exist"
        exit
      fi
      shift
      ;;
    -o )
      shift
      allF=${1}
      if [[ ! -d "${allF}" ]]; then
        mkdir "${allF}"
      fi
      allF=${allF}/cas.pem
      shift
      ;;
    *)
      echo 'Supply a valid option' >&2
      exit 1
      ;;
  esac
  if [[ $nar -lt "${narg}" ]]; then
    nar=$(( ++nar ))
  else
    break
  fi
done

copiedCAs=0
invalidCAs=0
echo "Copying CA certificates (${gsCerts}) into ${allF}"

for cert in "${gsCerts}"/*.0
do
  ossle="openssl x509 -noout -in ${cert}"
  if ${ossle} -checkend 3600; then
    openssl x509 -in "${cert}" >> "${allF}".gen
    copiedCAs=$(( "${copiedCAs}" + "1"))
  else
    echo " - CA ${cert} is expired"
    invalidCAs=$(( "${invalidCAs}" + "1" ))
  fi
done
echo " + There are ${invalidCAs} invalid CA certificates in ${gsCerts}"
echo " + Copied ${copiedCAs} CA certificates into ${allF}"
mv "${allF}".gen "${allF}"
