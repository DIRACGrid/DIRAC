#!/bin/bash

gsCerts="/etc/grid-security/certificates"

allF="/opt/dirac/etc/grid-security/crls.pem"

narg="${#}"
nar=0
while [[ "${nar}" -lt "${narg}" ]]; do
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
      if [[ ! -d "$gsCerts" ]]; then
        echo "$gsCerts does not exist"
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
      allF=${allF}/crls.pem
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
echo "Copying revoked certificates (${gsCerts}) into ${allF}"
for cert in "${gsCerts}"/*.r0
do
   openssl crl -in "${cert}" >> "${allF}".gen
   copiedCAs=$(( "${copiedCAs}" + "1" ))
done
echo " + Copied ${copiedCAs} revoked certificates into ${allF}"
mv "${allF}".gen "${allF}"
