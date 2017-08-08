#!/bin/bash

  gsCerts=/etc/grid-security/certificates

  allF="/opt/dirac/etc/grid-security/crls.pem"
  copiedCAs=0
  invalidCAs=0
  echo "Copying revoked certificates into $allF"
  for cert in $gsCerts/*.r0
  do
     openssl crl -in ${cert} >> $allF.gen
     copiedCAs=`expr "${copiedCAs}" + "1"`
  done
  echo " + Copied ${copiedCAs} revoked certificates into $allF"
  mv $allF.gen $allF
