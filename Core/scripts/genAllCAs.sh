#!/bin/bash

 gsCerts=/etc/grid-security/certificates

 allF="/opt/dirac/etc/grid-security/cas.pem"
 copiedCAs=0
 invalidCAs=0
 echo "Copying CA certificates into $allF"
 for cert in $gsCerts/*.0
 do
   ossle="openssl x509 -noout -in ${cert}"
   if ${ossle} -checkend 3600; then
         openssl x509 -in ${cert} >> $allF.gen
         copiedCAs=`expr "${copiedCAs}" + "1"`
   else
     echo " - CA ${cert} is expired"
     invalidCAs=`expr "${invalidCAs}" + "1"`
   fi
 done
 echo " + There are ${invalidCAs} invalid CA certificates in $gsCerts"
 echo " + Copied ${copiedCAs} CA certificates into $allF"
 mv $allF.gen $allF
