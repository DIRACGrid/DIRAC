#!/bin/bash

# This is a generic list of commands run from a client for testing/certification purposes.
#
# Submitter should follow through the logs

echo " "
echo " "
echo " ########################## RMS #############################"
echo " "
echo " "

echo "dirac-login dteam_user"
if ! dirac-login dteam_user; then
   exit 1
fi

dirac_user=$( dirac-proxy-info | awk '/^username / {print $3}' )
userdir="/dteam/user/$( echo "$dirac_user" | cut -c 1)/$dirac_user"
lfn="$userdir"/Dirac_Scripts_Test_Directory/resolv.conf"$( date +%s )"

echo "dirac-dms-put-and-register-request"
if ! resPut=$(dirac-dms-put-and-register-request "$dirac_user""$( date +%s )" "$lfn" /etc/resolv.conf RAL-SE); then
   echo "ERROR: could not dirac-dms-put-and-register-request"
   exit 1
fi
echo "$resPut"

reqID=$(echo "$resPut" | awk '/^Request /{print $2}' |  sed -r "s/'//g")

echo "## --> Now checking if it's done"

resReq=notDone
while [ "$resReq" != Done ]
do
   sleep 5
   resReq=$(dirac-rms-request "$reqID" | awk '/^Request / {print $4}' | cut -d '=' -f 2 | sed -r "s/'//g")
   echo "$resReq"
done

echo "## verify that the file is uploaded with dirac-dms-lfn-replicas"
if ! resRepl=$(dirac-dms-lfn-replicas "$lfn"); then
   echo "ERROR: could not dirac-dms-lfn-replicas"
   exit 1
fi

resSite=$(echo "$resRepl" | awk '/dteam/{print $2}')
if [ "$resSite" != RAL-SE ]; then
   echo "ERROR: GOT $resSite instead of RAL-SE"
   exit 1
fi

echo "## All good, so now we remove it"
if resRemove=$(! dirac-dms-create-removal-request All "$lfn"); then
   echo "ERROR: could not dirac-dms-create-removal-request"
   exit 1
fi
reqID=$(echo "$resRemove" | awk '/^Request /{print $2}' |  sed -r "s/'//g")

echo "## --> Now checking if it's done"

resReq=notDone
while [ "$resReq" != Done ]
do
   sleep 5
   resReq=$(dirac-rms-request "$reqID" | awk '/^Request / {print $4}' | cut -d '=' -f 2 | sed -r "s/'//g")
   echo "$resReq"
done

echo "## verify that the file is removed with dirac-dms-lfn-replicas"

if ! resRepl=$(dirac-dms-lfn-replicas "$lfn"); then
   echo "ERROR: could not dirac-dms-lfn-replicas"
   exit 1
fi

resSite=$(echo "$resRepl" | awk '/dteam/{print $2}')
if [ "$resSite" == RAL-SE ]; then
   echo "ERROR: lfn still at RAL-SE"
   exit 1
fi
