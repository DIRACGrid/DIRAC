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

echo "dirac-dms-put-and-register-request"
if ! dirac-dms-put-and-register-request "$dirac_user""$( date +%s )" "$userdir"/Dirac_Scripts_Test_Directory/resolv.conf /etc/resolv.conf RAL-SE; then
   exit 1
fi

echo "## --> This should be treated rather fast, we anyway wait 60 seconds"
sleep 60

echo "verify that the file is uploaded with dirac-dms-lfn-replicas"
if ! dirac-dms-lfn-replicas "$userdir"/Dirac_Scripts_Test_Directory/resolv.conf; then
   exit 1
fi

echo "## Now remove it"
if ! dirac-dms-create-removal-request All "$userdir"/Dirac_Scripts_Test_Directory/resolv.conf; then
   exit 1
fi

echo "## --> This should be treated rather fast, we anyway wait 60 seconds"
sleep 60

echo "verify that the file is removed with dirac-dms-lfn-replicas"
if ! dirac-dms-lfn-replicas "$userdir"/Dirac_Scripts_Test_Directory/resolv.conf; then
   exit 1
fi
