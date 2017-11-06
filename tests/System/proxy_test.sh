#!/bin/bash

echo ""
echo "Welcome to proxy certificate handling test script. Follow onscreen instructions and enjoy the ride!"
echo "========================================================================="
echo ""
echo "Running dirac-proxy-init with upload to ProxyManager, you need to have cert/key in ~/.globus, you will be asked for a key password."
echo "Expect: info about generated proxy and that it was uploaded"
echo ""
dirac-proxy-init --upload --VOMS
echo "========================================================================="

echo ""
echo "Running dirac-proxy-info"
echo "Expect: basic info about proxy"
echo "IMPORTANT: VOMS info should be present!"
echo ""
dirac-proxy-info
echo "========================================================================="


echo ""
echo "Running dirac-proxy-get-uploaded-info"
echo "Expect: Info containing your DN and username"
echo ""
dirac-proxy-get-uploaded-info
echo "========================================================================="

echo ""
echo "Creating tmp file to upload"
echo "file for upload test" > cert_test_file_upload.txt
echo "Uploading file with dirac-dms-add-file"
echo "WARNING: This is LHCb specific!"
dir=$( echo "$USER" |cut -c 1)/$USER
if dirac-dms-add-file /lhcb/user/$dir/cert_test_file_upload.txt ./cert_test_file_upload.txt CNAF-USER | grep -Fq "Successfully uploaded file to CNAF-USER";
then
  echo "Upload successfull!"
else
  echo "Something went wrong with uploading file!"
  exit 1
fi
echo "========================================================================="

echo ""
echo "Checking if file uploaded"
read -a output <<< `dirac-dms-user-lfns`
if grep -Fq "cert_test_file_upload.txt" ${output[${#output[@]}-1]};
then
  echo "File uploaded successfully!"
else
  echo "Something went wrong - file is not on user LFNs list!"
  exit 1
fi
echo "========================================================================="

echo ""
echo "Deleting uploaded file"
dirac-dms-remove-files /lhcb/user/$dir/cert_test_file_upload.txt
echo "========================================================================="
