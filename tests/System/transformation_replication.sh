#!/bin/bash

#Upload random files to one storage element, and then create a replication transformation to replicate them to another
#storage element

usage="$(basename "$0") needs -test_filter option to be set:
Example:
$(basename "$0") -test_filter [True,False]"

if [[ $# -ne 2 ]]; then
  echo "$usage"
exit 1
fi

TestFilter="False"
if [[ "$1" = "-test_filter" ]]; then
   if [[ "$2" == "True" ]] || [[ "$2" == "False" ]]; then
     TestFilter=$2
   else
     echo "$usage"
     exit 1
   fi
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "dirac-login dirac_prod"
if ! dirac-login dirac_prod; then
   exit 1
fi

echo " "

#Values to be used
stime=$(date +"%H%M%S")
tdate=$(date +"20%y-%m-%d")
version=$(dirac-version)

if [[ -d "TransformationSystemTest" ]]; then
  echo "Removing TransformationSystemTest"
  rm -R TransformationSystemTest
fi

echo "Creating TransformationSystemTest"
mkdir -p TransformationSystemTest
directory=/dteam/diracCertification/Test/INIT/$version/$tdate/$stime/replication
#get all SEs ending with -SE that are Active, randomize, turn into array
SEs=( $(dirac-dms-show-se-status | grep -e "-SE \|-disk " | grep -v 'RAL\|CESNET\|Banned\|Probing\|Error\|-new' | awk '{print $1}' | sort -R | xargs) )

SOURCE_SE=${SEs[0]}
TARGET_SE=${SEs[1]}

echo "Source: ${SOURCE_SE}"
echo "Target: ${TARGET_SE}"

# Create unique files"
echo ""
echo "Creating unique test files"
"${SCRIPT_DIR}/random_files_creator.sh" --Files=5 --Name="Test_Transformation_System_" --Path="$PWD"/TransformationSystemTest

# Add the random files to the transformation
echo ""
echo "Adding files to Storage Element ${SOURCE_SE}"
filesToUpload=$(ls TransformationSystemTest/)
for file in $filesToUpload
do
  echo "$directory/$file ./TransformationSystemTest/$file ${SOURCE_SE}" >> TransformationSystemTest/LFNlist.txt
done

echo "Uploading files, see TransformationSystemTest/upload.log"
dirac-dms-add-file TransformationSystemTest/LFNlist.txt -ddd &> TransformationSystemTest/upload.log

cat TransformationSystemTest/LFNlist.txt | awk '{print $1}' | sort > ./LFNstoTS.txt

echo "Checking if files have been uploaded..."
dirac-dms-lfn-replicas ./LFNstoTS.txt | grep "No such file"
# grep returns 1 if it cannot find anything, if we cannot find "No such file" we successfully uploaded all files
if [[ "${?}" -ne 1 ]]; then
    echo "Failed to upload all files, please check"
    exit 1
fi
echo "...files successfully uploaded"

echo ""
echo "Submitting test production"
if ! dirac-transformation-replication 0 "${TARGET_SE}" -G 2 -ddd -N replication_"${version}"_"${tdate}"_"${stime}" --Enable | tee TransformationSystemTest/trans.log; then
    echo "Failed to create transformation"
    exit 1
fi

transID=$(grep "Created transformation" TransformationSystemTest/trans.log | sed "s/.*Created transformation //")
echo "Adding files to transformation ${transID}"
if [[ $TestFilter == "False" ]]; then
  echo ""
  echo "Adding the files to the test production"
  if ! dirac-transformation-add-files "$transID" LFNstoTS.txt; then
    exit 1
  fi
fi

echo ""
echo "Checking if the files have been added to the transformation"
dirac-transformation-get-files "${transID}" | sort > ./transLFNs.txt
if diff --ignore-space-change LFNstoTS.txt transLFNs.txt!
then
  echo 'Error: files have not been  added to the transformation'
  exit 1
else
  echo 'Successful check'
fi
