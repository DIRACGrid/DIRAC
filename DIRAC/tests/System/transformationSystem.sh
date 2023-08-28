#!/bin/bash

usage="$(basename "$0") needs -test_filter option to be set:
Example:
$(basename "$0") -test_filter [True,False]"

if [[ $# -ne 2 ]]; then
  echo "${usage}"
exit 1
fi

TestFilter="False"
if [[ "${1}" = "-test_filter" ]]; then
   if [[ "${2}" == "True" ]] || [[ "${2}" == "False" ]]; then
     TestFilter=${2}
   else
     echo "${usage}"
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
userdir=$( echo "${USER}" | cut -c 1)/${USER}
stamptime=$(date +%Y%m%d_%H%M%S)
stime=$(date +"%H%M%S")
tdate=$(date +"20%y-%m-%d")
ttime=$(date +"%R")
version=$(dirac-version)

if [[ -d "TransformationSystemTest" ]]; then
  echo "Removing TransformationSystemTest"
  rm -R TransformationSystemTest
fi
echo "Creating TransformationSystemTest"
mkdir -p TransformationSystemTest
directory=/dteam/diracCertification/Test/INIT/$version/$tdate/$stime
#selecting a random USER Storage Element
#SEs=$(dirac-dms-show-se-status |grep USER |grep -v 'Banned\|Degraded\|-2' | awk '{print $1}')
#get all SEs ending with -SE that are Active
SEs=$(dirac-dms-show-se-status | grep -e "-SE \|-disk" | grep -v 'RAL\|CESNET-SE\|Banned\|Probing\|Error\|-new' | awk '{print $1}')

x=0
for n in $SEs
do
  arrSE[x]=$n
  let x++
done
# random=$[ $RANDOM % $x ]
# randomSE=${arrSE[$random]}

# Create unique files and adding entry to the bkk"
echo ""
echo "Creating unique test files"
"${SCRIPT_DIR}/random_files_creator.sh" --Files=5 --Name="Test_Transformation_System_" --Path="${PWD}"/TransformationSystemTest/

# Add the random files to the transformation
echo ""
echo "Adding files to Storage Element ${randomSE}"
filesToUpload=$(ls TransformationSystemTest/)
for file in $filesToUpload
do
  random=$(( RANDOM % x ))
  randomSE=${arrSE[${random}]}
  echo "${directory}/${file} ./TransformationSystemTest/${file} ${randomSE}" >> TransformationSystemTest/LFNlist.txt
done

if [[ -e "LFNlistNew.txt" ]]; then
  rm LFNlistNew.txt
fi

while IFS= read -r line
do
  random=$(( RANDOM % x ))
  randomSE=${arrSE[${random}]}
  echo "${line} ${randomSE}"
done < TransformationSystemTest/LFNlist.txt >> ./LFNlistNew.txt

dirac-dms-add-file LFNlistNew.txt -ddd

cat TransformationSystemTest/LFNlist.txt | awk '{print $1}' | sort > ./LFNstoTS.txt


echo "Checking if files have been uploaded"
dirac-dms-lfn-replicas ./LFNstoTS.txt | grep "No such file"
# grep returns 1 if it cannot find anything, if we cannot find "No such file" we successfully uploaded all files
if [[ "${?}" -ne 1 ]]; then
    echo "Failed to upload all files, please check"
    exit 1
fi


echo ""
echo "Submitting test production"
if ! python "${SCRIPT_DIR}/dirac-test-production.py" -ddd "${directory}"  --UseFilter="${TestFilter}"; then
   exit 1
fi

transID=$(cat TransformationID)

if [[ ${TestFilter} == "False" ]]; then
  echo ""
  echo "Adding the files to the test production"
  if ! dirac-transformation-add-files "${transID}" LFNstoTS.txt; then
    exit 1
  fi
fi

echo ""
echo "Checking if the files have been added to the transformation"
dirac-transformation-get-files "${transID}" | sort > ./transLFNs.txt
if ! diff --ignore-space-change LFNstoTS.txt transLFNs.txt; then
  echo 'Error: files have not been  added to the transformation'
  exit 1
else
  echo 'Successful check'
fi

# ___ Use Ramdom SEs___
