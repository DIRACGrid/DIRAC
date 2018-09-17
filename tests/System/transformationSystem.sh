#!/bin/bash

usage="$(basename "$0") needs -test_filter option to be set:
Example:
$(basename "$0") -test_filter [true,false]"

if [ $# -ne 2 ]
then
  echo "$usage"
exit 1
fi

TestFilter="False"
if [ "$1" = "-test_filter" ]; then
   if [ "$2" == "True" ]||[ "$2" == "False" ]; then
     TestFilter=$2
   else
     echo "$usage"
     exit 1
   fi
fi

echo "dirac-proxy-init -g dirac_prod"
dirac-proxy-init -g dirac_prod
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "

#Values to be used
userdir=$( echo "$USER" |cut -c 1)/$USER
stamptime=$(date +%Y%m%d_%H%M%S)
stime=$(date +"%H%M%S")
tdate=$(date +"20%y-%m-%d")
ttime=$(date +"%R")
version=$(dirac-version)

if [ -d "TransformationSystemTest" ]; then
  echo "Removing TransformationSystemTest"
  rm -R TransformationSystemTest
fi
echo "Creating TransformationSystemTest"
mkdir -p TransformationSystemTest
directory=/lhcb/certification/Test/INIT/$version/$tdate/$stime
#selecting a random USER Storage Element
#SEs=$(dirac-dms-show-se-status |grep USER |grep -v 'Banned\|Degraded\|-2' | awk '{print $1}')
SEs=$(dirac-dms-show-se-status |grep BUFFER |grep -v 'Banned\|Degraded\|-new' | awk '{print $1}')

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
$DIRAC/DIRAC/tests/System/random_files_creator.sh --Files=5 --Name="Test_Transformation_System_" --Path=$PWD/TransformationSystemTest/

# Add the random files to the transformation
echo ""
echo "Adding files to Storage Element $randomSE"
filesToUpload=$(ls TransformationSystemTest/)
for file in $filesToUpload
do
  random=$[ $RANDOM % $x ]
  randomSE=${arrSE[$random]}
  echo "$directory/$file ./TransformationSystemTest/$file $randomSE" >> TransformationSystemTest/LFNlist.txt
done

if [ -e "LFNlistNew.txt" ]; then
  rm LFNlistNew.txt
fi

while IFS= read -r line
do
  random=$[ $RANDOM % $x ]
  randomSE=${arrSE[$random]}
  echo "$line $randomSE"
done < TransformationSystemTest/LFNlist.txt >> ./LFNlistNew.txt

dirac-dms-add-file LFNlistNew.txt -ddd

cat TransformationSystemTest/LFNlist.txt | awk '{print $1}' | sort > ./LFNstoTS.txt

echo ""
echo "Submitting test production"
python $DIRAC/DIRAC/tests/System/dirac-test-production.py -ddd $directory  --UseFilter=$TestFilter
if [ $? -ne 0 ]
then
   exit $?
fi

transID=`cat TransformationID`

if [ $TestFilter == "False" ]
then
  echo ""
  echo "Adding the files to the test production"
  dirac-transformation-add-files $transID LFNstoTS.txt

  if [ $? -ne 0 ]
  then
    exit $?
  fi
fi

echo ""
echo "Checking if the files have been added to the transformation"
dirac-transformation-get-files $transID | sort > ./transLFNs.txt
diff --ignore-space-change LFNstoTS.txt transLFNs.txt
if [ $? -ne 0 ]
then
  echo 'Error: files have not been  added to the transformation'
  exit $?
else
  echo 'Successful check'
fi

# ___ Use Ramdom SEs___
