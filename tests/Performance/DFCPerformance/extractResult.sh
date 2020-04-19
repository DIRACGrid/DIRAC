#!/bin/bash

# The name of the server where the DFC service is running
machineName='volhcb38'

if [[ $# -ne 1 ]];
then
  echo "usage: extractResult.sh <jobName>"
  exit 1
fi

dir=$1

if [[ ! -d "$dir" ]]; then
  echo "$dir does not exist"
  exit 1
fi

for type in "remove" "insert" "list";
do
  for i in $(ls "$dir"/Done); do cat $dir/Done/$i/time.txt | grep $type | grep -ivE "($machineName|timeout)" ; done | sort > $dir/"$type"_good.txt
  for i in $(ls "$dir"/Done); do cat $dir/Done/$i/time.txt | grep $type | grep -iE "($machineName|timeout)" ; done | sort > $dir/"$type"_timeout.txt
done
wc -l $dir/*.txt
