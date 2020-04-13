#!/bin/bash


helpmessage="\n\nScript usage:\n\n
\t\t ./random_files_creator.sh <--option=value> \n\n
e.g.\t  ./random_files_creator.sh --Files=12 --Name='Pippo_files_' \n
e.g.\t  ./random_files_creator.sh --Path=/path/to/directory/i/want/ \n
\n
Options:
\n\n
  -f=\t  --Files=\t\t:Insert number of files needed --Files=20 (default = 10)\n
  -n=\t  --Name=\t\t:Insert fale name that means something to you  --Name='Pippo_files_'\n
  -p=\t  --Path=\t\t:Insert the path that you wish to use --Path=<path_to_dir>\n
  -h\t   --help\t\t\t:Shows this help\n\n
\n\n
\t\t* If no values inserted it will use defaults values:\n
\t\t\t Number of Files => 10\n
\t\t\t File Names      => random_content_<random value>_00x.init\n
\t\t\t Path            => /tmp/<username>/tmp.<random value>/ \n
\n\n\n

"

#Default values
numberOfFiles=10
filesName="random_content_"
if [[ $DIRAC ]]; then
  diracDir=$DIRAC
else
  diracDir=$PWD
fi

# Parsing arguments
if [[ $# -gt 0 ]]; then
  for i in "$@"
    do
      case $i in

        -h|--help|-?)
        echo -e $helpmessage
        exit 0
        ;;

        -f=*|--Files=*)
        numberOfFiles="${i#*=}"
        shift # past argument=value
        ;;

        -n=*|--Name=*)
        filesName="${i#*=}"
        shift # past argument=value
        ;;

        -p=*|--Path=*)
        temporaryPath="${i#*=}"
	if [[ ! -d "$temporaryPath" ]]; then
          mkdir -p $temporaryPath
        fi
        shift # past argument=value
        ;;

        *)
        echo -e $helpmessage
        exit 0
            # unknown option
        ;;
      esac
    done
fi

# Default temporary path
if [[ -z "$temporaryPath" ]]; then
  temporaryPath=$(mktemp -d)
fi

# Move to a tmp directory
cd $temporaryPath
if [[ $? -ne 0 ]]; then
  echo $(tput setaf 1)"ERROR: cannot change to directory: " $temporaryPath$(tput sgr 0)
  exit $?
fi

echo ""
echo "Random files created in: "
echo $(tput setaf 2)$temporaryPath$(tput sgr 0)
echo ""


# array of fileNames
fileNames=()
for n in $(eval echo "{1..$numberOfFiles}")
do
  fileNames+=($(date +"20%y%m%d")_$(date +"%H%M%S")_$( printf %03d "$n" ))
done

for n in $(eval echo "{1..$numberOfFiles}")
do
  randomx=$(( (RANDOM % 12) +1 )) # a random value between 1 and 12
  echo ""
  echo $(tput setaf 2)$temporaryPath"/"$filesName${fileNames[$n-1]}".init"$(tput sgr 0)
  dd if=/dev/urandom of=$filesName${fileNames[$n-1]}.init bs=1M count=$randomx
done
