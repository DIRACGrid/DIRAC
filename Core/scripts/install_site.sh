#!/usr/bin/env bash

function usage {
  echo Usage:
  echo "    install_site.sh [Options] ... CFG_file"
  echo
  echo "CFG_file - is the name of the installation configuration file which contains"
  echo "           all the instructions for the DIRAC installation. See DIRAC Administrator "
  echo "           Guide for the details"
  echo "Options:"
  echo "    -v, --version  for a specific version"
  echo "    -d, --debug    debug mode"
  echo "    -h, --help     print this"
  exit 1
}

DIRACVERSION='integration'

while [ "$1" ]
do
  case $1 in

  -h | --help )
    usage
    exit
  ;;

  -d | --debug )
    DEBUG='-o LogLevel=DEBUG'
  ;;

  -o | --dirac-os )
    USE_DIRACOS='--dirac-os'
  ;;

  -v | --version )
    switch=$1
    shift
    [ "$1" ] || error_exit "Switch $switch requires a argument"
    DIRACVERSION=$1
  ;;

  * )
    installCfg=$1
  ;;

  esac
  shift
done

if [[ -z "$installCfg" ]]; then
  usage
  exit 1
fi

# Get the version of dirac-install requested - if none is requested, the version will come from integration
#
wget --no-check-certificate -O dirac-install "https://github.com/DIRACGrid/DIRAC/raw/$DIRACVERSION/Core/scripts/dirac-install.py" || exit
#
# define the target Dir
#
installDir=$(grep TargetPath $installCfg | grep -v '#' | cut -d '=' -f 2 | sed -e 's/ //g')
#
mkdir -p "$installDir" || exit
#

python dirac-install -t server "$USE_DIRACOS" "$installCfg"
source "$installDir"/bashrc
dirac-configure "$installCfg" "$DEBUG"
dirac-configure -o /Operations/Defaults/Pilot/UpdatePilotCStoJSONFile=False -FDMH "$DEBUG"
dirac-setup-site "$DEBUG"
