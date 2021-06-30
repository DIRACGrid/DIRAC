#!/usr/bin/env bash

usage() {
  echo Usage:
  echo "    install_site.sh [Options] ... CFG_file"
  echo
  echo "CFG_file - is the name of the installation configuration file which contains"
  echo "           all the instructions for the DIRAC installation. See DIRAC Administrator "
  echo "           Guide for the details"
  echo "Options:"
  echo "    -d, --debug    debug mode"
  echo "    -h, --help     print this"
  exit 1
}

while [ "${1}" ]
do
  case "${1}" in

  -h | --help )
    usage
    exit
  ;;

  -d | --debug )
    DEBUG='-o LogLevel=DEBUG'
  ;;

  * )
    installCfg=${1}
  ;;

  esac
  shift
done

if [[ -z "${installCfg}" ]]; then
  usage
  exit 1
fi

# Get the version of dirac-install requested - if none is requested, the version will come from integration
#
curl -L -o dirac-install https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py || exit
#
# define the target Dir
#
installDir=$(grep TargetPath "${installCfg}" | grep -v '#' | cut -d '=' -f 2 | sed -e 's/ //g')
#
mkdir -p "${installDir}" || exit
#

python dirac-install -t server "${installCfg}"
source "${installDir}"/bashrc
dirac-configure --cfg "${installCfg}" "$DEBUG"
dirac-setup-site "${DEBUG}"
