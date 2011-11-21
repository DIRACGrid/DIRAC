#!/bin/bash
###############################################################
#
#
#
###############################################################
#
function usage {
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


while [ $1 ]
do
  case $1 in

  -h | --help )
    usage
    exit
  ;;
  -d | --debug )
    DEBUG='-o LogLevel=DEBUG' 
# -v | --version )
#    switch=$1
#    shift
#    [ $1 ] || error_exit "Switch $switch requires a argument"
#    DIRACVERSION=$1
  ;;
  * )
    installCfg=$1
  ;;

  esac
  shift
done

if [ -z "$installCfg" ]; then
  usage
  exit 1
fi

# Get the latest version of dirac-install
#
#wget -O dirac-install 'http://svnweb.cern.ch/guest/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-install.py' | exit
wget --no-check-certificate -O dirac-install 'https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py' || exit
#
# define the target Dir
#
installDir=`grep TargetPath $installCfg | grep -v '#' | cut -d '=' -f 2 | sed -e 's/ //g'`
#
mkdir -p $installDir || exit
#
python dirac-install -t server $installCfg
source $installDir/bashrc
dirac-configure $installCfg $DEBUG 
dirac-setup-site $DEBUG