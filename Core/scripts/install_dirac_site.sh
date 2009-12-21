#!/bin/bash
########################################################################
# $Header: /local/reps/dirac/DIRAC3/scripts/install_dirac_site.sh,v 1.21 2009/09/18 09:01:47 rgracian Exp $
# File:    install_dirac_site.sh
# Author : Florian Feldhaus, Ricardo Graciani
########################################################################
#
# Some Default Variables that can be over written with proper arguments
#
# User that is allowed to execute the script (default: dirac)
DIRACUSER=dirac
#
# Location of the DIRAC site installation (default: /opt/dirac)
DESTDIR=/opt/dirac
#
# Your submission Queue for DIRAC jobs (batch is the default queue of Torque)
Queue=default
#
# Log Level for installed Agents
LOGLEVEL=VERBOSE
#
# CE Type to be used (currently it also corresponds to the CE Name)
CEType=Inprocess

#
# Some Variable with no default that must be set by proper arguments
#
# The name of your site given to you by the DIRAC admins
SiteName=""
#
# Your execution Queue for DIRAC jobs (in case there submission is a routing queue)
ExecQueue=""
#
# Dirac version to install
DIRACVERSION=""
#
# Dirac Architecture as determined with the platform.py script (default: DIRAC Native Platform)
DIRACARCH=""
#
# If you have a proxy server which is able to handle big files up to 1GB config it here
# example: HttpProxy=http://yourproxy.yourdomain.tld:3128
HttpProxy=""
#
# The path to your shared area (default: /opt/shared)
SharedArea=""

usage(){
echo Usage: $0
echo "  -n --name SiteName      Set Site Name (mandatory)"
echo "  -v --version Version    DIRAC Version to install (mandatory)"
echo "  -L --LogLevel           LogLevel for installed Components"
echo "  -C --CE CEType          The CE Type to be installed: Torque, InProcess. (default: InProcess )"
echo "  -P --path Path          Site Installation PATH (default: $DESTDIR)"
echo "  -Q --Queue Queue        Batch System submit Queue (default: $Queue)"
echo "  -E --ExecQueue Queue    Batch System executing Queue (default: same as Queue)"
echo "  -U --User UserName      User executing the script (default: $DIRACUSER)"
echo "  -p --platform Platform  Use Platform instead of local one"
echo "  -s --shared SharedArea  Set and use SharedArea"
echo "  -h --help               Print this"
exit 0
}

error_exit(){
echo
echo ERROR: $1
echo
exit 1
}

#
# Python Version to use
DIRACPYTHON=25
# LCG tar version
LCGVER="2009-08-13"
# Directories to create at DESTDIR
DIRACDIRS="startup runit data work control"
#
# Dirac Setup (e.g. LHCb-Production or LHCb-Development)
DIRACSETUP=LHCb-Development
DIRACINSTANCE=Development
#
# Needs to be exported for the install_agent.sh script to make use of it.
export LOGLEVEL
#
while [ $1 ]
do
  case $1 in

  -h | --help )
    usage
  ;;
  -L | --LogLevel )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    LOGLEVEL=$1
  ;;
  -n | --name )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    SiteName=$1
  ;;
  -p | --platform )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    DIRACARCH=$1
  ;;
  -P | --path )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    DESTDIR=$1
  ;;
  -C | --CE )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    CEType=$1
  ;;
  -Q | --Queue )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    Queue=$1
  ;;
  -E | --ExecQueue )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    ExecQueue=$1
  ;;
  -s | --shared )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    SharedArea=$1
  ;;
  -U | --User )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    DIRACUSER=$1
  ;;
  -v | --version )
    switch=$1
    shift
    [ $1 ] || error_exit "Switch $switch requires a argument"
    DIRACVERSION=$1
  ;;
  * )
    error_exit "Unkown argument switch $1"
  ;;

  esac
  shift
done

[ -z "$SiteName" -o  -z "$DIRACVERSION" ] && error_exit "Missing mandatory argument, use -h for help"
[ -z "$ExecQueue" ] && ExecQueue=$Queue

#
# check we are the right user
[ "$USER" == "$DIRACUSER" ] || error_exit "$0 should be run by user: $DIRACUSER"
#
# check $DESTDIR is available
mkdir -p $DESTDIR || error_exit "Install directory $DESTDIR not available"

CURDIR=`dirname $0`
CURDIR=`cd $CURDIR; pwd -P`

ROOT=$DESTDIR/pro

echo
echo "Installing under $DESTDIR"
echo
echo "Installing DIRAC under $ROOT"
echo

if [ ! -d $DESTDIR/etc/grid-security/certificates ]; then
  mkdir -p $DESTDIR/etc/grid-security/certificates || error_exit "Can not create directory $DESTDIR/etc/grid-security/certificates"
fi
if [ ! -e $DESTDIR/etc/dirac.cfg ] ; then
  cat >> $DESTDIR/etc/dirac.cfg << EOF || error_exit "Can not create file $DESTDIR/etc/dirac.cfg"
DIRAC
{
  Setup = $DIRACSETUP
  Security
  {
    CertFile = $DESTDIR/etc/grid-security/hostcert.pem
    KeyFile = $DESTDIR/etc/grid-security/hostkey.pem
  }
}
EOF
fi

for dir in $DIRACDIRS ; do
  if [ ! -d $DESTDIR/$dir ]; then
    mkdir -p $DESTDIR/$dir || error_exit "Can not create directory $DESTDIR/$dir"
  fi
done

# give an unique name to dest directory
# VERDIR
VERDIR=$DESTDIR/versions/${DIRACVERSION}-`date +"%s"`
mkdir -p $VERDIR   || error_exit "Can not create directory $VERDIR"
for dir in etc $DIRACDIRS ; do
  ln -s ../../$dir $VERDIR   || error_exit "Can not create link $VERDIR/$dir -> ../../$dir"
done

# to make sure we do not use DIRAC python
dir=`echo $DESTDIR/pro/$DIRACARCH/bin | sed 's/\//\\\\\//g'`
PATH=`echo $PATH | sed "s/$dir://"`

Install_Options="-t server -r $DIRACVERSION -P $VERDIR -i $DIRACPYTHON -g $LCGVER "
[ $EXTVERSION ] && Install_Options="$Install_Options -e $EXTVERSION"
[ $DIRACARCH ] && Install_Options="$Install_Options -p $DIRACARCH"
[ "$LOGLEVEL" == "DEBUG" ] && Install_Options="$Install_Options -d"

python $CURDIR/dirac-install.py $Install_Options || error_exit "Failed DIRAC installation"
# "-o /LocalSite/Root=$ROOT -o /LocalSite/Site=$SiteName -o /LocalSite/ResourceDict/Site=$SiteName -o /DIRAC/Security/UseServerCertificate=yes"
#
# Create pro and old links
old=$DESTDIR/old
pro=$DESTDIR/pro
[ -L $old ] && rm $old
[ -e $old ] && error_exit "Fail to remove link $old"
[ -L $pro ] && mv $pro $old
[ -e $pro ] && error_exit "Fail to rename $pro $old"
ln -s $VERDIR $pro || error_exit "Fail to create link $pro -> $VERDIR"
#
# Retrive last version of CA's
#
[ $DIRACARCH ] || DIRACARCH=`$DESTDIR/pro/scripts/dirac-platform`
export DIRACPLAT=$DIRACARCH
$VERDIR/scripts/dirac-admin-get-CAs

#
# Create bin link
ln -sf pro/$DIRACARCH/bin $DESTDIR/bin

chmod +x $DESTDIR/pro/scripts/install_bashrc.sh
$DESTDIR/pro/scripts/install_bashrc.sh    $DESTDIR $DIRACVERSION $DIRACARCH python$DIRACPYTHON 2>&1 1>/dev/null || error_exit "Could not create $DESTDIR/bashrc"

##
## Compile all python files .py -> .pyc, .pyo
##
cmd="from compileall import compile_dir ; compile_dir('"$DESTDIR/pro"', force=1, quiet=True )"
$DESTDIR/pro/$DIRACARCH/bin/python -c "$cmd" 1> /dev/null     || error_exit "Fail to compile .pyc files"
$DESTDIR/pro/$DIRACARCH/bin/python -O -c "$cmd" 1> /dev/null  || error_exit "Fail to compile .pyo files"

chmod +x $DESTDIR/pro/scripts/install_service.sh
cp $CURDIR/dirac-install $DESTDIR/pro/scripts

##############################################################
# INSTALL SERVICES
#

##############################################################
# INSTALL AGENTS
#

cat > $DESTDIR/etc/WorkloadManagement_TaskQueueDirector.cfg <<EOF
Systems
{
  WorkloadManagement
  {
    $DIRACINSTANCE
    {
      Agents
      {
        TaskQueueDirector
        {
          SubmitPools = DIRAC
          DefaultSubmitPools = DIRAC
          ComputingElements = $CEType
        }
      }
    }
  }
}
EOF

$DESTDIR/pro/scripts/install_agent.sh WorkloadManagement TaskQueueDirector
$DESTDIR/pro/scripts/install_agent.sh Framework CAUpdateAgent

######################################################################
