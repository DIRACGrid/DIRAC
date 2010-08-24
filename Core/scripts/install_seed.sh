#!/bin/bash 
########################################################################
# File :   install_seed.sh
# $HeadURL$
# $Id$
#
# This script installs the bare minimal setup of DIRAC which allows
# to manage the composition of the DIRAC service remotely. The script should
# be only used for the initial installation and not for the software updates. 
#
# The installation is described in the CFG file which is passed to the script
# as a single argument. See DIRAC documentation for the contents of the
# installation CFG file.
#
# Points to check before the installation can be done:
# 1. The local user account under which the services will be running should
#    be created ( typically 'dirac' login name )
# 2. The directory where DIRAC will be installed should be created and owned
#    by the 'dirac' user
# 3. The host certificate and key pem files should be placed into the 
#    $DESTDIR/etc/grid-security directory and made readable by the 
#    'dirac' user. The $DESTDIR/etc/grid-security/certificates directory
#    should be populated with the CA certificates.
#
# Authors: R.Graciani, A.T.
########################################################################
CFG=$1
#
# Check the following settings before installation 
#
# Installation options. These options determine which  
# DIRAC software and which versions will be installed
# and where
#
# The root of the DIRAC installation - mandatory
DESTDIR=`grep InstancePath $CFG | grep -v '#' | cut -d '=' -f 2 | sed -e 's/ //g'`
if [ -z "$DESTDIR" ]; then
  echo InstancePath is not specified, exiting
fi 
# DIRAC software version - mandatory
DIRACVERSION=`grep DiracVersion $CFG | grep -v '#' | cut -d '=' -f 2 | sed -e 's/ //g'`
if [ -z "$DIRACVERSION" ]; then
  echo DIRAC version is not specified, using HEAD by default
  DIRACVERSION=HEAD
fi
#
# Use the following DIRAC software extensions, for example
# EXTENSION='LHCb EELA'
# or
# EXTENSION=Belle
# No extensions by default
EXTENSION=`grep Extensions $CFG | grep -v '#' | cut -d '=' -f 2 | sed -e 's/ //g' | sed -e 's/,/ /g'`
#
# Install Web Portal flag (yes/no). Default is no 
INSTALL_WEB=`grep WebPortal $CFG | grep -v '#' | cut -d '=' -f 2 | sed -e 's/ //g'`
#
# The binary platform as evaluated by the dirac-platform script. 
# Specify it only if the default platform evaluation is not acceptable
#DIRACARCH=Linux_x86_64_glibc-2.5
#
# The version of the python interpreter ( 24 for Python 2.4; 25 for Python 2.5 - default )
DIRACPYTHON=`grep PythonVersion $CFG | grep -v '#' | cut -d '=' -f 2 | sed -e 's/ //g'`
if [ -z "$DIRACPYTHON" ]; then
  echo DIRAC Python version is not specified, using 2.5 by default
  DIRACPYTHON=25
fi
#
# The version of the LCG middleware packaged by DIRAC if needed.
# It is needed if thrid party grid services are to be used, e.g. VOMS, MyProxy,
# gLite WMS, etc
LCGVERSION=`grep LCGVersion $CFG | grep -v '#' | cut -d '=' -f 2 | sed -e 's/ //g'`

#################################################################
# DIRAC Basic Configuration 
#
# In the following the install.cfg file is generated with the
# basic configuration parameters and the choice of components.
#

# Prepare some variables
if [ ! -z "$EXTENSION" ]; then
  for ext in $EXTENSION; do
    INSTALL_EXT="-e $ext $INSTALL_EXT"
    CONFIG_EXT="$ext,$CONFIG_EXT"
  done
fi
######################################################################
#
# The installation starts here
#
######################################################################

DIRACDIRS="startup runit data work control sbin"

# make sure $DESTDIR is available
mkdir -p $DESTDIR || exit 1

ROOT=`dirname $DESTDIR`/dirac

echo
echo "Installing under $ROOT"
echo
[ -d $ROOT ] || exit

#
# Check that the security info is in place
if [ ! -d $DESTDIR/etc ]; then
  echo Directory $DESTDIR/etc is missing, it should contain the host certificates
  echo and CA certificates. Exiting... 
  exit 1
fi

#################>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

for dir in $DIRACDIRS ; do
  if [ ! -d $DESTDIR/$dir ]; then
    mkdir -p $DESTDIR/$dir || exit 1
  fi
done

#
# give a unique name to dest directory VERDIR
VERDIR=$DESTDIR/versions/${DIRACVERSION}-`date +"%s"`
mkdir -p $VERDIR   || exit 1

#
# Install DIRAC software now
# First get the dirac-install script
echo Downloading dirac-install.py script
[ -e dirac-install.py ] && rm dirac-install.py
wget http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/dirac-install.py

# Prepare the list of extensions
if [ "$INSTALL_WEB" = "yes" ]; then
  INSTALL_EXT="$INSTALL_EXT -e Web"
fi
#
# Create link to etc directory to prevent etc directory to be created
[ -e $VERDIR/etc ] || ln -s ../../etc $VERDIR   || exit 1

INSTALL_ARGS=" -t server -P $VERDIR $INSTALL_EXT"
[ ! -z "$DIRACARCH" ] && INSTALL_ARGS="$INSTALL_ARGS -p $DIRACARCH"
[ ! -z "$LCGVERSION" ] && INSTALL_ARGS="$INSTALL_ARGS -g $LCGVERSION"
[ ! -z "$DIRACPYTHON" ] && INSTALL_ARGS="$INSTALL_ARGS -i $DIRACPYTHON"
if [ ! -z "$DIRACVERSION" ]; then
   INSTALL_ARGS="$INSTALL_ARGS -r $DIRACVERSION"
else
   INSTALL_ARGS="$INSTALL_ARGS -r HEAD"
fi

echo Installing DIRAC software
echo python dirac-install.py $INSTALL_ARGS $CFG || exit 1
     python dirac-install.py $INSTALL_ARGS $CFG || exit 1

#
# Do the standard DIRAC configuration
echo 
  $VERDIR/scripts/dirac-configure --UseServerCertificate -o /LocalSite/Root=$ROOT/pro --SkipCAChecks || exit 1
echo

#
# Create pro and old links
old=$DESTDIR/old
pro=$DESTDIR/pro
[ -L $old ] && rm $old; [ -e $old ] && exit 1; [ -L $pro ] && mv $pro $old; [ -e $pro ] && exit 1; ln -s $VERDIR $pro || exit 1

if [ -z "$DIRACARCH" ]; then
  DIRACARCH=`$VERDIR/scripts/dirac-platform`
fi 

#
# Create bin link
ln -sf pro/$DIRACARCH/bin $DESTDIR/bin

#
# Compile all python files .py -> .pyc, .pyo
cmd="from compileall import compile_dir ; compile_dir('"$DESTDIR/pro"', force=1, quiet=True )"
$DESTDIR/pro/$DIRACARCH/bin/python -c "$cmd" 1> /dev/null || exit 1
$DESTDIR/pro/$DIRACARCH/bin/python -O -c "$cmd" 1> /dev/null  || exit 1

#
# Generate environment setting bashrc script
$DESTDIR/pro/scripts/install_bashrc.sh    $DESTDIR $DIRACVERSION $DIRACARCH python$DIRACPYTHON || exit 1

#
# fix user .bashrc
#
grep -q "source $DESTDIR/bashrc" $HOME/.bashrc || \
  echo "source $DESTDIR/bashrc" >> $HOME/.bashrc

source $DESTDIR/bashrc
#
# install startup at boot script
if [ ! -e $DESTDIR/sbin/runsvdir-start ] ; then
cat >> $DESTDIR/sbin/runsvdir-start << EOF || exit
#!/bin/bash
source $DESTDIR/bashrc
RUNSVCTRL=`which runsvctrl`
chpst -u $DIRACUSER \$RUNSVCTRL d $DESTDIR/startup/*
killall runsv svlogd
RUNSVDIR=`which runsvdir`
exec chpst -u $DIRACUSER \$RUNSVDIR -P $DESTDIR/startup 'log:  DIRAC runsv'
EOF
fi
chmod +x $DESTDIR/sbin/runsvdir-start

echo "Setting up the site now"

dirac-setup-site $CFG
