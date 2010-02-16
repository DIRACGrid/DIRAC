#!/bin/bash 
########################################################################
# File :   update_sw.sh
# $HeadURL: $
# $Id:  $
#
# This script updates the DIRAC software for the already existing installation
#
# Authors: R.Graciani, A.T.
########################################################################
#
# Check the following settings before installation 
#
# User allowed to execute the script
DIRACUSER=dirac
#
# Location of the installation
DESTDIR=/opt/dirac
#
# DIRAC software version
DIRACVERSION=$1
if [ -z "$DIRACVERSION" ]; then
  echo DIRAC version is not given
  exit 1
fi  
#
# Use the following extensions
EXTENSION=LHCb
#
# Install Web Portal flag
INSTALL_WEB=
if [ -d $DESTDIR/pro/Web ]; then
  INSTALL_WEB=yes
fi
#
# The binary platform as evaluated by the dirac-platform script 
DIRACARCH=Linux_x86_64_glibc-2.5
#
# The version of the python interpreter
DIRACPYTHON=25
#
# The version of the LCG middleware
LCGVERSION=2009-08-13

ORACLE_CLIENT=`$DESTDIR/pro/$DIRACARCH/bin/python -c "import cx_Oracle"  > /dev/null 2>&1 ; echo $?`

######################################################################
#
# The installation starts here
#
######################################################################

DIRACDIRS="startup runit data work control sbin"

# check if we are the right user
#echo Checking the user
#if [ "$USER" != "$DIRACUSER" ] ; then
#  echo $0 should be run by $DIRACUSER
#  exit 1
#fi

#
# make sure $DESTDIR is available
if [ ! -d $DESTDIR ]; then
 echo There is no DIRAC installation to update
 exit 1
fi

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

#
# give a unique name to dest directory VERDIR
VERDIR=$DESTDIR/versions/${DIRACVERSION}-`date +"%s"`
mkdir -p $VERDIR || exit 1

#
# Install DIRAC software now
# First get the dirac-install script
if [ -e $DESTDIR/pro/DIRAC/Core/scripts/dirac-install.py ]; then
  INSTALL_SCRIPT=$DESTDIR/pro/DIRAC/Core/scripts/dirac-install.py
else  
  echo Downloading dirac-install.py script
  [ -e dirac-install.py ] && rm dirac-install.py
  wget http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/dirac-install.py
  INSTALL_SCRIPT=dirac-install.py
fi
#
# Prepare the list of extensions  
EXT=''
if [ ! -z "$EXTENSION" ]; then
  for ext in $EXTENSION; do
    EXT="-e $ext $EXT"
  done
fi
if [ "$INSTALL_WEB" == "yes" ]; then
  EXT="$EXT -e Web"
fi
#
# Create link to etc directory to prevent etc directory to be created
[ -e $VERDIR/etc ] || ln -s ../../etc $VERDIR   || exit 1

echo Installing DIRAC software
echo python $INSTALL_SCRIPT -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON $EXT || exit 1
     python $INSTALL_SCRIPT -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON $EXT || exit 1

#
# Create pro and old links
old=$DESTDIR/old
pro=$DESTDIR/pro
[ -L $old ] && rm $old; [ -e $old ] && exit 1; [ -L $pro ] && mv $pro $old; [ -e $pro ] && exit 1; ln -s $VERDIR $pro || exit 1

#
# Create bin link
ln -sf pro/$DIRACARCH/bin $DESTDIR/bin

#
# Compile all python files .py -> .pyc, .pyo
cmd="from compileall import compile_dir ; compile_dir('"$DESTDIR/pro"', force=1, quiet=True )"
$DESTDIR/pro/$DIRACARCH/bin/python -c "$cmd" 1> /dev/null || exit 1
$DESTDIR/pro/$DIRACARCH/bin/python -O -c "$cmd" 1> /dev/null  || exit 1

#
# Finalize the Web Portal installation
if [ "$INSTALL_WEB" == "yes" ]; then
  $DESTDIR/pro/Web/tarballs/deploy.sh
fi
#
# Create link to permanent directories
for dir in etc $DIRACDIRS ; do
  [ -e $VERDIR/$dir ] || ln -s ../../$dir $VERDIR   || exit 1
done

#
# Fix mysql.server to make it point to the actual db directory
#
sed -i "s:^datadir=.*:datadir=/opt/dirac/mysql/db:" /opt/dirac/pro/mysql/share/mysql/mysql.server

[ $ORACLE_CLIENT -eq 0 ] && $DESTDIR/pro/scripts/install_oracle-client.sh
