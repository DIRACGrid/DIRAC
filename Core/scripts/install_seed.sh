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
#
# Check the following settings before installation 
#
# User allowed to execute the script
DIRACUSER=dirac
#
# Host where it is allowed to run the script
DIRACHOST=volhcb17.cern.ch
#
# The DN of the host certificate
DIRACHOSTDN=/DC=ch/DC=cern/OU=computers/CN=volhcb17.cern.ch
#
# The user name of the primary DIRAC administrator
DIRACADMIN=atsareg
#
# The DN of the the primary DIRAC administrator
DIRACADMINDN='/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev'
#
# The CN of the admin user certificate issuer
DIRACADMINCN=/C=FR/O=CNRS/CN=GRID2-FR
#
# The e-mail address of the admin user 
DIRACADMINEMAIL=atsareg@in2p3.fr
#
# Location of the installation
DESTDIR=/opt/dirac
#
# Installation site name
SiteName=VOLHCB17.cern.ch
#
# The main VO name
VO=lhcb
#
# The name of the master configuration database
CONFIGNAME=LHCb-Prod
#
# The DIRAC setup name to which the local instance belongs
DIRACSETUP=LHCb-NewProduction
#
# The name of the local service instance 
DIRACINSTANCE=NewProduction
#
# DIRAC software version
DIRACVERSION=v5r0
#
# Use the following extensions
EXTENSION=LHCb
#
# Install Web Portal flag
INSTALL_WEB=yes
#
# The binary platform as evaluated by the dirac-platform script 
DIRACARCH=Linux_x86_64_glibc-2.5
#
# The version of the python interpreter
DIRACPYTHON=25
#
# The version of the LCG middleware
LCGVERSION=2009-08-13

######################################################################
#
# The installation starts here
#
######################################################################

DIRACDIRS="startup runit data work control sbin"

# check if we are called in the right host
echo Checking the host name
if [ "`hostname`" != "$DIRACHOST" ] ; then
  echo $0 should be run at $DIRACHOST
fi
# check if we are the right user
echo Checking the user
if [ $USER != $DIRACUSER ] ; then
  echo $0 should be run by $DIRACUSER
  exit
fi
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

###################<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Generate the minimally required global configuration
#
if [ ! -e $DESTDIR/etc/$CONFIGNAME.cfg ] ; then

  echo Generate $CONFIGNAME.cfg file

#
# Add Extensions option
  CONFIGEXT=
  if [ ! -z "$EXTENSION" ]; then
    EXT=`echo $EXTENSION | sed 's/ /,/'`
    CONFIGEXT="Extensions = $EXT"
  fi

  cat >> $DESTDIR/etc/$CONFIGNAME.cfg << EOF || exit
DIRAC
{
  Configuration
  {
    Servers = dips://$DIRACHOST:9135/Configuration/Server
    Name = $CONFIGNAME
  }
  Setups
  {
    $DIRACSETUP
    {
      Configuration = $DIRACINSTANCE
      Framework = $DIRACINSTANCE
    }
  }
  $CONFIGEXT
}
Registry
{
  Hosts
  {
    host-$DIRACHOST
    {
      DN = $DIRACHOSTDN
      Properties  = JobAdministrator
      Properties += FullDelegation
      Properties += Operator
      Properties += CSAdministrator
      Properties += ProductionManagement
      Properties += AlarmsManagement
      Properties += ProxyManagement
      Properties += TrustedHost
    }
  }
  Groups
  {
    diracAdmin
    {
      Users = $DIRACADMIN
      Properties  = Operator
      Properties += FullDelegation
      Properties += ProxyManagement
      Properties += NormalUser
      Properties += ServiceAdministrator
      Properties += JobAdministrator
      Properties += CSAdministrator
      Properties += AlarmsManagement
    }
    ${VO}_user
    {
      Users = $DIRACADMIN
      Properties = NormalUser
    }
    ${VO}_pilot
    {
      Users = $DIRACADMIN
      Properties = GenericPilot, LimitedDelegation, Pilot
    }
  }
  Users
  {
    $DIRACADMIN
    {
      DN = $DIRACADMINDN
      CN = $DIRACADMINCN
      Email = $DIRACADMINEMAIL
    }
  }
}
EOF
fi
#################>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

#################<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Generate the minimally required local configuration
#
if [ ! -e $DESTDIR/etc/dirac.cfg ] ; then
  cat >> $DESTDIR/etc/dirac.cfg << EOF || exit
LocalSite
{
  EnableAgentMonitoring = yes
}
DIRAC
{
  Setup = $DIRACSETUP
  Setups
  {
    $DIRACSETUP
    {
      Configuration = $DIRACINSTANCE
    }
  }
  Configuration
  {
    Master = yes
    Servers = dips://$DIRACHOST:9135/Configuration/Server
    Name = $CONFIGNAME
  }
  Security
  {
    CertFile = $DESTDIR/etc/grid-security/hostcert.pem
    KeyFile = $DESTDIR/etc/grid-security/hostkey.pem
  }
}
EOF
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
EXT=''
if [ ! -z "$EXTENSION" ]; then
  for ext in $EXTENSION; do
    EXT="-e $ext $EXT"
  done
fi
#
# Create link to etc directory to prevent etc directory to be created
[ -e $VERDIR/etc ] || ln -s ../../etc $VERDIR   || exit 1

echo Installing DIRAC software
echo python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON $EXT || exit 1
     python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -g $LCGVERSION -p $DIRACARCH -i $DIRACPYTHON $EXT || exit 1

#
# Do the standard DIRAC configuration
echo 
  $VERDIR/scripts/dirac-configure -n $SiteName --UseServerCertificate -o /LocalSite/Root=$ROOT/pro -V $VO --SkipCAChecks || exit 1
echo

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

##############################################################
# Install the minimal set of services which allows a remote 
# management of the DIRAC setup 
#
# Install basic services
$DESTDIR/pro/scripts/install_service.sh Configuration Server
$DESTDIR/pro/scripts/install_service.sh Framework SystemAdministrator

###################<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Generate the required Configuration Server configuration
#
[ -e $DESTDIR/etc/Configuration_Server.cfg ] && rm -f $DESTDIR/etc/Configuration_Server.cfg
cat >> $DESTDIR/etc/Configuration_Server.cfg << EOF || exit
Systems
{
  Configuration
  {
    $DIRACINSTANCE
    {
      Services
      {
        Server
        {
          LogLevel = DEBUG
          Port = 9135
          HandlerPath = DIRAC/ConfigurationSystem/Service/ConfigurationHandler.py
          Protocol = dips
          Authorization
          {
            Default = all
            commitNewData = CSAdministrator
          }
        }
      }
    }
  }
}
EOF

#
# Generate System Administrator service configuration
#[ -e $DESTDIR/etc/Framework_SystemAdministrator.cfg ] && rm -f $DESTDIR/etc/Framework_SysAdministrator.cfg
grep SystemAdministrator $DESTDIR/etc/$CONFIGNAME.cfg > /dev/null || cat >> $DESTDIR/etc/$CONFIGNAME.cfg << EOF || exit
Systems
{
  Framework
  {
    $DIRACINSTANCE
    {
      Services
      {
        SystemAdministrator
        {
          LogLevel = DEBUG
          Port = 9162
          Protocol = dips
          Authorization
          {
            Default = all
            commitNewData = CSAdministrator
          }
        }
      }
    }
  }
}
EOF

#
# Put the basic services under the runit control
[ -e  $DESTDIR/startup/Configuration_Server ] || ln -s $DESTDIR/runit/Configuration/Server $DESTDIR/startup/Configuration_Server
[ -e  $DESTDIR/startup/Framework_SystemAdministrator ] || ln -s $DESTDIR/runit/Framework/SystemAdministrator $DESTDIR/startup/Framework_SystemAdministrator

ls -ltr /opt/dirac/pro

#
# Install Web Portal
if [ ! -z "$INSTALL_WEB" ]; then
  install_web.sh $DESTDIR $VERDIR $DIRACVERSION $DIRACARCH $DIRACPYTHON $CONFIGNAME
fi

#
# Create link to permanent directories
for dir in etc $DIRACDIRS ; do
  [ -e $VERDIR/$dir ] || ln -s ../../$dir $VERDIR   || exit 1
done
