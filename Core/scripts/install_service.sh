#!/bin/bash
########################################################################
# $Header: /local/reps/dirac/DIRAC3/scripts/install_service.sh,v 1.13 2009/05/25 07:15:37 rgracian Exp $
# File :   install_service.sh
# Author : Ricardo Graciani
########################################################################
#
DESTDIR=`dirname $0`
DESTDIR=`cd $DESTDIR/../..; pwd`
[ -z "$LOGLEVEL" ] && LOGLEVEL=INFO
#
source $DESTDIR/bashrc
System=$1
Service=$2
[ -z "$Service" ] && exit 1
echo ${System}/${Service} ..
#
ServiceDir=$DESTDIR/runit/${System}/${Service}
if [ -d  $ServiceDir ] && [ ! -z "$3" ] ; then
  # Create a new installation or Replace existing on if required
  rm -rf $ServiceDir
  NewInstall=1
elif [ ! -d $ServiceDir ] ; then
  NewInstall=1
fi
mkdir -p $ServiceDir/log
#
cat > $ServiceDir/log/config << EOF
s10000000
n20
EOF
cat > $ServiceDir/log/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec svlogd .
EOF
cat > $ServiceDir/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec 2>&1
#
exec python \$DIRAC/DIRAC/Core/scripts/dirac-service.py $System/$Service \$DIRAC/etc/${System}_${Service}.cfg \$DIRAC/etc/DBs.cfg -o LogLevel=$LOGLEVEL < /dev/null
EOF
chmod +x $ServiceDir/log/run $ServiceDir/run

touch $DIRAC/etc/${System}_${Service}.cfg
cd $ServiceDir

# If the installation is not new do not try to restart the service
[ -z "$NewInstall" ] && exit 1

runsv . &
id=$!
sleep 5
echo d > supervise/control
sleep 1
kill  $id

exit 0
