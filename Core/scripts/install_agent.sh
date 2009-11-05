#!/bin/bash
########################################################################
# $Header: /local/reps/dirac/DIRAC3/scripts/install_agent.sh,v 1.17 2009/05/25 07:15:37 rgracian Exp $
# File :   install_agent.sh
# Author : Ricardo Graciani
########################################################################
#
DESTDIR=`dirname $0`
DESTDIR=`cd $DESTDIR/../..; pwd`
[ -z "$LOGLEVEL" ] && LOGLEVEL=INFO
#
source $DESTDIR/bashrc
System=$1
Agent=$2
[ -z "$Agent" ] && exit 1
echo ${System}/${Agent} ..
#
AgentDir=$DESTDIR/runit/${System}/${Agent}
if [ -d  $AgentDir ] && [ ! -z "$3" ] ; then
  # Create a new installation or Replace existing on if required
  rm -rf $AgentDir
  NewInstall=1
elif [ ! -d $AgentDir ] ; then
  NewInstall=1
fi
mkdir -p $AgentDir/log
#
cat > $AgentDir/log/config << EOF
s10000000
n20
EOF
cat > $AgentDir/log/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec svlogd .
EOF
cat > $AgentDir/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec 2>&1
#
renice 20 -p \$\$
#
exec python \$DIRAC/DIRAC/Core/scripts/dirac-agent.py $System/$Agent \$DIRAC/etc/${System}_${Agent}.cfg -o LogLevel=$LOGLEVEL < /dev/null
EOF
chmod +x $AgentDir/log/run $AgentDir/run

touch $DIRAC/etc/${System}_${Agent}.cfg
cd $AgentDir

# If the installation is not new do not try to restart the agent
[ -z "$NewInstall" ] && exit 1

runsv . &
id=$!
sleep 5
echo d > supervise/control
sleep 1
kill  $id

exit 0
