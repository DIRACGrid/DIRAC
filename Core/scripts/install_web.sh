#!/bin/bash
########################################################################
# $HeadURL: /local/reps/dirac/DIRAC3/scripts/install_agent.sh,v 1.17 2009/05/25 07:15:37 rgracian Exp $
# File :   install_agent.sh
# Author : Ricardo Graciani, A.T.
########################################################################
#
DESTDIR=`dirname $0`
DESTDIR=`cd $DESTDIR/../..; pwd`
[ -z "$LOGLEVEL" ] && LOGLEVEL=INFO
#
source $DESTDIR/bashrc

#################################################
# Create the Web Server runit directories
#

ServerDir=$DESTDIR/runit/Web/Server
mkdir -p $ServerDir/log
#
cat > $ServerDir/log/config << EOF
s10000000
n20
EOF
cat > $ServerDir/log/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec svlogd .
EOF
cat > $ServerDir/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec 2>&1
#
renice 20 -p \$\$
#
exec lighttpdSvc.sh  
EOF
chmod +x $ServerDir/log/run $ServerDir/run
#
# Create startup link
[ -e $DESTDIR/startup/Web_Server ] || ln -s $ServerDir $DESTDIR/startup/Web_Server

####################################################################
# Create the Paster runit directories
#
ServerDir=$DESTDIR/runit/Web/Paster
mkdir -p $ServerDir/log
#
cat > $ServerDir/log/config << EOF
s10000000
n20
EOF
cat > $ServerDir/log/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec svlogd .
EOF
cat > $ServerDir/run << EOF
#!/bin/bash
#
source $DESTDIR/bashrc
#
exec 2>&1
#
renice 20 -p \$\$
#
export PYTHONPATH=\$PYTHONPATH:\$DIRAC/Web/
exec 2>&1
exec paster serve --reload \$DIRAC/Web/production.ini
EOF
chmod +x $ServerDir/log/run $ServerDir/run
# Create startup link
[ -e $DESTDIR/startup/Web_Paster ] || ln -s $ServerDir $DESTDIR/startup/Web_Paster

exit 0
