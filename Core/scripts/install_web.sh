#!/bin/bash
########################################################################
# $Header: /local/reps/dirac/DIRAC3/scripts/install_agent.sh,v 1.17 2009/05/25 07:15:37 rgracian Exp $
# File :   install_web.sh
# Author : Ricardo Graciani, A.T.
########################################################################

echo Installing DIRAC Web Portal

#
DESTDIR=$1
VERDIR=$2
DIRACVERSION=$3
DIRACARCH=$4
DIRACPYTHON=$5
#
source $DESTDIR/bashrc
#
# Install the DIRAC Web Portal software
echo python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -p $DIRACARCH -i $DIRACPYTHON -e Web || exit 1
     python dirac-install.py -t server -P $VERDIR -r $DIRACVERSION -p $DIRACARCH -i $DIRACPYTHON -e Web || exit 1
echo python dirac-install.py -t web -P $VERDIR -r $DIRACVERSION -p $DIRACARCH -i $DIRACPYTHON || exit 1
     python dirac-install.py -t web -P $VERDIR -r $DIRACVERSION -p $DIRACARCH -i $DIRACPYTHON || exit 1

#
# Deploy Ext and YUI libraries
$DESTDIR/pro/Web/tarballs/deploy.sh

#################################################
# Create the Web Server runit directory
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
# Create the Paster runit directory
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

echo Web Portal successfully installed
exit 0