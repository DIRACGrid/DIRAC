#!/bin/bash
########################################################################
# $Header: /local/reps/dirac/DIRAC3/scripts/install_bashrc.sh,v 1.6 2009/10/06 13:39:49 rgracian Exp $
# File :   install_bashrc.sh
# Author : Ricardo Graciani
########################################################################
#
DESTDIR=$1
VERSION=$2
ARCH=$3
PYTHON=$4

cd $DESTDIR || exit 1

rm -f bashrc
[ -e bashrc ] && exit 1

cat > bashrc << EOF
export PYTHONUNBUFFERED=yes
export PYTHONOPTIMIZE=x
export HOME=$HOME
PYTHONPATH=""
LD_LIBRARY_PATH=""

DIRAC=$DESTDIR/pro
DIRACBIN=$DESTDIR/pro/$ARCH/bin
DIRACSCRIPTS=$DESTDIR/pro/scripts
DIRACLIB=$DESTDIR/pro/$ARCH/lib

export X509_CERT_DIR=${DESTDIR}/etc/grid-security/certificates
export X509_VOMS_DIR=${DESTDIR}/etc/grid-security/vomsdir

sDIRACBIN="`echo $DESTDIR/pro/$ARCH/bin | sed 's/\//\\\\\//g'`"
sDIRACSCRIPTS="`echo $DESTDIR/pro/scripts | sed 's/\//\\\\\//g'`"
PATH=\`echo \$PATH | sed "s/\$sDIRACBIN://g"\`
PATH=\`echo \$PATH | sed "s/\$sDIRACSCRIPTS://g"\`

( echo \$PATH | grep -q \$DIRACBIN ) || export PATH=\$DIRACBIN:\$PATH
( echo \$PATH | grep -q \$DIRACSCRIPTS ) || export PATH=\$DIRACSCRIPTS:\$PATH

[ -z "\$LD_LIBRARY_PATH" ] && export LD_LIBRARY_PATH=\$DIRACLIB:\$DIRACLIB/mysql
( echo \$LD_LIBRARY_PATH | grep -q \$DIRACLIB ) || export LD_LIBRARY_PATH=\$DIRACLIB:\$DIRACLIB/mysql:\$LD_LIBRARY_PATH

( echo \$PYTHONPATH | grep -q \$DIRAC: ) || export PYTHONPATH=\$DIRAC:\$PYTHONPATH
( echo \$PYTHONPATH | grep -q \$DIRACSCRIPTS ) || export PYTHONPATH=\$DIRACSCRIPTS:\$PYTHONPATH

( echo \$PATH | grep -q /usr/local/bin ) || export PATH=\$PATH:/usr/local/bin

EOF

source bashrc
echo PATH=$PATH
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
