#!/bin/bash

# ubeda@cern.ch
#-------------------------------------------------------------------------------

# Needs ( arguments )
# o $DIRACVERSION
#-------------------------------------------------------------------------------
if [ -z $1 ]
then
  echo Using local contents for documentation 
  LOCALDEBUG=true
else
  DIRACVERSION=$1
fi  

#DIRACVERSION is used by conf.py to declare the sphynx variable |release| 
export DIRACVERSION=$DIRACVERSION

# Create a temporary directory where to place all code
tmpdir=`mktemp -d /tmp/DIRACDocsXXXXXX`
echo Temporary directory: $tmpdir

#-------------------------------------------------------------------------------
# DIRAC

if [ -z "$LOCALDEBUG" ]
then
  DIRAC_GITHUB=https://github.com/DIRACGrid/DIRAC/archive/
  # Let's download DIRAC
  echo Downloading DIRAC from $DIRAC_GITHUB$DIRACVERSION.zip 
  # wget $DIRAC_GITHUB$DIRACVERSION.zip --no-check-certificate --directory-prefix $tmpdir -q
  echo "curl --insecure -L $DIRAC_GITHUB$DIRACVERSION.zip -s  > $tmpdir/$DIRACVERSION.zip"
  curl --insecure -L $DIRAC_GITHUB$DIRACVERSION.zip -s  > $tmpdir/$DIRACVERSION.zip

  unzip -q $tmpdir/$DIRACVERSION.zip -d $tmpdir
  mv $tmpdir/DIRAC-* $tmpdir/DIRAC
  rm $tmpdir/$DIRACVERSION*
  echo DIRAC downloaded successfully to $tmpdir/DIRAC
else  
  cp -r ../../../DIRAC $tmpdir/DIRAC
  echo DIRAC copied successfully to $tmpdir/DIRAC
fi

# Export tmpdir on PYTHONPATH so that we can import DIRAC
export PYTHONPATH=$PYTHONPATH:$tmpdir

#-------------------------------------------------------------------------------
# DIRACDocs

if [ -z "$LOCALDEBUG" ]
then

  repo=DIRACGrid
  diracDocsVersion=master

  DIRACDocs_GITHUB=https://github.com/$repo/DIRACDocs/archive/$diracDocsVersion.zip
  # Let's download DIRACDocs
  echo Downloading DIRACDocs from $DIRACDocs_GITHUB 
  # wget $DIRACDocs_GITHUB --no-check-certificate --directory-prefix $tmpdir -q
  echo "curl --insecure -L $DIRACDocs_GITHUB -s  > $tmpdir/$diracDocsVersion.zip"
  curl --insecure -L $DIRACDocs_GITHUB -s  > $tmpdir/$diracDocsVersion.zip

  unzip -q $tmpdir/$diracDocsVersion.zip -d $tmpdir
  mv $tmpdir/DIRACDocs-$diracDocsVersion $tmpdir/DIRACDocs
  rm $tmpdir/$diracDocsVersion*
  echo DIRACDocs downloaded successfully to $tmpdir/DIRACDocs
else
  cp -r ../../docs $tmpdir/DIRACDocs
  echo DIRACDocs copied successfully to $tmpdir/DIRACDocs
fi

# Export tmpdir on PYTHONPATH so that we can import fakeEnvironment
export PYTHONPATH=$PYTHONPATH:$tmpdir/DIRACDocs/Tools

#-------------------------------------------------------------------------------
# Generate scripts and code documentation

scriptsDIR=$tmpdir/build/scripts
mkdir -p $scriptsDIR
codeDIR=$tmpdir/build/code
mkdir -p $codeDIR

echo python $tmpdir/DIRACDocs/Tools/buildScriptsDOC.py $scriptsDIR
python $tmpdir/DIRACDocs/Tools/buildScriptsDOC.py $scriptsDIR

echo python $tmpdir/DIRACDocs/Tools/buildCodeDOC.py $codeDIR
python $tmpdir/DIRACDocs/Tools/buildCodeDOC.py $codeDIR

#-------------------------------------------------------------------------------
# Make html web pages from rst's

# This command hangs, so we kill it after 5 minutes
# FYI: This command hangs because of the thread that the PlotCache creates
( make -C $tmpdir/DIRACDocs html ) & sleep 300 ; kill -9 $!; echo "killed make"

#make -C $tmpdir/DIRACDocs html

#-------------------------------------------------------------------------------
# copying over

echo "Copying over from $tmpdir/DIRACDocs/build to $DIR/../build"

#WhereAmI
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
 
if [ -d $DIR/../build.bak ]
then
  rm -rf $DIR/../build.bak
fi 
if [ -d $DIR/../build ] 
then
  echo Backing up the previous build directory
  mv $DIR/../build $DIR/../build.bak 
fi 
 
cp -r $tmpdir/DIRACDocs/build $DIR/../build

echo Removing temporary directory $tmpdir 
#rm -rf $tmpdir

echo 'Done'

#EOF