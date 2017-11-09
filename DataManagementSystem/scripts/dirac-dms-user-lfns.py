#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
"""
Get the list of all the user files.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
days = 0
months = 0
years = 0
wildcard = None
baseDir = ''
emptyDirsFlag = False
Script.registerSwitch( "D:", "Days=", "Match files older than number of days [%s]" % days )
Script.registerSwitch( "M:", "Months=", "Match files older than number of months [%s]" % months )
Script.registerSwitch( "Y:", "Years=", "Match files older than number of years [%s]" % years )
Script.registerSwitch( "w:", "Wildcard=", "Wildcard for matching filenames [All]" )
Script.registerSwitch( "b:", "BaseDir=", "Base directory to begin search (default /[vo]/user/[initial]/[username])" )
Script.registerSwitch( "e", "EmptyDirs", "Create a list of empty directories" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

Script.parseCommandLine( ignoreErrors = False )

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "D" or switch[0].lower() == "days":
    days = int( switch[1] )
  if switch[0] == "M" or switch[0].lower() == "months":
    months = int( switch[1] )
  if switch[0] == "Y" or switch[0].lower() == "years":
    years = int( switch[1] )
  if switch[0].lower() == "w" or switch[0].lower() == "wildcard":
    wildcard = '*'+switch[1]
  if switch[0].lower() == "b" or switch[0].lower() == "basedir":
    baseDir = switch[1]
  if switch[0].lower() == "e" or switch[0].lower() == "emptydirs":
    emptyDirsFlag = True

import DIRAC
from DIRAC                                                   import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getVOForGroup
from DIRAC.Core.Security.ProxyInfo                           import getProxyInfo
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from datetime import datetime, timedelta
import sys, os, time, fnmatch
fc = FileCatalog()

def isOlderThan( cTimeStruct, days ):
  timeDelta = timedelta( days = days )
  maxCTime = datetime.utcnow() - timeDelta
  if cTimeStruct < maxCTime:
    return True
  return False

withMetadata = False
if days or months or years:
  withMetadata = True
totalDays = 0
if years:
  totalDays += 365 * years
if months:
  totalDays += 30 * months
if days:
  totalDays += days

res = getProxyInfo( False, False )
if not res['OK']:
  gLogger.error( "Failed to get client proxy information.", res['Message'] )
  DIRAC.exit( 2 )
proxyInfo = res['Value']
if proxyInfo['secondsLeft'] == 0:
  gLogger.error( "Proxy expired" )
  DIRAC.exit( 2 )
username = proxyInfo['username']
vo = ''
if 'group' in proxyInfo:
  vo = getVOForGroup( proxyInfo['group'] )
if not baseDir:
  if not vo:
    gLogger.error( 'Could not determine VO' )
    Script.showHelp()
  baseDir = '/%s/user/%s/%s' % ( vo, username[0], username )

baseDir = baseDir.rstrip( '/' )

gLogger.notice( 'Will search for files in %s%s' % ( baseDir, ( ' matching %s' % wildcard ) if wildcard else '' ) )
activeDirs = [baseDir]

allFiles = []
emptyDirs = []

while len( activeDirs ) > 0:
  currentDir = activeDirs.pop()
  res = fc.listDirectory( currentDir, withMetadata, timeout = 360 )
  if not res['OK']:
    gLogger.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Message'] ) )
  elif currentDir in res['Value']['Failed']:
    gLogger.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Value']['Failed'][currentDir] ) )
  else:
    dirContents = res['Value']['Successful'][currentDir]
    subdirs = dirContents['SubDirs']
    files = dirContents['Files']
    if not subdirs and not files:
      emptyDirs.append( currentDir )
      gLogger.notice( '%s: empty directory' % currentDir )
    else:
      for subdir in sorted( subdirs, reverse = True ):
        if ( not withMetadata ) or isOlderThan( subdirs[subdir]['CreationDate'], totalDays ):
          activeDirs.append( subdir )
      for filename in sorted( files ):
        fileOK = False
        if ( not withMetadata ) or isOlderThan( files[filename]['MetaData']['CreationDate'], totalDays ):
          if wildcard is None or fnmatch.fnmatch( filename, wildcard ):
            fileOK = True
        if not fileOK:
          files.pop( filename )
      allFiles += sorted( files )

      if len( files ) or len( subdirs ):
        gLogger.notice( "%s: %d files%s, %d sub-directories" % ( currentDir, len( files ), ' matching' if withMetadata or wildcard else '', len( subdirs ) ) )

outputFileName = '%s.lfns' % baseDir.replace( '/%s' % vo, '%s' % vo ).replace( '/', '-' )
outputFile = open( outputFileName, 'w' )
for lfn in sorted( allFiles ):
  outputFile.write( lfn + '\n' )
outputFile.close()
gLogger.notice( '%d matched files have been put in %s' % ( len( allFiles ), outputFileName ) )

if emptyDirsFlag:
  outputFileName = '%s.emptydirs' % baseDir.replace( '/%s' % vo, '%s' % vo ).replace( '/', '-' )
  outputFile = open( outputFileName, 'w' )
  for dir in sorted( emptyDirs ):
    outputFile.write( dir + '\n' )
  outputFile.close()
  gLogger.notice( '%d empty directories have been put in %s' % ( len( emptyDirs ), outputFileName ) )

DIRAC.exit( 0 )
