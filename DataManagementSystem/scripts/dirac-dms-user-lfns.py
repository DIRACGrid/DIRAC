#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC.Core.Base import Script
days = 0
months = 0
years = 0
wildcard = '*'
baseDir = ''
Script.registerSwitch( "d:", "Days=", "     Match files older than number of days [%s]" % days)
Script.registerSwitch( "m:", "Months=", "   Match files older than number of months [%s]" % months)
Script.registerSwitch( "y:", "Years=", "    Match files older than number of years [%s]" % years)
Script.registerSwitch( "w:", "Wildcard=", " Wildcard for matching filenames [%s]" % wildcard)
Script.registerSwitch( "b:", "BaseDir=", "  Base directory to begin search /[vo]/user/[initial]/[username]")
Script.parseCommandLine( ignoreErrors = False )

args = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "d" or switch[0].lower() == "days":
    days = int(switch[1])
  if switch[0].lower() == "m" or switch[0].lower() == "months":
    months = int(switch[1])
  if switch[0].lower() == "y" or switch[0].lower() == "years":
    years = int(switch[1])
  if switch[0].lower() == "w" or switch[0].lower() == "wildcard":
    wildcard = switch[1]
  if switch[0].lower() == "b" or switch[0].lower() == "basedir":
    baseDir = switch[1]

from DIRAC import gLogger
from DIRAC.Core.Security.Misc import getProxyInfo
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.Utilities.List import sortList
from datetime import datetime,timedelta
import sys,os,time,fnmatch
rm = ReplicaManager()

vo = DIRAC.gConfig.getValue('/DIRAC/VirtualOrganization', 'lhcb')

def usage():
  gLogger.info('Usage: %s [<options>] <Directory>' % (Script.scriptName))
  gLogger.info(' Get all the files contained in the supplied directory <or by default the user home directory>')
  gLogger.info(' Only files older than a given date can be found using the -d, -m and -y options.')
  gLogger.info(' Wildcards can be used to match the filename or path with the -w option.')
  gLogger.info(' Users may only search in their own directories according to /%s/user/[initial]/[username] convention.' % vo)
  gLogger.info(' Type "%s --help" for the available options and syntax' % Script.scriptName)
  DIRAC.exit(2)

def isOlderThan(cTimeStruct,days):
  timeDelta = timedelta(days=days)
  maxCTime = datetime.utcnow() -  timeDelta
  if cTimeStruct < maxCTime:
    return True
  return False

verbose = False
if days or months or years:
  verbose = True
totalDays = 0
if years:
  totalDays += 365*years
if months:
  totalDays += 30*months
if days:
  totalDays += days

res = getProxyInfo(False,False)
if not res['OK']:
  gLogger.error("Failed to get client proxy information.",res['Message'])
  DIRAC.exit(2)
proxyInfo = res['Value']
username = proxyInfo['username']
userBase = '/%s/user/%s/%s' % (vo, username[0], username)
if not baseDir:
  baseDir = userBase
elif not baseDir.startswith(userBase):
  usage()
gLogger.info('Will search for files in %s' % baseDir)
activeDirs = [baseDir]

allFiles = []
while len(activeDirs) > 0:
  currentDir = activeDirs[0]
  res = rm.getCatalogListDirectory(currentDir,verbose)
  activeDirs.remove(currentDir)
  if not res['OK']:
    gLogger.error("Error retrieving directory contents", "%s %s" % (currentDir, res['Message']))
  elif res['Value']['Failed'].has_key(currentDir):
    gLogger.error("Error retrieving directory contents", "%s %s" % (currentDir, res['Value']['Failed'][currentDir]))
  else:
    dirContents = res['Value']['Successful'][currentDir]
    subdirs = dirContents['SubDirs']
    for subdir,metadata in subdirs.items():
      if (not verbose) or isOlderThan(metadata['CreationDate'],totalDays):
        activeDirs.append(subdir)
    for filename,fileInfo in dirContents['Files'].items():
      metadata = fileInfo['MetaData']
      if (not verbose) or isOlderThan(metadata['CreationDate'],totalDays):
        if fnmatch.fnmatch(filename,wildcard):
          allFiles.append(filename)
    files = dirContents['Files'].keys()
    gLogger.info("%s: %d files, %d sub-directories" % (currentDir,len(files),len(subdirs)))

outputFileName = '%s.lfns' % baseDir.replace( '/%s' % vo, '%s' % vo ).replace('/','-')
outputFile = open(outputFileName,'w')
for lfn in sortList(allFiles):
  outputFile.write(lfn+'\n')
outputFile.close()
gLogger.info('%d matched files have been put in %s' % (len(allFiles),outputFileName))
DIRAC.exit(0)
