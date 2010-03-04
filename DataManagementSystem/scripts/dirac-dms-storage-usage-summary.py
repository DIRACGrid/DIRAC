#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.6 $"
import DIRAC
from DIRAC.Core.Base import Script
unit = 'TB'
dir = ''
fileType = ''
prod = ''
sites = []
ses = []
lcg = False
Script.registerSwitch( "u:", "Unit=","   Unit to use [%s] (MB,GB,TB,PB)" % unit)
Script.registerSwitch( "d:", "Dir=", "   Dir to search [ALL]")
Script.registerSwitch( "t:", "Type=", "   File type to search [ALL]")
Script.registerSwitch( "p:", "Prod=", "   Production ID to search [ALL]")
Script.registerSwitch( "g:", "Sites=", "  Sites to consider [ALL] (space or comma seperated list)")
Script.registerSwitch( "c:", "SEs=", "  SEs to consider [ALL] (space or comma seperated list)")
Script.registerSwitch( "l", "LCG", "  Group results by tape and disk")

Script.parseCommandLine( ignoreErrors = False )

from DIRAC import gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList
import re

def usage():
  print 'Usage: %s [<options>] <Directory>' % (Script.scriptName)
  print ' Get a summary of the storage usage <for an optionally supplied directory>.'
  print ' The usage can be given in any of the following units: (MB,GB,TB,PB)' 
  print ' The sites options should be a space or comma separated list e.g. --Sites="CNAF-RAW,GRIDKA-RAW" or --Sites="CNAF-RAW GRIDKA-RAW"'
  print ' Type "%s --help" for the available options and syntax' % Script.scriptName
  DIRAC.exit(2)

args = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "u" or switch[0].lower() == "unit":
    unit = switch[1]
  if switch[0].lower() == "d" or switch[0].lower() == "dir":
    dir = switch[1]
  if switch[0].lower() == "t" or switch[0].lower() == "type":
    fileType = switch[1]
  if switch[0].lower() == "p" or switch[0].lower() == "prod":
    prod = switch[1]
  if switch[0].lower() == "g" or switch[0].lower() == "sites":
    sites = switch[1].replace(',',' ').split()
  if switch[0].lower() == "c" or switch[0].lower() == "ses":
    ses = switch[1].replace(',',' ').split()
  if switch[0].lower() == "l" or switch[0].lower() == "lcg":
    lcg = True

if not type(ses) == type([]):
  usage()
if not type(sites) == type([]):
  usage()
for site in sites:
  res = gConfig.getOptionsDict('/Resources/Sites/LCG/%s' % site)
  if not res['OK']:
    print 'Site %s not known' % site
    usage()
  ses.extend(res['Value']['SE'].replace(' ','').split(','))
scaleDict = { 'MB' : 1000*1000.0,
              'GB' : 1000*1000*1000.0,
              'TB' : 1000*1000*1000*1000.0,
              'PB' : 1000*1000*1000*1000*1000.0}
if not unit in scaleDict.keys():
  usage()
scaleFactor = scaleDict[unit]

rpc = RPCClient('DataManagement/StorageUsage')
res = rpc.getStorageSummary(dir,fileType,prod,ses)
if not res['OK']:
  print 'No usage found'
  DIRAC.exit(2)

if lcg:
  tapeTotalFiles = 0
  diskTotalFiles = 0
  tapeTotalSize = 0
  diskTotalSize = 0
  for se in sortList(res['Value'].keys()):
    files = res['Value'][se]['Files']
    size = res['Value'][se]['Size']
    if re.search('-RAW',se) or re.search('-RDST',se) or re.search('-tape',se) or re.search('M-DST',se):
      tapeTotalFiles+= files
      tapeTotalSize+= size
    if re.search('-DST',se) or re.search('-FAILOVER',se) or re.search('-USER',se) or re.search('-disk',se) or re.search('-HIST',se):
      diskTotalFiles+=files
      diskTotalSize+=size      
  print '%s %s %s' % ('Storage Type'.ljust(20),('Size (%s)' % unit).ljust(20),'Files'.ljust(20))
  print '-'*50
  print "%s %s %s" % ('T1D*'.ljust(20),('%.1f' % (tapeTotalSize/scaleFactor)).ljust(20),str(tapeTotalFiles).ljust(20))       
  print "%s %s %s" % ('T*D1'.ljust(20),('%.1f' % (diskTotalSize/scaleFactor)).ljust(20),str(diskTotalFiles).ljust(20))       
  DIRAC.exit(0)

print '%s %s %s' % ('DIRAC SE'.ljust(20),('Size (%s)' % unit).ljust(20),'Files'.ljust(20))
print '-'*50
for se in sortList(res['Value'].keys()):
  dict = res['Value'][se]
  files = dict['Files']
  size = dict['Size']
  print "%s %s %s" % (se.ljust(20),('%.1f' % (size/scaleFactor)).ljust(20),str(files).ljust(20))
DIRAC.exit(0)
