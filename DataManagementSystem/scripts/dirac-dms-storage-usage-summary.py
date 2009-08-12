#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-storage-usage-summary.py,v 1.3 2009/08/12 15:38:21 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-storage-usage-summary.py,v 1.3 2009/08/12 15:38:21 acsmith Exp $"
__VERSION__ = "$Revision: 1.3 $"
import DIRAC
from DIRAC.Core.Base import Script
unit = 'GB'
dir = ''
fileType = ''
prod = ''
sites = []
Script.registerSwitch( "u:", "unit=","   Unit to use [%s] (MB,GB,TB,PB)" % unit)
Script.registerSwitch( "d:", "dir=", "   Dir to search [ALL]")
Script.registerSwitch( "t:", "type=", "   File type to search [ALL]")
Script.registerSwitch( "p:", "prod=", "   Production ID to search [ALL]")
Script.registerSwitch( "s:", "sites=", "  Sites to consider [ALL]")
Script.parseCommandLine( ignoreErrors = False )

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList

def usage():
  print 'Usage: %s [<options>] <Directory>' % (Script.scriptName)
  print ' Get a summary of the storage usage <for an optionally supplied directory>.'
  print ' The usage can be given in any of the following units: (MB,GB,TB,PB)' 
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
  if switch[0].lower() == "s" or switch[0].lower() == "sites":
    sites = switch[1].replace(',',' ').split()

scaleDict = { 'MB' : 1000*1000.0,
              'GB' : 1000*1000*1000.0,
              'TB' : 1000*1000*1000*1000.0,
              'PB' : 1000*1000*1000*1000*1000.0}
if not unit in scaleDict.keys():
  usage()
scaleFactor = scaleDict[unit]  
             
rpc = RPCClient('dips://volhcb08.cern.ch:9151/DataManagement/StorageUsage')
res = rpc.getStorageSummary(dir,fileType,prod,sites)
if not res['Value']:
  print 'No usage found'
  DIRAC.exit(2)
print '%s %s %s' % ('DIRAC SE'.ljust(20),('Size (%s)' % unit).ljust(20),'Files'.ljust(20))
print '-'*50
for se in sortList(res['Value'].keys()):
  dict = res['Value'][se]
  files = dict['Files']
  size = dict['Size']
  print "%s %s %s" % (se.ljust(20),('%.1f' % (size/scaleFactor)).ljust(20),str(files).ljust(20))
DIRAC.exit(0)
