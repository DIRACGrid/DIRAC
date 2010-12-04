#!/usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################
__RCSID__   = "$Id: $"
from DIRAC.Core.Base import Script

dir = ''
fileType = ''
ses = []
prods = []
Script.registerSwitch( "d:", "Dir=", "   Dir to search [ALL]")
Script.registerSwitch( "t:", "Type=", "   File type to search [ALL]")
Script.registerSwitch( "S:", "SEs=", "  SEs to consider [ALL] (space or comma separated list)")
Script.registerSwitch( "p:", "Prod=", "   Production ID to search [ALL] (space or comma separated list)")

Script.setUsageMessage("""
Get summary of storage directory usage

Usage:
   %s [option]
""" % Script.scriptName)

Script.parseCommandLine()

from DIRAC.DataManagementSystem.Client.StorageUsageClient import StorageUsageClient
rpc = StorageUsageClient()

from DIRAC.Core.Utilities.List import sortList

prods = [int(x) for x in Script.getPositionalArgs()]

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "d" or switch[0].lower() == "dir":
    dir = switch[1]
  if switch[0].lower() == "t" or switch[0].lower() == "type":
    fileType = switch[1]
  if switch[0].lower() == "c" or switch[0].lower() == "ses":
    ses = switch[1].replace(',',' ').split()
  if switch[0].lower() == "p" or switch[0].lower() == "prod":
    prods = switch[1].replace(',',' ').split()
    prods = [int(x) for x in prods]

allDirs = []
if not prods:
  res = rpc.getStorageDirectorySummary(dir,fileType,'',ses)
  if not res['OK']:
    print "ERROR getting storage directories",res['Message']
  else:
    for resDir,size,files in res['Value']:
      if resDir not in allDirs:
        allDirs.append(resDir)
for prod in prods:
  res = rpc.getStorageDirectorySummary(dir,fileType,prod,ses)
  if not res['OK']:
    print "ERROR getting storage directories",res['Message']
  else:
    for resDir,size,files in res['Value']:
      if resDir not in allDirs:
        allDirs.append(resDir)

for dir in sortList(allDirs):
  print dir
