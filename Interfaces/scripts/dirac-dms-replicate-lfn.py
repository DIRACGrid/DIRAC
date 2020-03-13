#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-replicate-lfn
# Author  : Stuart Paterson
########################################################################
"""
  Replicate an existing LFN to another Storage Element
"""
from __future__ import print_function
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... LFN Dest [Source [Cache]]' % Script.scriptName,
                                  'Arguments:',
                                  '  LFN:      Logical File Name or file containing LFNs',
                                  '  Dest:     Valid DIRAC SE',
                                  '  Source:   Valid DIRAC SE',
                                  '  Cache:    Local directory to be used as cache']))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) < 2 or len(args) > 4:
  Script.showHelp()

lfn = args[0]
seName = args[1]
sourceSE = ''
localCache = ''
if len(args) > 2:
  sourceSE = args[2]
if len(args) == 4:
  localCache = args[3]

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()
exitCode = 0

try:
  f = open(lfn, 'r')
  lfns = f.read().splitlines()
  f.close()
except BaseException:
  lfns = [lfn]

finalResult = {"Failed": [], "Successful": []}
for lfn in lfns:
  result = dirac.replicateFile(lfn, seName, sourceSE, localCache, printOutput=True)
  if not result['OK']:
    finalResult["Failed"].append(lfn)
    print('ERROR %s' % (result['Message']))
    exitCode = 2
  else:
    finalResult["Successful"].append(lfn)

if len(lfns) > 1:
  print(finalResult)

DIRAC.exit(exitCode)
