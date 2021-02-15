#!/usr/bin/env python
########################################################################
# File :    dirac-dms-replicate-lfn
# Author  : Stuart Paterson
########################################################################
"""
Replicate an existing LFN to another Storage Element

Usage:
  dirac-dms-replicate-lfn [options] ... LFN Dest [Source [Cache]]

Arguments:
  LFN:      Logical File Name or file containing LFNs (mandatory)
  Dest:     Valid DIRAC SE (mandatory)
  Source:   Valid DIRAC SE
  Cache:    Local directory to be used as cache

Example:
  $ dirac-dms-replicate-lfn /formation/user/v/vhamar/Test.txt DIRAC-USER
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Test.txt': {'register': 0.50833415985107422,
                                                        'replicate': 11.878520965576172}}}
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) < 2 or len(args) > 4:
    Script.showHelp(exitCode=1)

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


if __name__ == "__main__":
  main()
