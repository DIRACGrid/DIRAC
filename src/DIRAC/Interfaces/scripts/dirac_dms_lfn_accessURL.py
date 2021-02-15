#!/usr/bin/env python
########################################################################
# File :    dirac-dms-lfn-accessURL
# Author :  Stuart Paterson
########################################################################
"""
Retrieve an access URL for an LFN replica given a valid DIRAC SE.

Usage:
  dirac-dms-lfn-accessURL [options] ... LFN SE [PROTO]

Arguments:
  LFN:      Logical File Name or file containing LFNs (mandatory)
  SE:       Valid DIRAC SE (mandatory)
  PROTO:    Optional protocol for accessURL

Example:
  $ dirac-dms-lfn-accessURL /formation/user/v/vhamar/Example.txt DIRAC-USER
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Example.txt': 'dips://dirac.in2p3.fr:9148/DataManagement/StorageElement\
   /formation/user/v/vhamar/Example.txt'}}
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

  # pylint: disable=wrong-import-position
  from DIRAC.Interfaces.API.Dirac import Dirac

  if len(args) < 2:
    Script.showHelp(exitCode=1)

  if len(args) > 3:
    print('Only one LFN SE pair will be considered')

  dirac = Dirac()
  exitCode = 0

  lfn = args[0]
  seName = args[1]
  proto = False
  if len(args) > 2:
    proto = args[2]

  try:
    with open(lfn, 'r') as f:
      lfns = f.read().splitlines()
  except IOError:
    lfns = [lfn]

  for lfn in lfns:
    result = dirac.getAccessURL(lfn, seName, protocol=proto, printOutput=True)
    if not result['OK']:
      print('ERROR: ', result['Message'])
      exitCode = 2

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
