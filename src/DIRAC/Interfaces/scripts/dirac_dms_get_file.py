#!/usr/bin/env python
########################################################################
# File :    dirac-dms-get-file
# Author :  Stuart Paterson
########################################################################
"""
Retrieve a single file or list of files from Grid storage to the current directory.

Example:
  $ dirac-dms-get-file /formation/user/v/vhamar/Example.txt
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Example.txt': '/afs/in2p3.fr/home/h/hamar/Tests/DMS/Example.txt'}}
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
  # Registering arguments will automatically add their description to the help menu
  Script.registerArgument(["LFN:      Logical File Name or file containing LFNs"])
  Script.parseCommandLine(ignoreErrors=True)
  lfns = Script.getPositionalArgs()

  if len(lfns) < 1:
    Script.showHelp()

  from DIRAC.Interfaces.API.Dirac import Dirac
  dirac = Dirac()
  exitCode = 0

  if len(lfns) == 1:
    try:
      with open(lfns[0], 'r') as f:
        lfns = f.read().splitlines()
    except Exception:
      pass

  result = dirac.getFile(lfns, printOutput=True)
  if not result['OK']:
    print('ERROR %s' % (result['Message']))
    exitCode = 2

  DIRAC.exit(exitCode)


if __name__ == "__main__":
  main()
