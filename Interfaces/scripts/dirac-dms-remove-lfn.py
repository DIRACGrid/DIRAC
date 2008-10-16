#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-dms-remove-lfn.py,v 1.1 2008/10/16 09:28:33 paterson Exp $
# File :   dirac-dms-remove-lfn
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-dms-remove-lfn.py,v 1.1 2008/10/16 09:28:33 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                       import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> [<LFN>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

dirac = Dirac()
exitCode = 0
for lfn in args:
  result = dirac.removeFile(lfn,printOutput=True)
  if not result['OK']:
    print 'ERROR %s' %(result['Message'])
    exitCode = 2

DIRAC.exit(exitCode)