#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-framework-ping-service.py,v 1.1 2008/10/16 09:27:23 paterson Exp $
# File :   dirac-framework-ping-service
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-framework-ping-service.py,v 1.1 2008/10/16 09:27:23 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                        import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <DIRAC System Name> <DIRAC Service Name>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2:
  usage()

dirac = Dirac()
exitCode = 0

system = args[0]
service = args[1]
result = dirac.ping(system,service,printOutput=True)

if not result:
  exitCode=2
  print 'ERROR: Null result from ping()'
  DIRAC.exit(exitCode)

if not result['OK']:
  exitCode=2

DIRAC.exit(exitCode)
