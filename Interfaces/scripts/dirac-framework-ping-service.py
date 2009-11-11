#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-framework-ping-service
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
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
