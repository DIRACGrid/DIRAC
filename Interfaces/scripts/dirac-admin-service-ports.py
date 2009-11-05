#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-service-ports
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <DIRAC Setup>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) > 1:
  usage()

setup = ''
if args:
  setup = args[0]

diracAdmin = DiracAdmin()
result = diracAdmin.getServicePorts(setup,printOutput=True)
if result['OK']:
  DIRAC.exit(0)
else:
  print result['Message']
  DIRAC.exit(2)

