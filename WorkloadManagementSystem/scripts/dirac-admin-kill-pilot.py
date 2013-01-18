#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-kill-pilot
# Author :  A.T.
########################################################################
"""
  Kill the specified pilot
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s <pilot reference>' % Script.scriptName ] ) )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()
  
pilotRef = args[0]  

from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
diracAdmin = DiracAdmin()
exitCode = 0

result = diracAdmin.killPilot( pilotRef )
if not result['OK']:
  DIRAC.gLogger.error( 'Failed to kill pilot', pilotRef )
  DIRAC.gLogger.error( result['Message'] )
  exitCode = 1

DIRAC.exit( exitCode )
