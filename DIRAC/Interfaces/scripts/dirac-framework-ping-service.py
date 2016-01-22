#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-framework-ping-service
# Author :  Stuart Paterson
########################################################################
"""
  Ping the given DIRAC Service
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... System Service|System/Agent' % Script.scriptName,
                                    'Arguments:',
                                    '  System:   Name of the DIRAC system (ie: WorkloadManagement)',
                                    '  Service:  Name of the DIRAC service (ie: Matcher)'] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
if len( args ) == 1:
  args = args[0].split( '/' )

if len( args ) < 2:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac                        import Dirac
dirac = Dirac()
exitCode = 0

system = args[0]
service = args[1]
result = dirac.ping( system, service, printOutput = True )

if not result:
  print 'ERROR: Null result from ping()'
  exitCode = 2
elif not result['OK']:
  print 'ERROR: ', result['Message']
  exitCode = 2

DIRAC.exit( exitCode )
