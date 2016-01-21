#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-wms-job-kil
# Author :  Stuart Paterson
########################################################################
"""
  Issue a kill signal to a running DIRAC job
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                     'Arguments:',
                                     '  JobID:    DIRAC Job ID' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac                              import Dirac, parseArguments

result = Dirac().killJob( parseArguments( args ) )
if result['OK']:
  print 'Killed jobs %s' % ','.join( [str( j ) for j in result['Value']] )
  exitCode = 0
else:
  print 'ERROR', result['Message']
  exitCode = 2

DIRAC.exit( exitCode )
