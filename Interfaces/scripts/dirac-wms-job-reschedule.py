#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-wms-job-delete
# Author :  Stuart Paterson
########################################################################
"""
  Reschedule the given DIRAC job
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
dirac = Dirac()
exitCode = 0
errorList = []

for job in parseArguments( args ):

  result = dirac.rescheduleJob( job )
  if result['OK']:
    print 'Rescheduled job %s' % ( result['Value'][0] )
  else:
    errorList.append( ( job, result['Message'] ) )
    print result['Message']
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
