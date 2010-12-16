#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-production-job-delete
# Author :  Stuart Paterson
########################################################################
"""
  Delete DIRAC job from WMS, if running it will be killed
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

from DIRAC.Interfaces.API.Dirac                              import Dirac
dirac = Dirac()
exitCode = 0
errorList = []

for job in args:

  result = dirac.delete( job )
  if result['OK']:
    print 'Deleted job %s' % ( result['Value'][0] )
  else:
    errorList.append( ( job, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
