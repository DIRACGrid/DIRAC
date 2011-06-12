#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-wms-job-status
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve status of the given DIRAC job
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

from DIRAC.Interfaces.API.Dirac  import Dirac
dirac = Dirac()
exitCode = 0

try:
  jobs = [ int( job ) for job in args ]
except Exception, x:
  print 'Expected integer for jobID'
  exitCode = 2
  DIRAC.exit( exitCode )


result = dirac.status( jobs )
if result['OK']:
  for job in result['Value']:
    print 'JobID=' + str( job ),
    for status in result['Value'][job]:
      print status + '=' + result['Value'][job][status] + ';',
    print
else:
  exitCode = 2
  print "ERROR: %s" % result['Message']

DIRAC.exit( exitCode )
