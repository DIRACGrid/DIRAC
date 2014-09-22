#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-get-job-pilots
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve info about pilots that have matched a given Job
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JobID' % Script.scriptName,
                                     'Arguments:',
                                     '  JobID:    DIRAC ID of the Job' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

from DIRAC import exit as DIRACExit
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

for job in args:

  try:
    job = int( job )
  except Exception, x:
    errorList.append( ( job, 'Expected integer for jobID' ) )
    exitCode = 2
    continue

  result = diracAdmin.getJobPilots( job )
  if not result['OK']:
    errorList.append( ( job, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRACExit( exitCode )
