#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-get-job-pilot-output
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve the output of the pilot that executed a given job
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
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
    errorList.append( ( 'Expected integer for JobID', job ) )
    exitCode = 2
    continue

  result = diracAdmin.getJobPilotOutput( job )
  if not result['OK']:
    errorList.append( ( job, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRACExit( exitCode )
