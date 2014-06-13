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

import os
import DIRAC
from DIRAC import exit as DIRACExit
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Time import toString, date, day

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     '\nUsage:\n',
                                     '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                     '\nArguments:\n',
                                     '  JobID:    DIRAC Job ID' ] ) )

Script.registerSwitch( "f:", "File=", "Get status for jobs with IDs from the file" )
Script.registerSwitch( "g:", "JobGroup=", "Get status for jobs in the given group" )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.Dirac  import Dirac
dirac = Dirac()
exitCode = 0

jobs = []
for key, value in Script.getUnprocessedSwitches():  
  if key.lower() in ( 'f', 'file' ):
    if os.path.exists( value ):
      jFile = open( value )
      jobs += jFile.read().split()
      jFile.close()
  elif key.lower() in ( 'g', 'jobgroup' ):    
    jobDate = toString( date() - 30*day )
    # Choose jobs no more than 30 days old
    result = dirac.selectJobs( jobGroup=value, date=jobDate )    
    if not result['OK']:
      print "Error:", result['Message']
      DIRACExit( -1 )
    jobs += result['Value']  
        
if len( args ) < 1 and not jobs:
  Script.showHelp()

if len(args) > 0:
  jobs += args

try:
  jobs = [ int( job ) for job in jobs ]
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
