#!/usr/bin/env python
########################################################################
# $HeadURL$
# File : dirac-production-job-delete
# Author : Stuart Paterson
########################################################################
"""
Delete DIRAC job from WMS, if running it will be killed
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     ' %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                     'Arguments:',
                                     ' JobID: DIRAC Job ID' ] ) )

Script.registerSwitch( "f:", "File=", "Get output for jobs with IDs from the file" )
Script.registerSwitch( "g:", "JobGroup=", "Get output for jobs in the given group" )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

import os.path

if __name__ == "__main__":
  
  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Core.Utilities.Time import toString, date, day
  dirac = Dirac()
  exitCode = 0
  errorList = []
  
  jobs = []
  for sw, value in Script.getUnprocessedSwitches():
    if sw.lower() in ( 'f', 'file' ):
      if os.path.exists( value ):
        jFile = open( value )
        jobs += jFile.read().split()
        jFile.close()
    elif sw.lower() in ( 'g', 'jobgroup' ):    
      group = value    
      jobDate = toString( date() - 30*day )
      result = dirac.selectJobs( jobGroup = value, date = jobDate )
      if not result['OK']:
        if not "No jobs selected" in result['Message']:
          print "Error:", result['Message']
          DIRAC.exit( -1 )
      else:
        jobs += result['Value']
  
  for arg in args:
    jobs.append(arg)
  
  if not jobs:
    print "Warning: no jobs selected"
    Script.showHelp()
    DIRAC.exit( 0 )
  
  for job in jobs:
  
    result = dirac.delete( job )
    if result['OK']:
      print 'Deleted job %s' % ( result['Value'][0] )
    else:
      errorList.append( ( job, result['Message'] ) )
      exitCode = 2
  
  for error in errorList:
    print "ERROR %s: %s" % error
  
  DIRAC.exit( exitCode )
