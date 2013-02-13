#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-wms-job-get-output
# Author : Stuart Paterson
########################################################################
"""
  Retrieve output sandbox for a DIRAC job
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

import os, shutil, datetime

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                     'Arguments:',
                                     '  JobID:    DIRAC Job ID or a name of the file with JobID per line' ] ) )

Script.registerSwitch( "D:", "Dir=", "Store the output in this directory" )
Script.registerSwitch( "f:", "File=", "Get output for jobs with IDs from the file" )
Script.registerSwitch( "g:", "JobGroup=", "Get output for jobs in the given group" )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.Dirac  import Dirac
from DIRAC.Core.Utilities.Time import toString, date, day

dirac = Dirac()
exitCode = 0
errorList = []

outputDir = None
group = None
jobs = []
for sw, value in Script.getUnprocessedSwitches():
  if sw in ( 'D', 'Dir' ):
    outputDir = value
  elif sw.lower() in ( 'f', 'file' ):
    if os.path.exists( value ):
      jFile = open( value )
      jobs += jFile.read().split()
      jFile.close()
  elif sw.lower() in ( 'g', 'jobgroup' ):    
    group = value
    jobDate = toString( date() - 30*day )
    
    # Choose jobs in final state, no more than 30 days old
    result = dirac.selectJobs( jobGroup=value, date=jobDate, status='Done' )
    if not result['OK']:
      if not "No jobs selected" in result['Message']:
        print "Error:", result['Message']
        DIRAC.exit( -1 )
    else:    
      jobs += result['Value']      
    result = dirac.selectJobs( jobGroup=value, date=jobDate, status='Failed' )
    if not result['OK']:
      if not "No jobs selected" in result['Message']:
        print "Error:", result['Message']
        DIRAC.exit( -1 )
    else:
      jobs += result['Value']      

for arg in args:
  if os.path.isdir(arg):  
    print "Output for job %s already retrieved, remove the output directory to redownload" % arg
  else:
    jobs.append(arg)

if not jobs:
  print "No jobs selected"
  DIRAC.exit( 0 )

if group:
  if outputDir:
    outputDir = os.path.join(outputDir,group)
  else:
    outputDir = group  

if outputDir: 
  if not os.path.exists(outputDir):
    os.makedirs( outputDir)
else:
  outputDir = os.getcwd()    

jobs = [ str(job) for job in jobs ]
doneJobs = os.listdir( outputDir )
todoJobs = [ job for job in jobs if not job in doneJobs ]
  
for job in todoJobs:

  result = dirac.getOutputSandbox( job, outputDir = outputDir )
  
  jobDir = str(job)
  if outputDir:
    jobDir = os.path.join( outputDir, job )
  if result['OK']:
    if os.path.exists( jobDir ):
      print 'Job output sandbox retrieved in %s/' % ( jobDir )
  else:
    if os.path.exists( '%s' % jobDir ):
      shutil.rmtree( jobDir )
    errorList.append( ( job, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
