#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-submit-pilot-for-job
# Author :  Ricardo Graciani
########################################################################
__RCSID__ = "$Id$"
import sys
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

jobdb = JobDB()
tqdb = TaskQueueDB()

result = jobdb.selectJobs( { 'Status' : [ 'Received', 'Checking', 'Waiting' ] } )
if not result[ 'OK' ]:
  print result[ 'Message' ]
  sys.exit( 1 )
jobList = result[ 'Value' ]
print tqdb.forceRecreationOfTables()
for job in jobList:
  result = jobdb.getJobAttribute( job, 'RescheduleCounter' )
  if not result[ 'OK' ]:
    print "Cannot get reschedule counter for job %s" % job
    rC = 0
  rC = result[ 'Value' ]
  if rC >= jobdb.maxRescheduling:
    jobdb.setJobAttribute( job, "RescheduleCounter", "0" )
  jobdb.rescheduleJob( job )
  jobdb.setJobAttribute( job, "RescheduleCounter", rC )
sys.exit( 0 )
