#!/usr/bin/env python
""" Test specific of JobParameters with and without the flag in for ES backend

  flag in /Operations/[]/Services/JobMonitoring/useESForJobParametersFlag
"""

from __future__ import print_function, absolute_import

import os
import time

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient

# sut
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient

jobMonitoringClient = JobMonitoringClient()
jobStateUpdateClient = JobStateUpdateClient()

# First, create a job

from DIRAC.tests.Integration.WorkloadManagementSystem.Test_Client_WMS import helloWorldJob, createFile
job = helloWorldJob()
jobDescription = createFile(job)

wmsClient = WMSClient()
res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
assert res['OK']
jobID = int(res['Value'])


# Now, adding some parameters

# Use the MySQL backend

res = jobStateUpdateClient.setJobParameter(jobID, 'ParName-fromMySQL', 'ParValue-fromMySQL')
assert res['OK']

res = jobMonitoringClient.getJobParameter(jobID, 'ParName-fromMySQL')  # This will be in MySQL
assert res['OK']
assert res['Value'] == {'ParName-fromMySQL': 'ParValue-fromMySQL'}

res = jobMonitoringClient.getJobOwner(jobID)
assert res['OK']
print(res)

# Here now setting the flag as the following inside /Operations/Defaults:
# in Operations/Defaults/Services/JobMonitoring/useESForJobParametersFlag

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
csAPI = CSAPI()

res = csAPI.createSection('Operations/Defaults/Services/')
if not res['OK']:
  print(res['Message'])
  exit(1)

res = csAPI.createSection('Operations/Defaults/Services/JobMonitoring/')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Operations/Defaults/Services/JobMonitoring/useESForJobParametersFlag', True)

csAPI.commit()

# Now we need to restart the services for the new configuration to be picked up

time.sleep(2)

os.system("dirac-restart-component WorkloadManagement JobMonitoring")
os.system("dirac-restart-component WorkloadManagement JobStateUpdate")

time.sleep(5)

# So now we are using the ES backend

# This will still be in MySQL
res = jobMonitoringClient.getJobParameter(jobID, 'ParName-fromMySQL')
assert res['OK']
assert res['Value'] == {'ParName-fromMySQL': 'ParValue-fromMySQL'}

# Now we insert
res = jobStateUpdateClient.setJobParameter(jobID, 'ParName-fromES', 'ParValue-fromES')
assert res['OK']

# sleep to give time to ES to index
time.sleep(2)

res = jobMonitoringClient.getJobParameter(jobID, 'ParName-fromES')  # This will be in ES
assert res['OK']
assert res['Value'] == {'ParName-fromES': 'ParValue-fromES'}

res = jobMonitoringClient.getJobOwner(jobID)
assert res['OK']
print(res)


# setJobsParameter

# getJobParameters

# getJobAttribute
