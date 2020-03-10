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

from DIRAC.tests.Integration.WorkloadManagementSystem.Test_Client_WMS import helloWorldJob, createFile

jobMonitoringClient = JobMonitoringClient()
jobStateUpdateClient = JobStateUpdateClient()


def createJob():

  job = helloWorldJob()
  jobDescription = createFile(job)

  wmsClient = WMSClient()
  res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
  assert res['OK'], res['Message']
  jobID = int(res['Value'])
  return jobID


def updateFlag():
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


def test_MySQLandES_jobParameters():
  """ a basic put - remove test, changing the flag in between
  """

  # First, create a job
  jobID = createJob()

  # Use the MySQL backend

  res = jobStateUpdateClient.setJobParameter(jobID, 'ParName-fromMySQL', 'ParValue-fromMySQL')
  assert res['OK'], res['Message']

  res = jobMonitoringClient.getJobParameter(jobID, 'ParName-fromMySQL')  # This will be looked up in MySQL only
  assert res['OK'], res['Message']
  assert res['Value'] == {'ParName-fromMySQL': 'ParValue-fromMySQL'}, res['Value']

  res = jobMonitoringClient.getJobParameters(jobID)  # This will be looked up in MySQL only
  assert res['OK'], res['Message']
  assert isinstance(res['Value'], dict), res['Value']
  assert res['Value'] == {jobID: {'ParName-fromMySQL': 'ParValue-fromMySQL'}}, res['Value']

  res = jobMonitoringClient.getJobOwner(jobID)
  assert res['OK'], res['Message']
  assert res['Value'] == 'adminusername', res['Value']

  res = jobStateUpdateClient.setJobsParameter({jobID: ['SomeStatus', 'Waiting']})
  assert res['OK'], res['Message']

  res = jobMonitoringClient.getJobParameters(jobID)  # This will be looked up in MySQL only
  assert res['OK'], res['Message']
  assert res['Value'] == {jobID: {'ParName-fromMySQL': 'ParValue-fromMySQL', 'SomeStatus': 'Waiting'}}, res['Value']

  res = jobMonitoringClient.getJobAttributes(jobID)
  assert res['OK'], res['Message']

  # changing to use the ES flag
  updateFlag()
  # So now we are using the ES backend

  # This will still be in MySQL, but first it will look if it's in ES
  res = jobMonitoringClient.getJobParameter(jobID, 'ParName-fromMySQL')
  assert res['OK'], res['Message']
  assert res['Value'] == {'ParName-fromMySQL': 'ParValue-fromMySQL'}, res['Value']

  # Now we insert (in ES)
  res = jobStateUpdateClient.setJobParameter(jobID, 'ParName-fromES', 'ParValue-fromES')
  time.sleep(2)  # sleep to give time to ES to index
  assert res['OK'], res['Message']

  res = jobMonitoringClient.getJobParameter(jobID, 'ParName-fromES')  # This will be in ES
  assert res['OK'], res['Message']
  assert res['Value'] == {'ParName-fromES': 'ParValue-fromES'}, res['Value']

  res = jobMonitoringClient.getJobOwner(jobID)
  assert res['OK'], res['Message']
  assert res['Value'] == 'adminusername', res['Value']

  # These parameters will be looked up in MySQL and in ES, and combined
  res = jobMonitoringClient.getJobParameters(jobID)
  assert res['OK'], res['Message']
  assert res['Value'] == {jobID: {'ParName-fromMySQL': 'ParValue-fromMySQL', 'SomeStatus': 'Waiting',
				  'ParName-fromES': 'ParValue-fromES'}}, res['Value']

  # Do it again
  res = jobMonitoringClient.getJobParameters(jobID)
  assert res['OK'], res['Message']
  assert res['Value'] == {jobID: {'ParName-fromMySQL': 'ParValue-fromMySQL', 'SomeStatus': 'Waiting',
				  'ParName-fromES': 'ParValue-fromES'}}, res['Value']

  # this is updating an existing parameter, but in practice it will be in ES only,
  # while in MySQL the old status "Waiting" will stay
  res = jobStateUpdateClient.setJobsParameter({jobID: ['SomeStatus', 'Matched']})
  time.sleep(2)  # sleep to give time to ES to index
  assert res['OK'], res['Message']

  res = jobMonitoringClient.getJobParameters(jobID)
  assert res['OK'], res['Message']
  assert res['Value'][jobID]['SomeStatus'] == 'Matched', res['Value']

  # again updating the same parameter
  res = jobStateUpdateClient.setJobsParameter({jobID: ['SomeStatus', 'Running']})
  time.sleep(2)  # sleep to give time to ES to index
  assert res['OK'], res['Message']

  res = jobMonitoringClient.getJobParameters(jobID)
  assert res['OK'], res['Message']
  assert res['Value'][jobID]['SomeStatus'] == 'Running', res['Value']

  # Now we create a second job
  secondJobID = createJob()

  res = jobMonitoringClient.getJobParameter(secondJobID, 'ParName-fromMySQL')
  assert res['OK'], res['Message']

  # Now we insert (in ES)
  res = jobStateUpdateClient.setJobParameter(secondJobID, 'ParName-fromES-2', 'ParValue-fromES-2')
  time.sleep(2)  # sleep to give time to ES to index
  assert res['OK'], res['Message']

  res = jobMonitoringClient.getJobParameter(secondJobID, 'ParName-fromES-2')  # This will be in ES
  assert res['OK'], res['Message']
  assert res['Value'] == {'ParName-fromES-2': 'ParValue-fromES-2'}, res['Value']

  # These parameters will be looked up in MySQL and in ES, and combined
  res = jobMonitoringClient.getJobParameters([jobID, secondJobID])
  assert res['OK'], res['Message']
  assert res['Value'] == {jobID: {'ParName-fromMySQL': 'ParValue-fromMySQL', 'SomeStatus': 'Running',
                                  'ParName-fromES': 'ParValue-fromES'},
			  secondJobID: {'ParName-fromES-2': 'ParValue-fromES-2'}}, res['Value']

  # These parameters will be looked up in MySQL and in ES, and combined
  res = jobMonitoringClient.getJobParameters([jobID, secondJobID], 'SomeStatus')
  assert res['OK'], res['Message']
  assert res['Value'][jobID] == {'SomeStatus': 'Running'}, res['Value']

  res = jobMonitoringClient.getJobAttributes(jobID)  # these will still be all in MySQL
  assert res['OK'], res['Message']
