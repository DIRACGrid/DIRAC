# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Client/JobReport.py,v 1.1 2008/04/29 22:41:27 atsareg Exp $

"""
  JobReport class encapsulates various
  methods of the job status reporting
"""

__RCSID__ = "$Id: JobReport.py,v 1.1 2008/04/29 22:41:27 atsareg Exp $"

import datetime
from DIRAC.Core.DISET.RPCClient import RPCClient

class JobReport:

  def __init__(self, url, jobid = None):

    self.jobMonitor = RPCClient('WorkloadManagement/JobMonitoring')
    self.jobStatusInfo = []
    self.appStatusInfo = []
    self.jobParameters = []

  def setJobStatus(self,jobID, status='', minor=''):
    """ Send job status information to the JobState service for jobID
    """

    result = self.jobMonitor.setJobStatus(jobID,status,minor,'Job_%d' % jobID)
    if result['OK']:
      return result

    # add job status record
    timeStamp = datetime.datetime.utcnow()
    self.statusInfo.append((jobID,status,minor,timeStamp))
    return S_ERROR('Failed to update the job status')

  def setApplicationStatus(self,jobID, appStatus):
    """ Send application status information to the JobState service for jobID
    """

    result = self.jobMonitor.setJobApplicationStatus(jobID,appStatus,'Job_%d' % jobID)
    if result['OK']:
      return result

    # add job status record
    timeStamp = datetime.datetime.utcnow()
    self.appStatusInfo.append((jobID,appStatus,timeStamp))
    return S_ERROR('Failed to update the application status')

  def setJobParameter(self,jobid,par_name,par_value):
    """ Send job parameter for jobID
    """

    result = self.jobMonitor.setJobParameter(jobID,par_name,par_value)
    if result['OK']:
      return result

    # add job status record
    timeStamp = datetime.datetime.utcnow()
    self.jobParameters.append((jobID,par_name,par_value,timeStamp))
    return S_ERROR('Failed to send parameters')

  def setJobParameters(self,jobID, parameters):
    """ Send job parameters for jobID
    """

    result = self.jobMonitor.setJobParameters(jobID,parameters)
    if result['OK']:
      return result

    # add job status record
    timeStamp = datetime.datetime.utcnow()
    for pname,pvalue in parameters:
      self.jobParameters.append((jobID,pname,pvalue,timeStamp))
    return S_ERROR('Failed to send parameters')

  def sendStoredJobStatusInfo(self):
    """ Send the job status information stored in the internal cache
    """

    statusDict = {}
    jobID = 0
    for jID,status,minor,dtime in self.jobStatusInfo:
      jobID = jID
      statusDict[str(dtime.replace(microsecond=0))] = {'Status':status,
                                                       'MinorStatus':minor,
                                                       'ApplicationStatus':''}
    for jID,appStatus,dtime in self.appStatusInfo:
      jobID = jID
      statusDict[str(dtime.replace(microsecond=0))] = {'Status':'',
                                                       'MinorStatus':'',
                                                       'ApplicationStatus':appStatus}

    if statusDict:
      result = self.jobMonitor.sendJobStatusBulk(jobID,statusDict)
      if result['OK']:
        # Empty the internal status containers
        self.jobStatusInfo = []
        self.appStatusInfo = []
        return result

      return S_ERROR('Failed to send bulk job status info')
    else:
      return S_OK()

  def sendStoredJobParameters(self):
    """ Send the job parameters stored in the interna cache
    """

    parameters = []
    jobID = 0
    for jobID,pname,pvalue,timeStamp in self.jobParameters:
      parameters.append((pname,pvalue))

    if parameters:
      result = self.jobMonitor.setJobParameters(jobID,parameters)
      if result['OK']:
        # Empty the internal parameter container
        self.jobParameters = []
        return result

      return S_ERROR('Failed to send job parameters')
    else:
      return S_OK()