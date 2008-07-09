# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Client/JobReport.py,v 1.10 2008/07/09 11:50:23 paterson Exp $

"""
  JobReport class encapsulates various
  methods of the job status reporting
"""

__RCSID__ = "$Id: JobReport.py,v 1.10 2008/07/09 11:50:23 paterson Exp $"

import datetime
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest


class JobReport:

  def __init__(self, jobid, source=''):

    self.jobStatusInfo = []
    self.appStatusInfo = []
    self.jobParameters = []
    self.jobID = int(jobid)
    self.source = source
    if not source:
      self.source = 'Job_%d' % self.jobID

  def setJob(self,jobID):
    """ Set the job ID for which to send reports
    """

    self.jobID = jobID

  def setJobStatus(self, status='', minor='', sendFlag=True):
    """ Send job status information to the JobState service for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    if not sendFlag:
      # add job status record
      timeStamp = datetime.datetime.utcnow()
      self.jobStatusInfo.append((status,minor,timeStamp))
      return S_OK()

    if self.jobStatusInfo or self.appStatusInfo:
      # add new status record and try to send them all
      timeStamp = datetime.datetime.utcnow()
      self.jobStatusInfo.append((status,minor,timeStamp))
      result = self.sendStoredJobStatusInfo()
      return result
    else:
      # send the new status record
      jobMonitor = RPCClient('WorkloadManagement/JobStateUpdate',timeout=10)
      result = jobMonitor.setJobStatus(self.jobID,status,minor,self.source)
      if result['OK']:
        return result

    # add new job status record
    timeStamp = datetime.datetime.utcnow()
    self.jobStatusInfo.append((status,minor,timeStamp))
    return S_ERROR('Failed to update the job status')

  def setApplicationStatus(self, appStatus, sendFlag=True):
    """ Send application status information to the JobState service for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    if not sendFlag:
      # add job status record
      timeStamp = datetime.datetime.utcnow()
      self.appStatusInfo.append((appStatus,timeStamp))
      return S_OK()

    if self.jobStatusInfo or self.appStatusInfo:
      # add new application status record and try to send them all
      timeStamp = datetime.datetime.utcnow()
      self.appStatusInfo.append((appStatus,timeStamp))
      result = self.sendStoredStatusInfo()
      return result
    else:
      jobMonitor = RPCClient('WorkloadManagement/JobStateUpdate',timeout=10)
      result = jobMonitor.setJobApplicationStatus(self.jobID,appStatus,self.source)
      if result['OK']:
        return result

    # add job status record
    timeStamp = datetime.datetime.utcnow()
    self.appStatusInfo.append((appStatus,timeStamp))
    return S_ERROR('Failed to update the application status')

  def setJobParameter(self,par_name,par_value, sendFlag = True):
    """ Send job parameter for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    if not sendFlag:
      # add job status record
      timeStamp = datetime.datetime.utcnow()
      self.jobParameters.append((par_name,par_value,timeStamp))
      return S_OK()

    if self.jobParameters:
      timeStamp = datetime.datetime.utcnow()
      self.jobParameters.append((par_name,par_value,timeStamp))
      result = self.sendStoredJobParameters()
      return result
    else:
      jobMonitor = RPCClient('WorkloadManagement/JobStateUpdate',timeout=10)
      result = jobMonitor.setJobParameter(self.jobID,par_name,par_value)
      if result['OK']:
        return result

    # add job status record
    timeStamp = datetime.datetime.utcnow()
    self.jobParameters.append((par_name,par_value,timeStamp))
    return S_ERROR('Failed to send parameters')

  def setJobParameters(self, parameters, sendFlag = True):
    """ Send job parameters for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    if not sendFlag:
      # add job status record
      timeStamp = datetime.datetime.utcnow()
      for pname,pvalue in parameters:
        self.jobParameters.append((pname,pvalue,timeStamp))
      return S_OK()

    if self.jobParameters:
      timeStamp = datetime.datetime.utcnow()
      for pname,pvalue in parameters:
        self.jobParameters.append((pname,pvalue,timeStamp))
      result = self.sendStoredJobParameters()
      return result
    else:
      jobMonitor = RPCClient('WorkloadManagement/JobStateUpdate',timeout=10)
      result = jobMonitor.setJobParameters(self.jobID,parameters)
      if result['OK']:
        return result

    # add job status record
    timeStamp = datetime.datetime.utcnow()
    for pname,pvalue in parameters:
      self.jobParameters.append((pname,pvalue,timeStamp))
    return S_ERROR('Failed to send parameters')

  def sendStoredStatusInfo(self):
    """ Send the job status information stored in the internal cache
    """

    statusDict = {}
    for status,minor,dtime in self.jobStatusInfo:
      statusDict[str(dtime.replace(microsecond=0))] = {'Status':status,
                                                       'MinorStatus':minor,
                                                       'ApplicationStatus':'',
                                                       'Source':"Job_%d" % self.jobID}
    for appStatus,dtime in self.appStatusInfo:
      statusDict[str(dtime.replace(microsecond=0))] = {'Status':'',
                                                       'MinorStatus':'',
                                                       'ApplicationStatus':appStatus,
                                                       'Source':"Job_%d" % self.jobID}

    if statusDict:
      jobMonitor = RPCClient('WorkloadManagement/JobStateUpdate',timeout=10)
      result = jobMonitor.setJobStatusBulk(self.jobID,statusDict)
#      result['OK'] =  False
#      result['Message'] = ''
#      print result
      if not result:
        return S_ERROR('Null result from JobState service')

      if result['OK']:
        # Empty the internal status containers
        self.jobStatusInfo = []
        self.appStatusInfo = []

      return result

    else:
      return S_OK('Empty')

  def sendStoredJobParameters(self):
    """ Send the job parameters stored in the internal cache
    """

    parameters = []
    for pname,pvalue,timeStamp in self.jobParameters:
      parameters.append((pname,pvalue))

    if parameters:
      jobMonitor = RPCClient('WorkloadManagement/JobStateUpdate',timeout=10)
      if not result:
        return S_ERROR('Null result from JobState service')
      result = jobMonitor.setJobParameters(self.jobID,parameters)
      if result['OK']:
        # Empty the internal parameter container
        self.jobParameters = []

      return result
    else:
      return S_OK('Empty')

  def dump(self):
    """ Print out the contents of the internal cached information

    """

    print "Job status info:"
    for status,minor,timeStamp in self.jobStatusInfo:
      print status.ljust(20),minor.ljust(30),str(timeStamp)

    print "Application status info:"
    for status,timeStamp in self.appStatusInfo:
      print status.ljust(20),str(timeStamp)

    print "Job parameters:"
    for pname,pvalue,timeStamp in self.jobParameters:
      print pname.ljust(20),pvalue.ljust(30),str(timeStamp)

  def generateRequest(self):
    """ Generate failover requests for the operations in the internal cache
    """

    request = RequestContainer()
    result = self.sendStoredStatusInfo()
    if not result['OK']:
      request.addSubRequest(DISETSubRequest(result['rpcStub']).getDictionary(),'jobstate')

    result = self.sendStoredJobParameters()
    if not result['OK']:
      request.addSubRequest(DISETSubRequest(result['rpcStub']).getDictionary(),'jobparameters')

    if request.isEmpty()['Value']:
      request = None

    return S_OK(request)
