# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Client/JobReport.py,v 1.15 2008/07/25 09:07:35 rgracian Exp $

"""
  JobReport class encapsulates various
  methods of the job status reporting
"""

__RCSID__ = "$Id: JobReport.py,v 1.15 2008/07/25 09:07:35 rgracian Exp $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_OK, S_ERROR, Time
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

    timeStamp = Time.toString()
    # add job status record
    self.jobStatusInfo.append((status,minor,timeStamp))
    if sendFlag:
      # and send
      return self.sendStoredStatusInfo()

    return S_OK()

  def setApplicationStatus(self, appStatus, sendFlag=True):
    """ Send application status information to the JobState service for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    timeStamp = Time.toString()
    # add Application status record
    self.appStatusInfo.append((appStatus,timeStamp))
    if sendFlag:
      # and send
      return self.sendStoredStatusInfo()

    return S_OK()

  def setJobParameter(self,par_name,par_value, sendFlag = True):
    """ Send job parameter for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    timeStamp = Time.toString()
    # add job paramenter record
    self.jobParameters.append((par_name,par_value,timeStamp))
    if sendFlag:
      # and send
      return self.sendStoredJobParameters()

    return S_OK()

  def setJobParameters(self, parameters, sendFlag = True):
    """ Send job parameters for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    timeStamp = Time.toString()
    # add job paramenter record
    for pname,pvalue in parameters:
      self.jobParameters.append((pname,pvalue,timeStamp))

    if sendFlag:
      # and send
      return self.sendStoredJobParameters()

  def sendStoredStatusInfo(self):
    """ Send the job status information stored in the internal cache
    """

    statusDict = {}
    for status,minor,dtime in self.jobStatusInfo:
      statusDict[dtime] = {'Status':status,
                                                       'MinorStatus':minor,
                                                       'ApplicationStatus':'',
                                                       'Source': self.source}
    for appStatus,dtime in self.appStatusInfo:
      statusDict[dtime] = {'Status':'',
                                                       'MinorStatus':'',
                                                       'ApplicationStatus':appStatus,
                                                       'Source': self.source}

    if statusDict:
      jobMonitor = RPCClient('WorkloadManagement/JobStateUpdate',timeout=60)
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
    # FIXME: What if the same paramenter is set twice?
    parameters = []
    for pname,pvalue,timeStamp in self.jobParameters:
      parameters.append((pname,pvalue))

    if parameters:
      jobMonitor = RPCClient('WorkloadManagement/JobStateUpdate',timeout=60)
      result = jobMonitor.setJobParameters(self.jobID,parameters)
      if not result:
        return S_ERROR('Null result from JobState service')

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
      print status.ljust(20),minor.ljust(30),timeStamp

    print "Application status info:"
    for status,timeStamp in self.appStatusInfo:
      print status.ljust(20),timeStamp

    print "Job parameters:"
    for pname,pvalue,timeStamp in self.jobParameters:
      print pname.ljust(20),pvalue.ljust(30),timeStamp

  def generateRequest(self):
    """ Generate failover requests for the operations in the internal cache
    """

    request = RequestContainer()
    result = self.sendStoredStatusInfo()
    if not result['OK'] and result.has_key('rpcStub'):
      request.addSubRequest(DISETSubRequest(result['rpcStub']).getDictionary(),'jobstate')
    else:
      return S_ERROR('Could not create job state sub-request')

    result = self.sendStoredJobParameters()
    if not result['OK'] and result.has_key('rpcStub'):
      request.addSubRequest(DISETSubRequest(result['rpcStub']).getDictionary(),'jobparameters')
    else:
      return S_ERROR('Could not create job parameters sub-request')

    if request.isEmpty()['Value']:
      request = None

    return S_OK(request)
