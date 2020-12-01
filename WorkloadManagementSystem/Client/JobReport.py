"""
  JobReport class encapsulates various methods of the job status reporting blah, blah, blah...

"""

from __future__ import print_function
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import Time, DEncode
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient


class JobReport(object):
  """
    .. class:: JobReport
  """

  def __init__(self, jobid, source=''):
    """ c'tor


    """
    self.jobStatusInfo = []
    self.appStatusInfo = []
    self.jobParameters = {}
    self.jobID = int(jobid)
    self.source = source
    if not source:
      self.source = 'Job_%d' % self.jobID

  def setJob(self, jobID):
    """ Set the job ID for which to send reports
    """
    self.jobID = jobID

  def setJobStatus(self, status='', minorStatus='', applicationStatus='', sendFlag=True):
    """ Send job status information to the JobState service for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    timeStamp = Time.toString()
    # add job status record
    self.jobStatusInfo.append((status.replace("'", ''), minorStatus.replace("'", ''), timeStamp))
    if applicationStatus:
      self.appStatusInfo.append((applicationStatus.replace("'", ''), timeStamp))
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
    if not isinstance(appStatus, str):
      appStatus = repr(appStatus)
    self.appStatusInfo.append((appStatus.replace("'", ''), timeStamp))
    if sendFlag:
      # and send
      return self.sendStoredStatusInfo()

    return S_OK()

  def setJobParameter(self, par_name, par_value, sendFlag=True):
    """ Send job parameter for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    timeStamp = Time.toString()
    # add job parameter record
    self.jobParameters[par_name] = (par_value, timeStamp)
    if sendFlag:
      # and send
      return self.sendStoredJobParameters()

    return S_OK()

  def setJobParameters(self, parameters, sendFlag=True):
    """ Send job parameters for jobID
    """
    if not self.jobID:
      return S_OK('Local execution, jobID is null.')

    timeStamp = Time.toString()
    # add job parameter record
    for pname, pvalue in parameters:
      self.jobParameters[pname] = (pvalue, timeStamp)

    if sendFlag:
      # and send
      return self.sendStoredJobParameters()

    return S_OK()

  def sendStoredStatusInfo(self):
    """ Send the job status information stored in the internal cache
    """

    statusDict = {}
    for status, minor, dtime in self.jobStatusInfo:
      # No need to send empty items in dictionary
      sDict = {}
      if status:
        sDict['Status'] = status
      if minor:
        sDict['MinorStatus'] = minor
      if sDict:
        sDict['Source'] = self.source
        statusDict[dtime] = sDict
    for appStatus, dtime in self.appStatusInfo:
      # No need to send empty items in dictionary
      if appStatus:
        statusDict[dtime] = {'ApplicationStatus': appStatus,
                             'Source': self.source}

    if statusDict:
      result = JobStateUpdateClient().setJobStatusBulk(self.jobID, statusDict)
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

    parameters = [[pname, value[0]] for pname, value in self.jobParameters.items()]
    if parameters:
      result = JobStateUpdateClient().setJobParameters(self.jobID, parameters)
      if result['OK']:
        # Empty the internal parameter container
        self.jobParameters = {}
      return result
    else:
      return S_OK('Empty')

  def commit(self):
    """ Send all the accumulated information
    """

    success = True
    result = self.sendStoredStatusInfo()
    success &= result['OK']
    result = self.sendStoredJobParameters()
    success &= result['OK']

    if success:
      return S_OK()
    return S_ERROR('Information upload to JobStateUpdate service failed')

  def dump(self):
    """ Print out the contents of the internal cached information
    """

    print("Job status info:")
    for status, minor, timeStamp in self.jobStatusInfo:
      print(status.ljust(20), minor.ljust(30), timeStamp)

    print("Application status info:")
    for status, timeStamp in self.appStatusInfo:
      print(status.ljust(20), timeStamp)

    print("Job parameters:")
    for pname, value in self.jobParameters.items():
      pvalue, timeStamp = value
      print(pname.ljust(20), pvalue.ljust(30), timeStamp)

  def generateForwardDISET(self):
    """ Generate and return failover requests for the operations in the internal cache
    """
    forwardDISETOp = None

    result = self.sendStoredStatusInfo()
    if not result['OK']:
      gLogger.error("Error while sending the job status", result['Message'])
      if 'rpcStub' in result:

        rpcStub = result['rpcStub']

        forwardDISETOp = Operation()
        forwardDISETOp.Type = "ForwardDISET"
        forwardDISETOp.Arguments = DEncode.encode(rpcStub)

      else:
        return S_ERROR('Could not create ForwardDISET operation')

    return S_OK(forwardDISETOp)
