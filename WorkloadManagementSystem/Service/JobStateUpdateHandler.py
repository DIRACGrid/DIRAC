########################################################################
# $Id: JobStateUpdateHandler.py,v 1.4 2007/12/07 08:56:31 paterson Exp $
########################################################################

""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface
    
    setJobStatus()

"""

__RCSID__ = "$Release:  $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB

# This is a global instance of the JobDB class
jobDB = False
logDB = False

def initializeJobStateUpdateHandler( serviceInfo ):

  global jobDB
  global logDB
  jobDB = JobDB()
  logDB = JobLoggingDB()
  return S_OK()

class JobStateUpdateHandler( RequestHandler ):

  ###########################################################################
  types_setJobStatus = [IntType,StringType,StringType,StringType,StringType]
  def export_setJobStatus(self,jobID,status,minorStatus,source='Unknown',datetime=None):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """

    if status:
      result = jobDB.setJobAttribute(jobID,'Status',status)
      if not result['OK']:
        return result
    if minorStatus:
      result = jobDB.setJobAttribute(jobID,'MinorStatus',minorStatus,True)
      if not result['OK']:
        return result

    result = jobDB.getJobAttributes(jobID, ['Status','MinorStatus'] )
    if not result['OK']:
      return result

    status = result['Value']['Status']
    minorStatus = result['Value']['MinorStatus']
    if datetime:
      date = datetime.split()[0]
      time = datetime.split()[1]
      result = logDB.addLoggingRecord(jobID,status,minorStatus,date,time,source)
    else:
      result = logDB.addLoggingRecord(jobID,status,minorStatus,source=source)

    return result

  ###########################################################################
  types_setJobApplicationStatus = [IntType,StringType]
  def export_setJobApplicationStatus(self,jobId,status):
    """ Set the application status for job specified by its JobId.
    """

    result = jobDB.setJobAttribute(jobId,'ApplicationStatus',status)
    if result['Status'] != "OK":
      return result

    return S_OK()

  ###########################################################################
  types_setJobParameter = [IntType,StringType,StringType]
  def export_setJobParameter(self,jobId,name,value):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """

    result = jobDB.setJobParameter(jobId,name,value)

    return result

  ###########################################################################
  types_setJobParameters = [IntType,ListType]
  def export_setJobParameters(self,jobId,parameters):
    """ Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
    """

    OK = True
    for name,value in parameters:
      result = jobDB.setJobParameter(jobId,name,value)
      if result['Status'] != "OK":
        OK = False

    if OK:
      return S_OK()
    else:
      return S_ERROR('Failed to store some of the parameters')

  ###########################################################################
  types_sendHeartBeat = [IntType]
  def export_sendHeartBeat(self,jobId):
    """ Send a heart beat sign of life for a job jobId
    """

    result = jobDB.getJobsAttributes([jobId], ['Status'] )
    if result['Status'] != "OK":
      return result

    status = result['Value'][0]['Status']
    if status == "stalled":
      result = jobDB.setJobAttribute(jobId,'Status','running',True)

