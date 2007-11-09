########################################################################
# $Id: JobStateUpdateHandler.py,v 1.2 2007/11/09 08:13:50 atsareg Exp $
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
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB,JobLoggingDB

# This is a global instance of the JobDB class
jobDB = False

def initializeJobStateUpdateHandler( serviceInfo ):

  global jobDB
  global logDB
  jobDB = JobDB()
  logDB = JobLoggingDB()
  return S_OK()

class JobStateUpdateHandler( RequestHandler ):

    ###########################################################################
  types_setJobStatus = [IntType,StringType,StringType,StringType,StringType]
  def export_setJobStatus(self,jobId,status,minorStatus,source='Unknown',datetime=None):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """

    if status:
      result = jobDB.setJobAttribute(jobId,'Status',status)
      if result['Status'] != "OK":
        return result
    if minorStatus:
      result = jobDB.setJobAttribute(jobId,'MinorStatus',minorStatus,True)
      if result['Status'] != "OK":
        return result

    result = jobDB.getJobsAttributes([jobId], ['Status','MinorStatus'] )
    status = result['Value'][0]['Status']
    minorStatus = result['Value'][0]['MinorStatus']
    if date_time:
      date = date_time.split()[0]
      time = date_time.split()[1]
      result = jobDB.addLoggingRecord(jobId,status+'/'+minorStatus,date,time,source)
    else:
      result = jobDB.addLoggingRecord(jobId,status+'/'+minorStatus,source=source)

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
  types_sendSignOfLife = [IntType]
  def export_sendSignOfLife(self,jobId):
    """ Send a heart beat sign of life for a job jobId
    """

    result = jobDB.getJobsAttributes([jobId], ['Status'] )
    if result['Status'] != "OK":
      return result

    status = result['Value'][0]['Status']
    if status == "stalled":
      result = jobDB.setJobAttribute(jobId,'Status','running',True)

  ###########################################################################
  types_deleteJob = [IntType]
  def export_deleteJob(self,jobId):
    """ Delete job jobId
    """

    result = jobDB.deleteJobFromQueue(jobId)

    result = jobDB.getJobsAttributes([jobId], ['MinorStatus'] )
    minorStatus = result['Value'][0]['MinorStatus']

    result = jobDB.setJobAttribute(jobId,'Status','deleted',True)
    result = jobDB.addLoggingRecord(jobId,'deleted/'+minorStatus,source='JobStateService')

    return S_OK()
