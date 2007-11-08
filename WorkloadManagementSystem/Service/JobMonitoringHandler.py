########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/JobMonitoringHandler.py,v 1.2 2007/11/08 08:11:53 atsareg Exp $
########################################################################

""" JobMonitoringHandler is the implementation of the JobMonitoring service
    in the DISET framework

    The following methods are available in the Service interface



"""

__RCSID__ = "$Id: JobMonitoringHandler.py,v 1.2 2007/11/08 08:11:53 atsareg Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

# This is a global instance of the JobDB class
jobDB = False
proxyRepository = False

def initializeJobMonitoringHandler( serviceInfo ):

  global jobDB
  jobDB = JobDB()
  return S_OK()

class JobMonitoringHandler( RequestHandler ):


##############################################################################
  types_getApplicationStates = []
  def export_getApplicationStates (self):
    """ Return Distict Values of ApplicationStatus job Attribute in WMS
    """
    return jobDB.getDistinctJobAttributes( 'ApplicationStatus' )

##############################################################################
  types_getJobTypes = []
  def export_getJobTypes (self):
    """ Return Distict Values of JobType job Attribute in WMS
    """
    return jobDB.getDistinctJobAttributes( 'JobType' )

##############################################################################
  types_getOwners = []
  def export_getOwners (self):
    """
    Return Distict Values of Owner job Attribute in WMS
    """
    return jobDB.getDistinctJobAttributes( 'Owner' )

##############################################################################
  types_getProductionIds = []
  def export_getProductionIds (self):
    """
    Return Distict Values of ProductionId job Attribute in WMS
    """
    return jobDB.getDistinctJobAttributes( 'JobGroup' )

##############################################################################
  types_getSites = []
  def export_getSites (self):
    """
    Return Distict Values of Site job Attribute in WMS
    """
    return jobDB.getDistinctJobAttributes( 'Site' )

##############################################################################
  types_getStates = []
  def export_getStates (self):
    """
    Return Distict Values of Status job Attribute in WMS
    """
    return jobDB.getDistinctJobAttributes( 'Status' )

 ##############################################################################
  types_getMinorStates = []
  def export_getStates (self):
    """
    Return Distict Values of Status job Attribute in WMS
    """
    return jobDB.getDistinctJobAttributes( 'MinorStatus' )

##############################################################################
  types_getJobs = []
  def export_getJobs (self, attrDict=None, cutDate=None):
    """
    Return list of JobIds matching the condition given in attrDict
    """
    queryDict = {}

    if attrDict:
      if type ( attrDict ) != DictType:
        return S_ERROR( 'Argument must be of Dict Type' )
      for attribute in self.queryAttributes:
        # Only those Attribute in self.queryAttributes can be used
        if attrDict.has_key(attribute):
          queryDict[attribute] = attrDict[attribute]

    return jobDB.selectJobs( queryDict, newer=cutDate)

##############################################################################
  types_getCounters = [ ListType ]
  def export_getCounters( self, attrList, attrDict={}, cutDate=''):
    """
    Retrieve list of distinct attributes values from attrList
    with attrDict as condition.
    For each set of distinct values, count number of occurences.
    Return a list. Each item is a list with 2 items, the list of distinct
    attribute values and the counter
    """

    # Check that Attributes in attrList and attrDict, they must be in
    # self.queryAttributes.

    for attr in attrList:
      try:
        self.queryAttributes.index(attr)
      except:
        return S_ERROR( 'Requested Attribute not Allowed: %s.' % attr )

    for attr in attrDict:
      try:
        self.queryAttributes.index(attr)
      except:
        return S_ERROR( 'Condition Attribute not Allowed: %s.' % attr )


    cutdate = str(cutDate)

    return jobDB.getCounters( attrList, attrDict, cutDate)

##############################################################################
  types_getJobStatus = [ IntType ]
  def export_getJobStatus (self, jobId ):

    return jobDB.getJobAttribute(jobId, 'Status')

##############################################################################
  types_getJobOwner = [ IntType ]
  def export_getJobOwner (self, jobId ):

    return jobDB.getJobAttribute(jobId,'Owner')

##############################################################################
  types_getJobSite = [ IntType ]
  def export_getJobSite (self, jobId ):

    return jobDB.getJobAttribute(jobId, 'Site')

##############################################################################
#  types_getJobLogInfo = [ IntType ]
#  def export_getJobLogInfo(self, jobId):
#
#    return jobDB.getJobLogInfo( jobId )

##############################################################################
  types_getJobsStatus = [ ListType ]
  def export_getJobsStatus (self, jobIds):

    return jobDB.getAttributesForJobList( jobIds, ['Status'] )

##############################################################################
  types_getJobsSites = [ ListType ]
  def export_getJobsSites (self, jobIds):

    return jobDB.getAttributesForJobList( jobIds, ['Site'] )

##############################################################################
  types_getJobSummary = [ IntType ]
  def export_getJobSummary(self, jobId):
    #return jobDB.getAllJobAttributes( [jobId], self.summary )
    return jobDB.getAllJobAttributes( jobId )

##############################################################################
  types_getJobPrimarySummary = [ IntType ]
  def export_getJobPrimarySummary(self, jobId ):
    return jobDB.getJobsAttributes( [jobId], self.primarySummary )

##############################################################################
  types_getJobsSummary = [ ListType ]
  def export_getJobsSummary(self, jobIds):
    return jobDB.getJobsAttributes( jobIds, self.summary )

##############################################################################
  types_getJobsPrimarySummary = [ ListType ]
  def export_getJobsPrimarySummary (self, jobIds):
    return jobDB.getJobsAttributes( jobIds, self.primarySummary )

##############################################################################
  types_getJobParameter = [ IntType, StringType ]
  def export_getJobParameter( self, jobId, parName ):
    return jobDB.getJobParameters( jobId, [parName] )

##############################################################################
  types_getJobParameters = [ IntType ]
  def export_getJobParameters( self, jobId ):
    return jobDB.getJobParameters( jobId )
