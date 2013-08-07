########################################################################
# $HeadURL$
########################################################################

""" JobMonitoringHandler is the implementation of the JobMonitoring service
    in the DISET framework

    The following methods are available in the Service interface



"""

__RCSID__ = "$Id$"

from types import IntType, LongType, ListType, DictType, StringTypes, StringType
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import JobPolicy, RIGHT_GET_INFO
import DIRAC.Core.Utilities.Time as Time

# These are global instances of the DB classes
gJobDB = False
gJobLoggingDB = False
gTaskQueueDB = False

SUMMARY = ['JobType', 'Site', 'JobName', 'Owner', 'SubmissionTime',
           'LastUpdateTime', 'Status', 'MinorStatus', 'ApplicationStatus']
SUMMARY = []
PRIMARY_SUMMARY = []
FINAL_STATES = ['Done', 'Completed', 'Stalled', 'Failed', 'Killed']

def initializeJobMonitoringHandler( serviceInfo ):

  global gJobDB, gJobLoggingDB, gTaskQueueDB
  gJobDB = JobDB()
  gJobLoggingDB = JobLoggingDB()
  gTaskQueueDB = TaskQueueDB()
  return S_OK()

class JobMonitoringHandler( RequestHandler ):

  def initialize( self ):

    credDict = self.getRemoteCredentials()
    self.ownerDN = credDict['DN']
    self.ownerGroup = credDict['group']
    self.userProperties = credDict[ 'properties' ]
    self.jobPolicy = JobPolicy( self.ownerDN, self.ownerGroup, self.userProperties )
    self.jobPolicy.setJobDB( gJobDB )
    self.globalJobsInfo = self.getCSOption( 'GlobalJobsInfo', True )
    return S_OK()

##############################################################################
  types_getApplicationStates = []
  @staticmethod
  def export_getApplicationStates ():
    """ Return Distinct Values of ApplicationStatus job Attribute in WMS
    """
    return gJobDB.getDistinctJobAttributes( 'ApplicationStatus' )

##############################################################################
  types_getJobTypes = []
  @staticmethod
  def export_getJobTypes ():
    """ Return Distinct Values of JobType job Attribute in WMS
    """
    return gJobDB.getDistinctJobAttributes( 'JobType' )

##############################################################################
  types_getOwners = []
  @staticmethod
  def export_getOwners ():
    """
    Return Distinct Values of Owner job Attribute in WMS
    """
    return gJobDB.getDistinctJobAttributes( 'Owner' )

##############################################################################
  types_getProductionIds = []
  @staticmethod
  def export_getProductionIds ():
    """
    Return Distinct Values of ProductionId job Attribute in WMS
    """
    return gJobDB.getDistinctJobAttributes( 'JobGroup' )

##############################################################################
  types_getJobGroups = []
  @staticmethod
  def export_getJobGroups( condDict = None, cutDate = None ):
    """
    Return Distinct Values of ProductionId job Attribute in WMS
    """
    return gJobDB.getDistinctJobAttributes( 'JobGroup', condDict,
                                           newer = cutDate )

##############################################################################
  types_getSites = []
  @staticmethod
  def export_getSites ():
    """
    Return Distinct Values of Site job Attribute in WMS
    """
    return gJobDB.getDistinctJobAttributes( 'Site' )

##############################################################################
  types_getStates = []
  @staticmethod
  def export_getStates ():
    """
    Return Distinct Values of Status job Attribute in WMS
    """
    return gJobDB.getDistinctJobAttributes( 'Status' )

##############################################################################
  types_getMinorStates = []
  @staticmethod
  def export_getMinorStates ():
    """
    Return Distinct Values of Minor Status job Attribute in WMS
    """
    return gJobDB.getDistinctJobAttributes( 'MinorStatus' )

##############################################################################
  types_getJobs = []
  @staticmethod
  def export_getJobs ( attrDict = None, cutDate = None ):
    """
    Return list of JobIds matching the condition given in attrDict
    """
    #queryDict = {}

    #if attrDict:
    #  if type ( attrDict ) != DictType:
    #    return S_ERROR( 'Argument must be of Dict Type' )
    #  for attribute in self.queryAttributes:
    #    # Only those Attribute in self.queryAttributes can be used
    #    if attrDict.has_key(attribute):
    #      queryDict[attribute] = attrDict[attribute]

    print attrDict

    return gJobDB.selectJobs( attrDict, newer = cutDate )

##############################################################################
  types_getCounters = [ ListType ]
  @staticmethod
  def export_getCounters( attrList, attrDict = None, cutDate = '' ):
    """
    Retrieve list of distinct attributes values from attrList
    with attrDict as condition.
    For each set of distinct values, count number of occurences.
    Return a list. Each item is a list with 2 items, the list of distinct
    attribute values and the counter
    """

    # Check that Attributes in attrList and attrDict, they must be in
    # self.queryAttributes.

    #for attr in attrList:
    #  try:
    #    self.queryAttributes.index(attr)
    #  except:
    #    return S_ERROR( 'Requested Attribute not Allowed: %s.' % attr )
    #
    #for attr in attrDict:
    #  try:
    #    self.queryAttributes.index(attr)
    #  except:
    #    return S_ERROR( 'Condition Attribute not Allowed: %s.' % attr )

    cutDate = str( cutDate )
    if not attrDict:
      attrDict = {}

    return gJobDB.getCounters( 'Jobs', attrList, attrDict, newer = cutDate, timeStamp = 'LastUpdateTime' )

##############################################################################
  types_getCurrentJobCounters = [ ]
  @staticmethod
  def export_getCurrentJobCounters( attrDict = None ):
    """ Get job counters per Status with attrDict selection. Final statuses are given for
        the last day.
    """

    if not attrDict:
      attrDict = {}
    result = gJobDB.getCounters( 'Jobs', ['Status'], attrDict, timeStamp = 'LastUpdateTime' )
    if not result['OK']:
      return result
    last_update = Time.dateTime() - Time.day
    resultDay = gJobDB.getCounters( 'Jobs', ['Status'], attrDict, newer = last_update,
                                   timeStamp = 'LastUpdateTime' )
    if not resultDay['OK']:
      return resultDay

    resultDict = {}
    for statusDict, count in result['Value']:
      status = statusDict['Status']
      resultDict[status] = count
      if status in FINAL_STATES:
        resultDict[status] = 0
        for statusDayDict, ccount in resultDay['Value']:
          if status == statusDayDict['Status']:
            resultDict[status] = ccount
          break

    return S_OK( resultDict )

##############################################################################
  types_getJobStatus = [ IntType ]
  @staticmethod
  def export_getJobStatus ( jobID ):

    return gJobDB.getJobAttribute( jobID, 'Status' )

##############################################################################
  types_getJobOwner = [ IntType ]
  @staticmethod
  def export_getJobOwner ( jobID ):

    return gJobDB.getJobAttribute( jobID, 'Owner' )

##############################################################################
  types_getJobSite = [ IntType ]
  @staticmethod
  def export_getJobSite ( jobID ):

    return gJobDB.getJobAttribute( jobID, 'Site' )

##############################################################################
  types_getJobJDL = [ IntType ]
  @staticmethod
  def export_getJobJDL ( jobID ):

    return gJobDB.getJobJDL( jobID )

##############################################################################
  types_getJobLoggingInfo = [ IntType ]
  @staticmethod
  def export_getJobLoggingInfo( jobID ):

    return gJobLoggingDB.getJobLoggingInfo( jobID )

##############################################################################
  types_getJobsStatus = [ ListType ]
  @staticmethod
  def export_getJobsStatus ( jobIDs ):
    if not jobIDs:
      return S_OK( {} )
    return gJobDB.getAttributesForJobList( jobIDs, ['Status'] )

##############################################################################
  types_getJobsMinorStatus = [ ListType ]
  @staticmethod
  def export_getJobsMinorStatus ( jobIDs ):

    return gJobDB.getAttributesForJobList( jobIDs, ['MinorStatus'] )

##############################################################################
  types_getJobsApplicationStatus = [ ListType ]
  @staticmethod
  def export_getJobsApplicationStatus ( jobIDs ):

    return gJobDB.getAttributesForJobList( jobIDs, ['ApplicationStatus'] )

##############################################################################
  types_getJobsSites = [ ListType ]
  @staticmethod
  def export_getJobsSites ( jobIDs ):

    return gJobDB.getAttributesForJobList( jobIDs, ['Site'] )

##############################################################################
  types_getJobSummary = [ IntType ]
  @staticmethod
  def export_getJobSummary( jobID ):
    return gJobDB.getJobAttributes( jobID, SUMMARY )

##############################################################################
  types_getJobPrimarySummary = [ IntType ]
  @staticmethod
  def export_getJobPrimarySummary( jobID ):
    return gJobDB.getJobAttributes( jobID, PRIMARY_SUMMARY )

##############################################################################
  types_getJobsSummary = [ ListType ]
  @staticmethod
  def export_getJobsSummary( jobIDs ):

    if not jobIDs:
      return S_ERROR( 'JobMonitoring.getJobsSummary: Received empty job list' )

    result = gJobDB.getAttributesForJobList( jobIDs, SUMMARY )
    #return result
    restring = str( result['Value'] )
    return S_OK( restring )

##############################################################################
  types_getJobPageSummaryWeb = [DictType, ListType, IntType, IntType]
  @staticmethod
  def export_getJobPageSummaryWeb( selectDict, sortList, startItem, maxItems, selectJobs = True ):
    """ Get the summary of the job information for a given page in the
        job monitor in a generic format
    """
    resultDict = {}
    startDate = selectDict.get( 'FromDate', None )
    if startDate:
      del selectDict['FromDate']
    # For backward compatibility
    if startDate is None:
      startDate = selectDict.get( 'LastUpdate', None )
      if startDate:
        del selectDict['LastUpdate']
    endDate = selectDict.get( 'ToDate', None )
    if endDate:
      del selectDict['ToDate']

    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None

    statusDict = {}
    result = gJobDB.getCounters( 'Jobs', ['Status'], selectDict,
                               newer = startDate,
                               older = endDate,
                               timeStamp = 'LastUpdateTime' )

    nJobs = 0
    if result['OK']:
      for stDict, count in result['Value']:
        nJobs += count
        statusDict[stDict['Status']] = count

    resultDict['TotalRecords'] = nJobs
    if nJobs == 0:
      return S_OK( resultDict )

    resultDict['Extras'] = statusDict

    if selectJobs:
      iniJob = startItem
      if iniJob >= nJobs:
        return S_ERROR( 'Item number out of range' )

      result = gJobDB.selectJobs( selectDict, orderAttribute = orderAttribute,
                                newer = startDate, older = endDate, limit = ( maxItems, iniJob ) )
      if not result['OK']:
        return S_ERROR( 'Failed to select jobs: ' + result['Message'] )

      summaryJobList = result['Value']
      result = gJobDB.getAttributesForJobList( summaryJobList, SUMMARY )
      if not result['OK']:
        return S_ERROR( 'Failed to get job summary: ' + result['Message'] )

      summaryDict = result['Value']

      # Evaluate last sign of life time
      for jobID, jobDict in summaryDict.items():
        if jobDict['HeartBeatTime'] == 'None':
          jobDict['LastSignOfLife'] = jobDict['LastUpdateTime']
        else:
          lastTime = Time.fromString( jobDict['LastUpdateTime'] )
          hbTime = Time.fromString( jobDict['HeartBeatTime'] )
          if ( hbTime - lastTime ) > ( lastTime - lastTime ) or jobDict['Status'] == "Stalled":
            jobDict['LastSignOfLife'] = jobDict['HeartBeatTime']
          else:
            jobDict['LastSignOfLife'] = jobDict['LastUpdateTime']

      tqDict = {}
      result = gTaskQueueDB.getTaskQueueForJobs( summaryJobList )
      if result['OK']:
        tqDict = result['Value']

      # prepare the standard structure now
      key = summaryDict.keys()[0]
      paramNames = summaryDict[key].keys()

      records = []
      for jobID, jobDict in summaryDict.items():
        jParList = []
        for pname in paramNames:
          jParList.append( jobDict[pname] )
        jParList.append( tqDict.get( jobID, 0 ) )
        records.append( jParList )

      resultDict['ParameterNames'] = paramNames + ['TaskQueueID']
      resultDict['Records'] = records

    return S_OK( resultDict )


  def getJobPageSummaryWeb( self, selectDict, sortList, startItem, maxItems, selectJobs = True ):
    """ Get the summary of the job information for a given page in the
        job monitor in a generic format
    """
    resultDict = {}
    startDate = selectDict.get( 'FromDate', None )
    if startDate:
      del selectDict['FromDate']
    # For backward compatibility
    if startDate is None:
      startDate = selectDict.get( 'LastUpdate', None )
      if startDate:
        del selectDict['LastUpdate']
    endDate = selectDict.get( 'ToDate', None )
    if endDate:
      del selectDict['ToDate']

    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None

    if selectJobs:
      result = gJobDB.selectJobs( selectDict, orderAttribute = orderAttribute,
                                newer = startDate, older = endDate )
      if not result['OK']:
        return S_ERROR( 'Failed to select jobs: ' + result['Message'] )

      jobList = result['Value']

      if not self.globalJobsInfo:      
        validJobs, invalidJobs, nonauthJobs, ownerJobs = self.jobPolicy.evaluateJobRights( jobList,
                                                                                           RIGHT_GET_INFO )
        jobList = validJobs
      
      nJobs = len( jobList )
      resultDict['TotalRecords'] = nJobs
      if nJobs == 0:
        return S_OK( resultDict )

      iniJob = startItem
      lastJob = iniJob + maxItems
      if iniJob >= nJobs:
        return S_ERROR( 'Item number out of range' )

      if lastJob > nJobs:
        lastJob = nJobs

      summaryJobList = jobList[iniJob:lastJob]
      result = gJobDB.getAttributesForJobList( summaryJobList, SUMMARY )
      if not result['OK']:
        return S_ERROR( 'Failed to get job summary: ' + result['Message'] )

      summaryDict = result['Value']

      # Evaluate last sign of life time
      for jobID, jobDict in summaryDict.items():
        if jobDict['HeartBeatTime'] == 'None':
          jobDict['LastSignOfLife'] = jobDict['LastUpdateTime']
        else:
          lastTime = Time.fromString( jobDict['LastUpdateTime'] )
          hbTime = Time.fromString( jobDict['HeartBeatTime'] )
          if ( hbTime - lastTime ) > ( lastTime - lastTime ) or jobDict['Status'] == "Stalled":
            jobDict['LastSignOfLife'] = jobDict['HeartBeatTime']
          else:
            jobDict['LastSignOfLife'] = jobDict['LastUpdateTime']

      tqDict = {}
      result = gTaskQueueDB.getTaskQueueForJobs( summaryJobList )
      if result['OK']:
        tqDict = result['Value']

      # prepare the standard structure now
      key = summaryDict.keys()[0]
      paramNames = summaryDict[key].keys()

      records = []
      for jobID, jobDict in summaryDict.items():
        jParList = []
        for pname in paramNames:
          jParList.append( jobDict[pname] )
        if tqDict and tqDict.has_key( jobID ):
          jParList.append( tqDict[jobID] )
        else:
          jParList.append( 0 )
        records.append( jParList )

      resultDict['ParameterNames'] = paramNames + ['TaskQueueID']
      resultDict['Records'] = records

    statusDict = {}
    result = gJobDB.getCounters( 'Jobs', ['Status'], selectDict,
                               newer = startDate,
                               older = endDate,
                               timeStamp = 'LastUpdateTime' )
    if result['OK']:
      for stDict, count in result['Value']:
        statusDict[stDict['Status']] = count
    resultDict['Extras'] = statusDict

    return S_OK( resultDict )

##############################################################################
  types_getJobStats = [ StringTypes, DictType ]
  @staticmethod
  def export_getJobStats ( attribute, selectDict ):
    """ Get job statistics distribution per attribute value with a given selection
    """
    startDate = selectDict.get( 'FromDate', None )
    if startDate:
      del selectDict['FromDate']
    # For backward compatibility
    if startDate is None:
      startDate = selectDict.get( 'LastUpdate', None )
      if startDate:
        del selectDict['LastUpdate']
    endDate = selectDict.get( 'ToDate', None )
    if endDate:
      del selectDict['ToDate']

    result = gJobDB.getCounters( 'Jobs', [attribute], selectDict,
                               newer = startDate,
                               older = endDate,
                               timeStamp = 'LastUpdateTime' )
    resultDict = {}
    if result['OK']:
      for cDict, count in result['Value']:
        resultDict[cDict[attribute]] = count

    return S_OK( resultDict )

##############################################################################
  types_getJobsPrimarySummary = [ ListType ]
  @staticmethod
  def export_getJobsPrimarySummary ( jobIDs ):
    return gJobDB.getAttributesForJobList( jobIDs, PRIMARY_SUMMARY )

##############################################################################
  types_getJobParameter = [ [StringType, IntType, LongType] , StringTypes ]
  @staticmethod
  def export_getJobParameter( jobID, parName ):
    return gJobDB.getJobParameters( jobID, [parName] )

##############################################################################
  types_getJobParameters = [ [IntType, LongType] ]
  @staticmethod
  def export_getJobParameters( jobID ):
    return gJobDB.getJobParameters( jobID )

##############################################################################
  types_getAtticJobParameters = [ [IntType, LongType] ]
  @staticmethod
  def export_getAtticJobParameters( jobID, parameters = None, rescheduleCycle = -1 ):
    if not parameters:
      parameters = []
    return gJobDB.getAtticJobParameters( jobID, parameters, rescheduleCycle )

##############################################################################
  types_getJobAttributes = [ IntType ]
  @staticmethod
  def export_getJobAttributes( jobID ):
    return gJobDB.getJobAttributes( jobID )

##############################################################################
  types_getSiteSummary = [ ]
  @staticmethod
  def export_getSiteSummary():
    return gJobDB.getSiteSummary()

##############################################################################
  types_getJobHeartBeatData = [ IntType ]
  @staticmethod
  def export_getJobHeartBeatData( jobID ):
    return gJobDB.getHeartBeatData( jobID )

##############################################################################
  types_getInputData = [ [IntType, LongType] ]
  @staticmethod
  def export_getInputData( jobID ):
    """ Get input data for the specified jobs
    """
    return  gJobDB.getInputData( jobID )
