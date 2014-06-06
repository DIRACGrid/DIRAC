"""
This is a DIRAC WMS administrator interface.
It exposes the following methods:

Site mask related methods:
    setMask(<site mask>)
    getMask()

Access to the pilot data:
    getWMSStats()

"""

__RCSID__ = "$Id$"

from types import DictType, ListType, IntType, LongType, StringTypes, StringType, FloatType

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import getPilotLoggingInfo, getPilotOutput
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
import DIRAC.Core.Utilities.Time as Time
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupOption
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources

# This is a global instance of the database classes
jobDB = False
pilotDB = False
taskQueueDB = False

FINAL_STATES = ['Done','Aborted','Cleared','Deleted','Stalled']

def initializeWMSAdministratorHandler( serviceInfo ):
  """  WMS AdministratorService initialization
  """

  global jobDB
  global pilotDB
  global taskQueueDB

  jobDB = JobDB()
  pilotDB = PilotAgentsDB()
  taskQueueDB = TaskQueueDB()
  return S_OK()

class WMSAdministratorHandler(RequestHandler):

##############################################################################
  types_getCurrentPilotCounters = [ ]
  def export_getCurrentPilotCounters( self, attrDict={}):
    """ Get pilot counters per Status with attrDict selection. Final statuses are given for
        the last day.
    """

    result = pilotDB.getCounters( 'PilotAgents',['Status'], attrDict, timeStamp='LastUpdateTime')
    if not result['OK']:
      return result
    last_update = Time.dateTime() - Time.day
    resultDay = pilotDB.getCounters( 'PilotAgents',['Status'], attrDict, newer=last_update,
                                   timeStamp='LastUpdateTime')
    if not resultDay['OK']:
      return resultDay

    resultDict = {}
    for statusDict, count in result['Value']:
      status = statusDict['Status']
      resultDict[status] = count
      if status in FINAL_STATES:
        resultDict[status] = 0
        for statusDayDict,ccount in resultDay['Value']:
          if status == statusDayDict['Status']:
            resultDict[status] = ccount
          break

    return S_OK(resultDict)

##########################################################################################
  types_addPilotTQReference = [ ListType, [IntType, LongType], StringTypes, StringTypes ]
  def export_addPilotTQReference( self, pilotRef, taskQueueID, ownerDN, ownerGroup, broker='Unknown',
                        gridType='DIRAC', requirements='Unknown',pilotStampDict={}):
    """ Add a new pilot job reference """
    return pilotDB.addPilotTQReference(pilotRef, taskQueueID,
                                       ownerDN, ownerGroup,
                                       broker, gridType, requirements,pilotStampDict)


  ##############################################################################
  types_getPilotOutput = [StringTypes]
  def export_getPilotOutput(self,pilotReference):
    """ Get the pilot job standard output and standard error files for the Grid
        job reference
    """

    return self.__getGridJobOutput(pilotReference)

  ##############################################################################
  types_getPilotInfo = [ list(StringTypes)+[ListType] ]
  def export_getPilotInfo(self,pilotReference):
    """ Get the info about a given pilot job reference
    """
    return pilotDB.getPilotInfo(pilotReference)

  ##############################################################################
  types_selectPilots = [ DictType ]
  def export_selectPilots(self,condDict):
    """ Select pilots given the selection conditions
    """
    return pilotDB.selectPilots(condDict)

  ##############################################################################
  types_storePilotOutput = [ StringTypes,StringTypes,StringTypes ]
  def export_storePilotOutput(self,pilotReference,output,error):
    """ Store the pilot output and error
    """
    return pilotDB.storePilotOutput(pilotReference,output,error)

  ##############################################################################
  types_getPilotLoggingInfo = [StringTypes]
  def export_getPilotLoggingInfo(self,pilotReference):
    """ Get the pilot logging info for the Grid job reference
    """

    result = pilotDB.getPilotInfo(pilotReference)
    if not result['OK'] or not result[ 'Value' ]:
      return S_ERROR('Failed to determine owner for pilot ' + pilotReference)

    pilotDict = result['Value'][pilotReference]
    owner = pilotDict['OwnerDN']
    group = pilotDict['OwnerGroup']

    group = getGroupOption(group,'VOMSRole',group)
    ret = gProxyManager.getPilotProxyFromVOMSGroup( owner, group )
    if not ret['OK']:
      gLogger.error( ret['Message'] )
      gLogger.error( 'Could not get proxy:', 'User "%s", Group "%s"' % ( owner, group ) )
      return S_ERROR("Failed to get the pilot's owner proxy")
    proxy = ret['Value']

    gridType = pilotDict['GridType']

    return getPilotLoggingInfo( proxy, gridType, pilotReference )



  ##############################################################################
  types_getJobPilotOutput = [[StringType, IntType, LongType]]
  def export_getJobPilotOutput(self,jobID):
    """ Get the pilot job standard output and standard error files for the DIRAC
        job reference
    """

    pilotReference = ''
    # Get the pilot grid reference first from the job parameters
    result = jobDB.getJobParameter( int( jobID ), 'Pilot_Reference' )
    if result['OK']:
      pilotReference = result['Value']

    if not pilotReference:
      # Failed to get the pilot reference, try to look in the attic parameters
      result = jobDB.getAtticJobParameters( int( jobID ), ['Pilot_Reference'] )
      if result['OK']:
        c = -1
        # Get the pilot reference for the last rescheduling cycle
        for cycle in result['Value']:
          if cycle > c:
            pilotReference = result['Value'][cycle]['Pilot_Reference']
            c = cycle

    if pilotReference:
      return self.__getGridJobOutput(pilotReference)
    else:
      return S_ERROR('No pilot job reference found')

  ##############################################################################
  def __getGridJobOutput(self,pilotReference):
    """ Get the pilot job standard output and standard error files for the Grid
        job reference
    """

    result = pilotDB.getPilotInfo(pilotReference)
    if not result['OK'] or not result[ 'Value' ]:
      return S_ERROR('Failed to get info for pilot ' + pilotReference)

    pilotDict = result['Value'][pilotReference]
    owner = pilotDict['OwnerDN']
    group = pilotDict['OwnerGroup']

    # FIXME: What if the OutputSandBox is not StdOut and StdErr, what do we do with other files?
    result = pilotDB.getPilotOutput(pilotReference)
    if result['OK']:
      stdout = result['Value']['StdOut']
      error = result['Value']['StdErr']
      if stdout or error:
        resultDict = {}
        resultDict['StdOut'] = stdout
        resultDict['StdErr'] = error
        resultDict['OwnerDN'] = owner
        resultDict['OwnerGroup'] = group
        resultDict['FileList'] = []
        return S_OK(resultDict)
      else:
        gLogger.warn( 'Empty pilot output found for %s' % pilotReference )

    gridType = pilotDict['GridType']
    if gridType in ["LCG","gLite","CREAM"]:
      group = getGroupOption(group,'VOMSRole',group)
      ret = gProxyManager.getPilotProxyFromVOMSGroup( owner, group )
      if not ret['OK']:
        gLogger.error( ret['Message'] )
        gLogger.error( 'Could not get proxy:', 'User "%s", Group "%s"' % ( owner, group ) )
        return S_ERROR("Failed to get the pilot's owner proxy")
      proxy = ret['Value']

      pilotStamp = pilotDict['PilotStamp']
      result = getPilotOutput( proxy, gridType, pilotReference, pilotStamp )
      if not result['OK']:
        return S_ERROR('Failed to get pilot output: '+result['Message'])
      # FIXME: What if the OutputSandBox is not StdOut and StdErr, what do we do with other files?
      stdout = result['StdOut']
      error = result['StdErr']
      fileList = result['FileList']
      if stdout:
        result = pilotDB.storePilotOutput(pilotReference,stdout,error)
        if not result['OK']:
          gLogger.error('Failed to store pilot output:',result['Message'])

      resultDict = {}
      resultDict['StdOut'] = stdout
      resultDict['StdErr'] = error
      resultDict['OwnerDN'] = owner
      resultDict['OwnerGroup'] = group
      resultDict['FileList'] = fileList
      return S_OK(resultDict)
    else:
      # Instantiate the appropriate CE
      ceFactory = ComputingElementFactory()
      result = Resources( group=group ).getQueueDescription( pilotDict['Queue'] )
      if not result['OK']:
        return result
      queueDict = result['Value']
      result = ceFactory.getCE( gridType, pilotDict['DestinationSite'], queueDict )
      if not result['OK']:
        return result
      ce = result['Value']
      pilotStamp = pilotDict['PilotStamp']
      pRef = pilotReference
      if pilotStamp:
        pRef = pRef + ':::' + pilotStamp
      result = ce.getJobOutput( pRef )
      if not result['OK']:
        return result
      stdout,error = result['Value']
      if stdout:
        result = pilotDB.storePilotOutput(pilotReference,stdout,error)
        if not result['OK']:
          gLogger.error('Failed to store pilot output:',result['Message'])

      resultDict = {}
      resultDict['StdOut'] = stdout
      resultDict['StdErr'] = error
      resultDict['OwnerDN'] = owner
      resultDict['OwnerGroup'] = group
      resultDict['FileList'] = []
      return S_OK( resultDict )

  ##############################################################################
  types_getPilotSummary = []
  def export_getPilotSummary(self,startdate='',enddate=''):
    """ Get summary of the status of the LCG Pilot Jobs
    """

    result = pilotDB.getPilotSummary(startdate,enddate)
    return result

  ##############################################################################
  types_getPilotMonitorWeb = [DictType, ListType, IntType, IntType]
  def export_getPilotMonitorWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
    """

    result = pilotDB.getPilotMonitorWeb(selectDict, sortList, startItem, maxItems)
    return result

  ##############################################################################
  types_getPilotMonitorSelectors = []
  def export_getPilotMonitorSelectors(self):
    """ Get all the distinct selector values for the Pilot Monitor web portal page
    """

    result = pilotDB.getPilotMonitorSelectors()
    return result

  ##############################################################################
  types_getPilotSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getPilotSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
    """

    result = pilotDB.getPilotSummaryWeb(selectDict, sortList, startItem, maxItems)
    return result

  ##############################################################################
  types_getUserSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getUserSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
    """

    result = jobDB.getUserSummaryWeb(selectDict, sortList, startItem, maxItems)
    return result

  ##############################################################################
  types_getSiteSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getSiteSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the jobs running on sites in a generic format
    """

    result = jobDB.getSiteSummaryWeb(selectDict, sortList, startItem, maxItems)
    return result

  ##############################################################################
  types_getSiteSummarySelectors = []
  def export_getSiteSummarySelectors(self):
    """ Get all the distinct selector values for the site summary web portal page
    """

    resultDict = {}
    statusList = ['Good','Fair','Poor','Bad','Idle']
    resultDict['Status'] = statusList
    maskStatus = ['Active','Banned','NoMask','Reduced']
    resultDict['MaskStatus'] = maskStatus

    resources = Resources()
    result = resources.getSites()
    if not result['OK']:
      return result
    siteList = result['Value']

    countryList = []
    for site in siteList:
      if site.find('.') != -1:
        country = site.split('.')[1]
        country = country.lower()
        if country not in countryList:
          countryList.append(country)
    countryList.sort()
    resultDict['Country'] = countryList
    siteList.sort()
    resultDict['Site'] = siteList

    return S_OK(resultDict)

  ##############################################################################
  types_getPilots = [[StringType, IntType, LongType]]
  def export_getPilots(self,jobID):
    """ Get pilot references and their states for :
      - those pilots submitted for the TQ where job is sitting
      - (or) the pilots executing/having executed the Job
    """

    pilots = []
    result = pilotDB.getPilotsForJobID( int( jobID ) )
    if not result['OK']:
      if result['Message'].find('not found') == -1:
        return S_ERROR('Failed to get pilot: '+result['Message'])
    else:
      pilots += result['Value']
    if not pilots:
      # Pilots were not found try to look in the Task Queue
      taskQueueID = 0
      result = taskQueueDB.getTaskQueueForJob( int( jobID ) )
      if result['OK'] and result['Value']:
        taskQueueID = result['Value']
      if taskQueueID:
        result = pilotDB.getPilotsForTaskQueue( taskQueueID, limit=10 )
        if not result['OK']:
          return S_ERROR('Failed to get pilot: '+result['Message'])
        pilots += result['Value']

    if not pilots:
      return S_ERROR( 'Failed to get pilot for Job %d' % int( jobID ) )

    return pilotDB.getPilotInfo(pilotID=pilots)
  
  ##############################################################################
  types_killPilot = [ list(StringTypes)+[ListType] ]
  def export_killPilot(self, pilotRefList ):
    """ Kill the specified pilots
    """
    # Make a list if it is not yet
    pilotRefs = list( pilotRefList )
    if type( pilotRefList ) in StringTypes:
      pilotRefs = [pilotRefList]
    
    # Regroup pilots per site and per owner
    pilotRefDict = {}
    for pilotReference in pilotRefs:
      result = pilotDB.getPilotInfo(pilotReference)
      if not result['OK'] or not result[ 'Value' ]:
        return S_ERROR('Failed to get info for pilot ' + pilotReference)
  
      pilotDict = result['Value'][pilotReference]
      owner = pilotDict['OwnerDN']
      group = pilotDict['OwnerGroup']
      queue = '@@@'.join( [owner, group, pilotDict['GridSite'], pilotDict['DestinationSite'], pilotDict['Queue']] )
      gridType = pilotDict['GridType']
      pilotRefDict.setdefault( queue, {} )
      pilotRefDict[queue].setdefault( 'PilotList', [] )
      pilotRefDict[queue]['PilotList'].append( pilotReference )
      pilotRefDict[queue]['GridType'] = gridType
      
    # Do the work now queue by queue  
    ceFactory = ComputingElementFactory()
    failed = []
    for key, pilotDict in pilotRefDict.items():
      
      owner,group,site,ce,queue = key.split( '@@@' )
      result = Resources( group=group ).getQueueDescription( queue )
      if not result['OK']:
        return result
      queueDict = result['Value']
      gridType = pilotDict['GridType']
      result = ceFactory.getCE( gridType, ce, queueDict )
      if not result['OK']:
        return result
      ce = result['Value']
  
      if gridType in ["LCG","gLite","CREAM"]:
        group = getGroupOption(group,'VOMSRole',group)
        ret = gProxyManager.getPilotProxyFromVOMSGroup( owner, group )
        if not ret['OK']:
          gLogger.error( ret['Message'] )
          gLogger.error( 'Could not get proxy:', 'User "%s", Group "%s"' % ( owner, group ) )
          return S_ERROR("Failed to get the pilot's owner proxy")
        proxy = ret['Value']
        ce.setProxy( proxy )

      pilotList = pilotDict['PilotList']
      result = ce.killJob( pilotList )
      if not result['OK']:
        failed.extend( pilotList )
      
    if failed:
      return S_ERROR('Failed to kill at least some pilots')
    
    return S_OK()  

  ##############################################################################
  types_setJobForPilot = [ [StringType, IntType, LongType], StringTypes]
  def export_setJobForPilot(self,jobID,pilotRef,destination=None):
    """ Report the DIRAC job ID which is executed by the given pilot job
    """

    result = pilotDB.setJobForPilot( int( jobID ), pilotRef )
    if not result['OK']:
      return result
    result = pilotDB.setCurrentJobID( pilotRef, int( jobID ) )
    if not result['OK']:
      return result
    if destination:
      result = pilotDB.setPilotDestinationSite(pilotRef,destination)

    return result

  ##########################################################################################
  types_setPilotBenchmark = [StringTypes, FloatType]
  def export_setPilotBenchmark(self,pilotRef,mark):
    """ Set the pilot agent benchmark
    """
    result = pilotDB.setPilotBenchmark(pilotRef,mark)
    return result

  ##########################################################################################
  types_setAccountingFlag = [StringTypes]
  def export_setAccountingFlag(self,pilotRef,mark='True'):
    """ Set the pilot AccountingSent flag
    """
    result = pilotDB.setAccountingFlag(pilotRef,mark)
    return result

  ##########################################################################################
  types_setPilotStatus = [StringTypes, StringTypes]
  def export_setPilotStatus(self,pilotRef,status,destination=None,reason=None,gridSite=None,queue=None):
    """ Set the pilot agent status
    """

    result = pilotDB.setPilotStatus(pilotRef,status,destination=destination,
                                    statusReason=reason,gridSite=gridSite,queue=queue)
    return result

  ##########################################################################################
  types_countPilots = [ DictType ]
  def export_countPilots(self,condDict, older=None, newer=None, timeStamp='SubmissionTime'):
    """ Set the pilot agent status
    """

    result = pilotDB.countPilots(condDict, older, newer, timeStamp )
    return result

  ##########################################################################################
  types_getCounters = [ StringTypes, ListType, DictType ]
  def export_getCounters(self, table, keys, condDict, newer=None, timeStamp='SubmissionTime'):
    """ Set the pilot agent status
    """

    result = pilotDB.getCounters( table, keys, condDict, newer=newer, timeStamp=timeStamp )
    return result
