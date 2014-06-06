########################################################################
# $Id$
########################################################################
"""
Matcher class. It matches Agent Site capabilities to job requirements.
It also provides an XMLRPC interface to the Matcher

"""

__RCSID__ = "$Id$"

import time
from   types import StringType, DictType, StringTypes
import threading

from DIRAC.ConfigurationSystem.Client.Helpers          import Registry, Operations
from DIRAC.Core.DISET.RequestHandler                   import RequestHandler
from DIRAC.Core.Utilities.ClassAd.ClassAdLight         import ClassAd
from DIRAC                                             import gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB           import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB    import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB     import TaskQueueDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB   import PilotAgentsDB
from DIRAC                                             import gMonitor
from DIRAC.Core.Utilities.ThreadScheduler              import gThreadScheduler
from DIRAC.Core.Security                               import Properties
from DIRAC.Core.Utilities.DictCache                    import DictCache
from DIRAC.ResourceStatusSystem.Client.SiteStatus      import SiteStatus

DEBUG = 0

gMutex = threading.Semaphore()
gTaskQueues = {}
gJobDB = False
gJobLoggingDB = False
gTaskQueueDB = False
gPilotAgentsDB = False

def initializeMatcherHandler( serviceInfo ):
  """  Matcher Service initialization
  """

  global gJobDB
  global gJobLoggingDB
  global gTaskQueueDB
  global gPilotAgentsDB

  # Create JobDB object and initialize its tables.
  gJobDB = JobDB()
  res = gJobDB._checkTable()
  if not res[ 'OK' ]:
    return res
  
  # Create JobLoggingDB object and initialize its tables.
  gJobLoggingDB = JobLoggingDB()
  res = gJobLoggingDB._checkTable()
  if not res[ 'OK' ]:
    return res
  
  gTaskQueueDB   = TaskQueueDB()
  
  # Create PilotAgentsDB object and initialize its tables.
  gPilotAgentsDB = PilotAgentsDB()
  res = gPilotAgentsDB._checkTable()
  if not res[ 'OK' ]:
    return res

  gMonitor.registerActivity( 'matchTime', "Job matching time",
                             'Matching', "secs" , gMonitor.OP_MEAN, 300 )
  gMonitor.registerActivity( 'matchesDone', "Job Match Request",
                             'Matching', "matches" , gMonitor.OP_RATE, 300 )
  gMonitor.registerActivity( 'matchesOK', "Matched jobs",
                             'Matching', "matches" , gMonitor.OP_RATE, 300 )
  gMonitor.registerActivity( 'numTQs', "Number of Task Queues",
                             'Matching', "tqsk queues" , gMonitor.OP_MEAN, 300 )

  gTaskQueueDB.recalculateTQSharesForAll()
  gThreadScheduler.addPeriodicTask( 120, gTaskQueueDB.recalculateTQSharesForAll )
  gThreadScheduler.addPeriodicTask( 60, sendNumTaskQueues )

  sendNumTaskQueues()

  return S_OK()

def sendNumTaskQueues():
  result = gTaskQueueDB.getNumTaskQueues()
  if result[ 'OK' ]:
    gMonitor.addMark( 'numTQs', result[ 'Value' ] )
  else:
    gLogger.error( "Cannot get the number of task queues", result[ 'Message' ] )


class Limiter:

  __csDictCache = DictCache()
  __condCache = DictCache()
  __delayMem = {}

  def __init__( self, opsHelper ):
    """ Constructor
    """
    self.__runningLimitSection = "JobScheduling/RunningLimit"
    self.__matchingDelaySection = "JobScheduling/MatchingDelay"
    self.__opsHelper = opsHelper

  def checkJobLimit( self ):
    return self.__opsHelper.getValue( "JobScheduling/CheckJobLimits", True )

  def checkMatchingDelay( self ):
    return self.__opsHelper.getValue( "JobScheduling/CheckMatchingDelay", True )

  def getNegativeCond( self ):
    """ Get negative condition for ALL sites
    """
    orCond = Limiter.__condCache.get( "GLOBAL" )
    if orCond:
      return orCond
    negCond = {}
    #Run Limit
    result = self.__opsHelper.getSections( self.__runningLimitSection )
    sites = []
    if result[ 'OK' ]:
      sites = result[ 'Value' ]
    for siteName in sites:
      result = self.__getRunningCondition( siteName )
      if not result[ 'OK' ]:
        continue
      data = result[ 'Value' ]
      if data:
        negCond[ siteName ] = data
    #Delay limit
    result = self.__opsHelper.getSections( self.__matchingDelaySection )
    sites = []
    if result[ 'OK' ]:
      sites = result[ 'Value' ]
    for siteName in sites:
      result = self.__getDelayCondition( siteName )
      if not result[ 'OK' ]:
        continue
      data = result[ 'Value' ]
      if not data:
        continue
      if siteName in negCond:
        negCond[ siteName ] = self.__mergeCond( negCond[ siteName ], data )
      else:
        negCond[ siteName ] = data
    orCond = []
    for siteName in negCond:
      negCond[ siteName ][ 'Site' ] = siteName
      orCond.append( negCond[ siteName ] )
    Limiter.__condCache.add( "GLOBAL", 10, orCond )
    return orCond

  def getNegativeCondForSite( self, siteName ):
    """ Generate a negative query based on the limits set on the site
    """
    # Check if Limits are imposed onto the site
    negativeCond = {}
    if self.checkJobLimit():
      result = self.__getRunningCondition( siteName )
      if result['OK']:
        negativeCond = result['Value']
      gLogger.verbose( 'Negative conditions for site %s after checking limits are: %s' % ( siteName, str( negativeCond ) ) )

    if self.checkMatchingDelay():
      result = self.__getDelayCondition( siteName )
      if result['OK']:
        delayCond = result['Value']
        gLogger.verbose( 'Negative conditions for site %s after delay checking are: %s' % ( siteName, str( delayCond ) ) )
        negativeCond = self.__mergeCond( negativeCond, delayCond )

    if negativeCond:
      gLogger.info( 'Negative conditions for site %s are: %s' % ( siteName, str( negativeCond ) ) )

    return negativeCond

  def __mergeCond( self, negCond, addCond ):
    """ Merge two negative dicts
    """
    #Merge both negative dicts
    for attr in addCond:
      if attr not in negCond:
        negCond[ attr ] = []
      for value in addCond[ attr ]:
        if value not in negCond[ attr ]:
          negCond[ attr ].append( value )
    return negCond

  def __extractCSData( self, section ):
    """ Extract limiting information from the CS in the form:
        { 'JobType' : { 'Merge' : 20, 'MCGen' : 1000 } }
    """
    stuffDict = Limiter.__csDictCache.get( section )
    if stuffDict:
      return S_OK( stuffDict )

    result = self.__opsHelper.getSections( section )
    if not result['OK']:
      return result
    attribs = result['Value']
    stuffDict = {}
    for attName in attribs:
      result = self.__opsHelper.getOptionsDict( "%s/%s" % ( section, attName ) )
      if not result[ 'OK' ]:
        return result
      attLimits = result[ 'Value' ]
      try:
        attLimits = dict( [ ( k, int( attLimits[k] ) ) for k in attLimits ] )
      except Exception, excp:
        errMsg = "%s/%s has to contain numbers: %s" % ( section, attName, str( excp ) )
        gLogger.error( errMsg )
        return S_ERROR( errMsg )
      stuffDict[ attName ] = attLimits

    Limiter.__csDictCache.add( section, 300, stuffDict )
    return S_OK( stuffDict )

  def __getRunningCondition( self, siteName ):
    """ Get extra conditions allowing site throttling
    """
    siteSection = "%s/%s" % ( self.__runningLimitSection, siteName )
    result = self.__extractCSData( siteSection )
    if not result['OK']:
      return result
    limitsDict = result[ 'Value' ]
    #limitsDict is something like { 'JobType' : { 'Merge' : 20, 'MCGen' : 1000 } }
    if not limitsDict:
      return S_OK( {} )
    # Check if the site exceeding the given limits
    negCond = {}
    for attName in limitsDict:
      if attName not in gJobDB.jobAttributeNames:
        gLogger.error( "Attribute %s does not exist. Check the job limits" % attName )
        continue
      cK = "Running:%s:%s" % ( siteName, attName )
      data = self.__condCache.get( cK )
      if not data:
        result = gJobDB.getCounters( 'Jobs', [ attName ], { 'Site' : siteName, 'Status' : [ 'Running', 'Matched', 'Stalled' ] } )
        if not result[ 'OK' ]:
          return result
        data = result[ 'Value' ]
        data = dict( [ ( k[0][ attName ], k[1] )  for k in data ] )
        self.__condCache.add( cK, 10, data )
      for attValue in limitsDict[ attName ]:
        limit = limitsDict[ attName ][ attValue ]
        running = data.get( attValue, 0 )
        if running >= limit:
          gLogger.verbose( 'Job Limit imposed at %s on %s/%s=%d,'
                           ' %d jobs already deployed' % ( siteName, attName, attValue, limit, running ) )
          if attName not in negCond:
            negCond[ attName ] = []
          negCond[ attName ].append( attValue )
    #negCond is something like : {'JobType': ['Merge']}
    return S_OK( negCond )

  def updateDelayCounters( self, siteName, jid ):
    #Get the info from the CS
    siteSection = "%s/%s" % ( self.__matchingDelaySection, siteName )
    result = self.__extractCSData( siteSection )
    if not result['OK']:
      return result
    delayDict = result[ 'Value' ]
    #limitsDict is something like { 'JobType' : { 'Merge' : 20, 'MCGen' : 1000 } }
    if not delayDict:
      return S_OK()
    attNames = []
    for attName in delayDict:
      if attName not in gJobDB.jobAttributeNames:
        gLogger.error( "Attribute %s does not exist in the JobDB. Please fix it!" % attName )
      else:
        attNames.append( attName )
    result = gJobDB.getJobAttributes( jid, attNames )
    if not result[ 'OK' ]:
      gLogger.error( "While retrieving attributes coming from %s: %s" % ( siteSection, result[ 'Message' ] ) )
      return result
    atts = result[ 'Value' ]
    #Create the DictCache if not there
    if siteName not in Limiter.__delayMem:
      Limiter.__delayMem[ siteName ] = DictCache()
    #Update the counters
    delayCounter = Limiter.__delayMem[ siteName ]
    for attName in atts:
      attValue = atts[ attName ]
      if attValue in delayDict[ attName ]:
        delayTime = delayDict[ attName ][ attValue ]
        gLogger.notice( "Adding delay for %s/%s=%s of %s secs" % ( siteName, attName,
                                                                   attValue, delayTime ) )
        delayCounter.add( ( attName, attValue ), delayTime )
    return S_OK()

  def __getDelayCondition( self, siteName ):
    """ Get extra conditions allowing matching delay
    """
    if siteName not in Limiter.__delayMem:
      return S_OK( {} )
    lastRun = Limiter.__delayMem[ siteName ].getKeys()
    negCond = {}
    for attName, attValue in lastRun:
      if attName not in negCond:
        negCond[ attName ] = []
      negCond[ attName ].append( attValue )
    return S_OK( negCond )


#####
#
#  End of Limiter
#
#####

class MatcherHandler( RequestHandler ):

  __opsCache = {}

  def initialize( self ):
    self.__opsHelper = self.__getOpsHelper()
    self.__limiter = Limiter( self.__opsHelper )
    self.__siteStatus = SiteStatus()

  def __getOpsHelper( self, setup = False, vo = False ):
    if not setup:
      setup = self.srv_getClientSetup()
    if not vo:
      vo = Registry.getVOForGroup( self.getRemoteCredentials()[ 'group' ] )
    cKey = ( vo, setup )
    if cKey not in MatcherHandler.__opsCache:
      MatcherHandler.__opsCache[ cKey ] = Operations.Operations( vo = vo, setup = setup )
    return MatcherHandler.__opsCache[ cKey ]

  def __processResourceDescription( self, resourceDescription ):
    # Check and form the resource description dictionary
    resourceDict = {}
    if type( resourceDescription ) in StringTypes:
      classAdAgent = ClassAd( resourceDescription )
      if not classAdAgent.isOK():
        return S_ERROR( 'Illegal Resource JDL' )
      gLogger.verbose( classAdAgent.asJDL() )

      for name in gTaskQueueDB.getSingleValueTQDefFields():
        if classAdAgent.lookupAttribute( name ):
          if name == 'CPUTime':
            resourceDict[name] = classAdAgent.getAttributeInt( name )
          else:
            resourceDict[name] = classAdAgent.getAttributeString( name )

      for name in gTaskQueueDB.getMultiValueMatchFields():
        if classAdAgent.lookupAttribute( name ):
          if name == 'SubmitPool':
            resourceDict[name] = classAdAgent.getListFromExpression( name )      
          else:
            resourceDict[name] = classAdAgent.getAttributeString( name )

      # Check if a JobID is requested
      if classAdAgent.lookupAttribute( 'JobID' ):
        resourceDict['JobID'] = classAdAgent.getAttributeInt( 'JobID' )

      for k in ( 'DIRACVersion', 'ReleaseVersion', 'ReleaseProject', 'VirtualOrganization' ):
        if classAdAgent.lookupAttribute( k ):
          resourceDict[ k ] = classAdAgent.getAttributeString( k )
          
    else:
      for name in gTaskQueueDB.getSingleValueTQDefFields():
        if resourceDescription.has_key( name ):
          resourceDict[name] = resourceDescription[name]

      for name in gTaskQueueDB.getMultiValueMatchFields():
        if resourceDescription.has_key( name ):
          resourceDict[name] = resourceDescription[name]

      if resourceDescription.has_key( 'JobID' ):
        resourceDict['JobID'] = resourceDescription['JobID']

      for k in ( 'DIRACVersion', 'ReleaseVersion', 'ReleaseProject', 'VirtualOrganization',
                 'PilotReference', 'PilotInfoReportedFlag', 'PilotBenchmark' ):
        if k in resourceDescription:
          resourceDict[ k ] = resourceDescription[ k ]

    return resourceDict

  def selectJob( self, resourceDescription ):
    """ Main job selection function to find the highest priority job
        matching the resource capacity
    """

    startTime = time.time()
    resourceDict = self.__processResourceDescription( resourceDescription )

    credDict = self.getRemoteCredentials()
    #Check credentials if not generic pilot
    if Properties.GENERIC_PILOT in credDict[ 'properties' ]:
      #You can only match groups in the same VO
      vo = Registry.getVOForGroup( credDict[ 'group' ] )
      result = Registry.getGroupsForVO( vo )
      if result[ 'OK' ]:
        resourceDict[ 'OwnerGroup' ] = result[ 'Value' ]
    else:
      #If it's a private pilot, the DN has to be the same
      if Properties.PILOT in credDict[ 'properties' ]:
        gLogger.notice( "Setting the resource DN to the credentials DN" )
        resourceDict[ 'OwnerDN' ] = credDict[ 'DN' ]
      #If it's a job sharing. The group has to be the same and just check that the DN (if any)
      # belongs to the same group
      elif Properties.JOB_SHARING in credDict[ 'properties' ]:
        resourceDict[ 'OwnerGroup' ] = credDict[ 'group' ]
        gLogger.notice( "Setting the resource group to the credentials group" )
        if 'OwnerDN'  in resourceDict and resourceDict[ 'OwnerDN' ] != credDict[ 'DN' ]:
          ownerDN = resourceDict[ 'OwnerDN' ]
          result = Registry.getGroupsForDN( resourceDict[ 'OwnerDN' ] )
          if not result[ 'OK' ] or credDict[ 'group' ] not in result[ 'Value' ]:
            #DN is not in the same group! bad boy.
            gLogger.notice( "You cannot request jobs from DN %s. It does not belong to your group!" % ownerDN )
            resourceDict[ 'OwnerDN' ] = credDict[ 'DN' ]
      #Nothing special, group and DN have to be the same
      else:
        resourceDict[ 'OwnerDN' ] = credDict[ 'DN' ]
        resourceDict[ 'OwnerGroup' ] = credDict[ 'group' ]

    # Check the pilot DIRAC version
    if self.__opsHelper.getValue( "Pilot/CheckVersion", True ):
      if 'ReleaseVersion' not in resourceDict:
        if not 'DIRACVersion' in resourceDict:
          return S_ERROR( 'Version check requested and not provided by Pilot' )
        else:
          pilotVersion = resourceDict['DIRACVersion']
      else:
        pilotVersion = resourceDict['ReleaseVersion']

      validVersions = self.__opsHelper.getValue( "Pilot/Version", [] )
      if validVersions and pilotVersion not in validVersions:
        return S_ERROR( 'Pilot version does not match the production version %s not in ( %s )' % \
                       ( pilotVersion, ",".join( validVersions ) ) )
      #Check project if requested
      validProject = self.__opsHelper.getValue( "Pilot/Project", "" )
      if validProject:
        if 'ReleaseProject' not in resourceDict:
          return S_ERROR( "Version check requested but expected project %s not received" % validProject )
        if resourceDict[ 'ReleaseProject' ] != validProject:
          return S_ERROR( "Version check requested but expected project %s != received %s" % ( validProject,
                                                                                               resourceDict[ 'ReleaseProject' ] ) )

    # Update pilot information
    pilotInfoReported = False
    pilotReference = resourceDict.get( 'PilotReference', '' )
    if pilotReference:
      if "PilotInfoReportedFlag" in resourceDict and not resourceDict['PilotInfoReportedFlag']:
        gridCE = resourceDict.get( 'GridCE', 'Unknown' )
        site = resourceDict.get( 'Site', 'Unknown' )
        benchmark = benchmark = resourceDict.get( 'PilotBenchmark', 0.0 )
        gLogger.verbose('Reporting pilot info for %s: gridCE=%s, site=%s, benchmark=%f' % (pilotReference,gridCE,site,benchmark) )
        result = gPilotAgentsDB.setPilotStatus( pilotReference, status = 'Running',
                                                gridSite = site,
                                                destination = gridCE,
                                                benchmark = benchmark )
        if result['OK']:
          pilotInfoReported = True                                        
    
    #Check the site mask
    if not 'Site' in resourceDict:
      return S_ERROR( 'Missing Site Name in Resource JDL' )

    # Get common site mask and check the agent site
    result = self.__siteStatus.getUsableSites( 'ComputingAccess' )
    if not result['OK']:
      return S_ERROR( 'Internal error: can not get site mask' )
    usableSites = result['Value']

    siteName = resourceDict['Site']
    if siteName not in maskList:
      
      # if 'GridCE' not in resourceDict:
      #  return S_ERROR( 'Site not in mask and GridCE not specified' )
      # Even if the site is banned, if it defines a CE, it must be able to check it
      # del resourceDict['Site']
      
      # Banned site can only take Test jobs 
      resourceDict['JobType'] = 'Test'

    resourceDict['Setup'] = self.serviceInfoDict['clientSetup']

    gLogger.verbose( "Resource description:" )
    for key in resourceDict:
      gLogger.verbose( "%s : %s" % ( key.rjust( 20 ), resourceDict[ key ] ) )

    negativeCond = self.__limiter.getNegativeCondForSite( siteName )
    result = gTaskQueueDB.matchAndGetJob( resourceDict, negativeCond = negativeCond )

    if DEBUG:
      print result

    if not result['OK']:
      return result
    result = result['Value']
    if not result['matchFound']:
      return S_ERROR( 'No match found' )

    jobID = result['jobId']
    resAtt = gJobDB.getJobAttributes( jobID, ['OwnerDN', 'OwnerGroup', 'Status'] )
    if not resAtt['OK']:
      return S_ERROR( 'Could not retrieve job attributes' )
    if not resAtt['Value']:
      return S_ERROR( 'No attributes returned for job' )
    if not resAtt['Value']['Status'] == 'Waiting':
      gLogger.error( 'Job matched by the TQ is not in Waiting state', str( jobID ) )
      result = gTaskQueueDB.deleteJob( jobID )
      if not result[ 'OK' ]:
        return result
      return S_ERROR( "Job %s is not in Waiting state" % str( jobID ) )

    attNames = ['Status','MinorStatus','ApplicationStatus','Site']
    attValues = ['Matched','Assigned','Unknown',siteName]
    result = gJobDB.setJobAttributes( jobID, attNames, attValues )
    # result = gJobDB.setJobStatus( jobID, status = 'Matched', minor = 'Assigned' )
    result = gJobLoggingDB.addLoggingRecord( jobID,
                                           status = 'Matched',
                                           minor = 'Assigned',
                                           source = 'Matcher' )

    result = gJobDB.getJobJDL( jobID )
    if not result['OK']:
      return S_ERROR( 'Failed to get the job JDL' )

    resultDict = {}
    resultDict['JDL'] = result['Value']
    resultDict['JobID'] = jobID

    matchTime = time.time() - startTime
    gLogger.info( "Match time: [%s]" % str( matchTime ) )
    gMonitor.addMark( "matchTime", matchTime )

    # Get some extra stuff into the response returned
    resOpt = gJobDB.getJobOptParameters( jobID )
    if resOpt['OK']:
      for key, value in resOpt['Value'].items():
        resultDict[key] = value
    resAtt = gJobDB.getJobAttributes( jobID, ['OwnerDN', 'OwnerGroup'] )
    if not resAtt['OK']:
      return S_ERROR( 'Could not retrieve job attributes' )
    if not resAtt['Value']:
      return S_ERROR( 'No attributes returned for job' )

    if self.__opsHelper.getValue( "JobScheduling/CheckMatchingDelay", True ):
      self.__limiter.updateDelayCounters( siteName, jobID )

    # Report pilot-job association
    if pilotReference:
      result = gPilotAgentsDB.setCurrentJobID( pilotReference, jobID )
      result = gPilotAgentsDB.setJobForPilot( jobID, pilotReference, updateStatus=False )

    resultDict['DN'] = resAtt['Value']['OwnerDN']
    resultDict['Group'] = resAtt['Value']['OwnerGroup']
    resultDict['PilotInfoReportedFlag'] = pilotInfoReported
    return S_OK( resultDict )

##############################################################################
  types_requestJob = [ [StringType, DictType] ]
  def export_requestJob( self, resourceDescription ):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """

    result = self.selectJob( resourceDescription )
    gMonitor.addMark( "matchesDone" )
    if result[ 'OK' ]:
      gMonitor.addMark( "matchesOK" )
    return result

##############################################################################
  types_getActiveTaskQueues = []
  def export_getActiveTaskQueues( self ):
    """ Return all task queues
    """
    return gTaskQueueDB.retrieveTaskQueues()

##############################################################################
  types_getMatchingTaskQueues = [ DictType ]
  def export_getMatchingTaskQueues( self, resourceDict ):
    """ Return all task queues
    """
    if 'Site' in resourceDict and type( resourceDict[ 'Site' ] ) in StringTypes:
      negativeCond = self.__limiter.getNegativeCondForSite( resourceDict[ 'Site' ] )
    else:
      negativeCond = self.__limiter.getNegativeCond()
    return gTaskQueueDB.retrieveTaskQueuesThatMatch( resourceDict, negativeCond = negativeCond )

##############################################################################
  types_matchAndGetTaskQueue = [ DictType ]
  def export_matchAndGetTaskQueue( self, resourceDict ):
    """ Return matching task queues
    """
    return gTaskQueueDB.matchAndGetTaskQueue( resourceDict )

