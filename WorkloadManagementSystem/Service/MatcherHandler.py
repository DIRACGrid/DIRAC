""" Matcher class. It matches Agent Site capabilities to job requirements.

    It also provides an XMLRPC interface to the Matcher
"""

__RCSID__ = "$Id$"

from types import StringType, DictType, StringTypes

from DIRAC                                             import gLogger, S_OK, S_ERROR

from DIRAC.ConfigurationSystem.Client.Helpers          import Registry, Operations
from DIRAC.Core.Utilities.ThreadScheduler              import gThreadScheduler
from DIRAC.Core.Utilities.DictCache                    import DictCache
from DIRAC.Core.DISET.RequestHandler                   import RequestHandler

from DIRAC.FrameworkSystem.Client.MonitoringClient     import gMonitor

from DIRAC.WorkloadManagementSystem.DB.JobDB           import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB     import TaskQueueDB

gJobDB = False
gTaskQueueDB = False

def initializeMatcherHandler( serviceInfo ):
  """  Matcher Service initialization
  """

  global gJobDB
  global gTaskQueueDB

  gJobDB = JobDB()
  gTaskQueueDB = TaskQueueDB()

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


class Limiter( object ):

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

  def __getOpsHelper( self, setup = False, vo = False ):
    if not setup:
      setup = self.srv_getClientSetup()
    if not vo:
      vo = Registry.getVOForGroup( self.getRemoteCredentials()[ 'group' ] )
    cKey = ( vo, setup )
    if cKey not in MatcherHandler.__opsCache:
      MatcherHandler.__opsCache[ cKey ] = Operations.Operations( vo = vo, setup = setup )
    return MatcherHandler.__opsCache[ cKey ]

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

