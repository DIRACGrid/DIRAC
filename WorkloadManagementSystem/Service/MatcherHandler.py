""" The Matcher service provides an XMLRPC interface for matching jobs to pilots

    It uses a Matcher and a Limiter object that encapsulated the matching logic.
    It connects to JobDB, TaskQueueDB, and PilotAgentsDB.
"""

__RCSID__ = "$Id$"

from types import StringTypes, DictType, StringTypes

from DIRAC                                               import gLogger, S_OK, S_ERROR

from DIRAC.Core.Utilities.ThreadScheduler                import gThreadScheduler
from DIRAC.Core.DISET.RequestHandler                     import RequestHandler

from DIRAC.FrameworkSystem.Client.MonitoringClient       import gMonitor

from DIRAC.WorkloadManagementSystem.DB.JobDB             import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB       import TaskQueueDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB      import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB     import PilotAgentsDB

from DIRAC.WorkloadManagementSystem.Client.Matcher       import Matcher
from DIRAC.WorkloadManagementSystem.Client.Limiter       import Limiter
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

gJobDB = False
gTaskQueueDB = False


def initializeMatcherHandler( serviceInfo ):
  """  Matcher Service initialization
  """

  global gJobDB
  global gTaskQueueDB
  global jlDB
  global pilotAgentsDB

  gJobDB = JobDB()
  gTaskQueueDB = TaskQueueDB()
  jlDB = JobLoggingDB()
  pilotAgentsDB = PilotAgentsDB()

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

class MatcherHandler( RequestHandler ):

  def initialize( self ):
    self.limiter = Limiter( jobDB = gJobDB )

##############################################################################
  types_requestJob = [ list( StringTypes ) + [DictType] ]
  def export_requestJob( self, resourceDescription ):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """

    resourceDescription['Setup'] = self.serviceInfoDict['clientSetup']
    credDict = self.getRemoteCredentials()

    try:
      opsHelper = Operations( group = credDict['group'] )
      matcher = Matcher( pilotAgentsDB = pilotAgentsDB,
                         jobDB = gJobDB,
                         tqDB = gTaskQueueDB,
                         jlDB = jlDB,
                         opsHelper = opsHelper )
      result = matcher.selectJob( resourceDescription, credDict )
    except RuntimeError, rte:
      self.log.error( "Error requesting job: ", rte )
      return S_ERROR( "Error requesting job" )

    # result can be empty, meaning that no job matched
    if result:
      gMonitor.addMark( "matchesDone" )
      gMonitor.addMark( "matchesOK" )
      return S_OK( result )
    else:
      # FIXME: This is correctly interpreted by the JobAgent, but DErrno should be used instead
      return S_ERROR( "No match found" )

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
      negativeCond = self.limiter.getNegativeCondForSite( resourceDict[ 'Site' ] )
    else:
      negativeCond = self.limiter.getNegativeCond()
    return gTaskQueueDB.retrieveTaskQueuesThatMatch( resourceDict, negativeCond = negativeCond )

##############################################################################
  types_matchAndGetTaskQueue = [ DictType ]
  def export_matchAndGetTaskQueue( self, resourceDict ):
    """ Return matching task queues
    """
    return gTaskQueueDB.matchAndGetTaskQueue( resourceDict )

