""" Matcher class. It matches Agent Site capabilities to job requirements.

    It also provides an XMLRPC interface to the Matcher
"""

__RCSID__ = "$Id$"

from types import StringType, DictType, StringTypes

from DIRAC                                             import gLogger, S_OK, S_ERROR

from DIRAC.Core.Utilities.ThreadScheduler              import gThreadScheduler
from DIRAC.Core.DISET.RequestHandler                   import RequestHandler

from DIRAC.FrameworkSystem.Client.MonitoringClient     import gMonitor

from DIRAC.WorkloadManagementSystem.DB.JobDB           import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB     import TaskQueueDB
from DIRAC.WorkloadManagementSystem.Client.Matcher     import Matcher, Limiter

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

class MatcherHandler( RequestHandler ):

  def initialize( self ):
    self.limiter = Limiter()
    self.matcher = Matcher()

##############################################################################
  types_requestJob = [ [StringType, DictType] ]
  def export_requestJob( self, resourceDescription ):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """

    result = self.matcher.selectJob( resourceDescription )
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

