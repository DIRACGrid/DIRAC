""" Update all the Transformation counters, to speed up the production monitoring page loading
    It requires the definition of Operations section Transformation/TasksStates and Transformation/FilesStates
    Those are also used to define the columns in the TransformationCounters table
"""

from DIRAC                                                          import S_OK, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from DIRAC.TransformationSystem.Client.TransformationClient         import TransformationClient

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/UpdateTransformationCounters'

class UpdateTransformationCounters( AgentModule ):
  """ This agent is doing what getTransformationSummaryWeb does, but can take the time it needs
  """
  def __init__( self, *args, **kwargs ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    """
    AgentModule.__init__( self, *args, **kwargs )

    self.transClient = TransformationClient()
    self.transfStatuses = self.am_getOption( 'TransformationStatuses', ['Active', 'Stopped'] )

  def initialize( self ):
    ''' Make the necessary initializations
    '''
    gMonitor.registerActivity( "Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM )
    return S_OK()

  def execute( self ):
    ''' Main execution method
    '''

    gMonitor.addMark( 'Iteration', 1 )
    # Get all the transformations
    result = self.transClient.getTransformations( condDict = {'Status': self.transfStatuses }, timeout = 320 )
    if not result['OK']:
      gLogger.error( "UpdateTransformationCounters.execute: Failed to get transformations.", result['Message'] )
      return S_OK()
    # Process each transformation
    jobsStates = self.transClient.getTransformationCountersStatuses( 'Tasks' )['Value']
    filesStates = self.transClient.getTransformationCountersStatuses( 'Files' )['Value']

    for transDict in result['Value']:
      transID = long( transDict['TransformationID'] )
      gLogger.debug( "Looking at transformationID %d" % transID )
      counterDict = {}
      counterDict['TransformationID'] = transID

      #Take care of the Tasks' states
      gLogger.verbose( "Getting the tasks stats for Transformation %s" % transID )
      res = self.transClient.getTransformationTaskStats( transID )
      if not res['OK']:
        gLogger.warn( "Could not get Transformation Task Stats for transformation %s : %s" % ( transID,
                                                                                               res['Message'] ) )
        break
      else:
        taskDict = {}
        if res['Value']:
          taskDict = res['Value']
          gLogger.verbose( "Got %s tasks dict for transformation %s" % ( str( taskDict ), transID ) )
          for state in jobsStates:
            counterDict[state] = taskDict.get( state, 0 )
        else:
          gLogger.warn( "No Task Statuses found" )
          break

      #Now look for the files' states  
      gLogger.verbose( "Getting the files stats for Transformation %s" % transID )
      res = self.transClient.getTransformationStats( transID )
      if not res['OK']:
        gLogger.warn( "Could not get Transformation Stats for transformation %s : %s" % ( transID,
                                                                                          res['Message'] ) )
        break
      else:
        fileDict = {}
        if res['Value']:
          fileDict = res['Value']
          gLogger.debug( "Got %s file dict for transformation %s" % ( str( fileDict ), transID ) )
          for state in filesStates:
            counterDict[state] = fileDict.get( state, 0 )
        else:
          gLogger.warn( "No File Statuses found" )
          break

      gLogger.verbose( "Updating the counters for transformation %s" % transID )
      res = self.transClient.updateTransformationCounters( counterDict )
      if not res['OK']:
        gLogger.error( "Failed updating counters for transformation %s: %s" % ( transID, res['Message'] ) )
      else:
        gLogger.verbose( "Updated the counters of transformation %s" % transID )

    return S_OK()
