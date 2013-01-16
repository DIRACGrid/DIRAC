""" Update all the Transformation counters, to speed up the production monitoring page loading
It requires the definition of Operations section Transformation/TasksStates and Transformation/FilesStates
Those are also used to define the columns in the TransformationCounters table
"""

from DIRAC                                                          import S_OK, S_ERROR, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from DIRAC.TransformationSystem.Client.TransformationClient         import TransformationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations

AGENT_NAME = 'Transformation/UpdateTransformationCounters'

class UpdateTransformationCounters(AgentModule):
  """ This agent is doing what getTransformationSummaryWeb does, but can take the time it needs
  """
  def __init__( self, agentName, loadName, baseAgentName, properties ):
    ''' c'tor
    '''
    AgentModule.__init__( agentName, loadName, baseAgentName, properties )

    self.fileLog = {}
    self.timeLog = {}
    self.fullTimeLog = {}
    self.pollingTime = self.am_getOption( 'PollingTime', 800 )
    self.TransfStatuses = Operations().getValue( 'Transformations/TransformationStatuses', ['Active',"Stopped"] )
    self.transClient = TransformationClient()

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
    result = self.transClient.getTransformations( condDict = {'Status': self.TransfStatuses }, timeout = 320  )
    if not result['OK']:
      gLogger.error( "UpdateTransformationCounters.execute: Failed to get transformations.", result['Message'] )
      return S_OK()
    # Process each transformation
    self.JobsStates = self.transClient.getTransformationCountersStatuses('Tasks')
    self.FilesStates= self.transClient.getTransformationCountersStatuses('Files')
    for transDict in result['Value']:
      transID = long( transDict['TransformationID'] )
      counterDict = {}
      counterDict['TransformationID'] = transID
      
      #Take care of the Tasks' states
      res = self.transClient.getTransformationTaskStats( transID )
      taskDict = {}
      if res['OK'] and res['Value']:
        taskDict = res['Value']
      else:
        gLogger.warn("UpdateTransformationCounters.execute: Something wrong with Task Statuses")  
      for state in self.JobsStates:
        counterDict[state] = taskDict.get( state, 0 ) 
      
      #Now look for the files' states  
      res = self.transClient.getTransformationStats( transID ) 
      fileDict = {}
      if res['OK'] and res['Value']:
        fileDict = res['Value']
      else:
        gLogger.warn("UpdateTransformationCounters.execute: Something wrong with File Statuses")
      for state in self.FilesStates:
        counterDict[state] = fileDict.get( state, 0 ) 
        
      res = self.transClient.updateTransformationCounters( counterDict )  
      if not res['OK']:
        gLogger.error("UpdateTransformationCounters.execute: failed updating counters", res['Message'])
    return S_OK()  
      