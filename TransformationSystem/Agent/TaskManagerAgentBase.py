""" NOTA BENE: This agent should NOT be run alone. Instead, it serves as a base class for extensions.

    The TaskManagerAgentBase is the base class to submit tasks to external systems,
    monitor and update the tasks and file status in the transformation DB.

    This agent is extended in WorkflowTaskAgent and RequestTaskAgent.
    In case you want to further extend it you are required to follow the note on the
    initialize method and on the _getClients method.
"""

__RCSID__ = "$Id$"

import time
import datetime
from Queue import Queue

from DIRAC import S_OK, gMonitor

from DIRAC.Core.Base.AgentModule                                    import AgentModule
from DIRAC.Core.Utilities.ThreadPool                                import ThreadPool
from DIRAC.TransformationSystem.Client.FileReport                   import FileReport
from DIRAC.Core.Security.ProxyInfo                                  import getProxyInfo

from DIRAC.TransformationSystem.Client.TaskManager                  import WorkflowTasks
from DIRAC.TransformationSystem.Client.TransformationClient         import TransformationClient
from DIRAC.TransformationSystem.Agent.TransformationAgentsUtilities import TransformationAgentsUtilities

AGENT_NAME = 'Transformation/TaskManagerAgentBase'

class TaskManagerAgentBase( AgentModule, TransformationAgentsUtilities ):
  """ To be extended. Please look at WorkflowTaskAgent and RequestTaskAgent.
  """

  def __init__( self, *args, **kwargs ):
    """ c'tor

        Always call this in the extension agent
    """
    AgentModule.__init__( self, *args, **kwargs )
    TransformationAgentsUtilities.__init__( self )

    self.transClient = None
    self.transType = []

    self.tasksPerLoop = 50

    self.owner = ''
    self.ownerGroup = ''
    self.ownerDN = ''

    self.pluginLocation = ''

    # for the threading
    self.transQueue = Queue()
    self.transInQueue = []
    self.transInThread = {}

  #############################################################################

  def initialize( self ):
    """ Agent initialization.

        The extensions MUST provide in the initialize method the following data members:
        - TransformationClient objects (self.transClient),
        - set the shifterProxy if different from the default one set here ('ProductionManager')
        - list of transformation types to be looked (self.transType)
    """

    gMonitor.registerActivity( "SubmittedTasks", "Automatically submitted tasks", "Transformation Monitoring", "Tasks",
                               gMonitor.OP_ACUM )

    self.pluginLocation = self.am_getOption( 'PluginLocation', 'DIRAC.TransformationSystem.Client.TaskManagerPlugin' )

    # Default clients
    self.transClient = TransformationClient()

    # setting up the threading
    maxNumberOfThreads = self.am_getOption( 'maxNumberOfThreads', 15 )
    threadPool = ThreadPool( maxNumberOfThreads, maxNumberOfThreads )
    self.log.verbose( "Multithreaded with %d threads" % maxNumberOfThreads )

    for i in xrange( maxNumberOfThreads ):
      threadPool.generateJobAndQueueIt( self._execute, [i] )

    return S_OK()

  def finalize( self ):
    """ graceful finalization
    """
    if self.transInQueue:
      self._logInfo( "Wait for threads to get empty before terminating the agent (%d tasks)" % len( self.transInThread ) )
      self.transInQueue = []
      while self.transInThread:
        time.sleep( 2 )
      self.log.info( "Threads are empty, terminating the agent..." )
    return S_OK()

  #############################################################################

  def execute( self ):
    """ The TaskManagerBase execution method is just filling the Queues of transformations that need to be processed
    """

    operationsOnTransformationDict = {}

    # Determine whether the task status is to be monitored and updated
    enableTaskMonitor = self.am_getOption( 'MonitorTasks', '' )
    if not enableTaskMonitor:
      self.log.verbose( "Monitoring of tasks is disabled. To enable it, create the 'MonitorTasks' option" )
    else:
      # Get the transformations for which the tasks have to be updated
      status = self.am_getOption( 'UpdateTasksStatus', ['Active', 'Completing', 'Stopped'] )
      transformations = self._selectTransformations( transType = self.transType, status = status, agentType = [] )
      if not transformations['OK']:
        self.log.warn( "Could not select transformations: %s" % transformations['Message'] )
      else:
        transformationIDsAndBodies = dict( [( transformation['TransformationID'],
                                              transformation['Body'] ) for transformation in transformations['Value']] )
        for transID, body in transformationIDsAndBodies.iteritems():
          operationsOnTransformationDict[transID] = {'Body': body, 'Operations': ['updateTaskStatus']}

    # Determine whether the task files status is to be monitored and updated
    enableFileMonitor = self.am_getOption( 'MonitorFiles', '' )
    if not enableFileMonitor:
      self.log.verbose( "Monitoring of files is disabled. To enable it, create the 'MonitorFiles' option" )
    else:
      # Get the transformations for which the files have to be updated
      status = self.am_getOption( 'UpdateFilesStatus', ['Active', 'Completing', 'Stopped'] )
      transformations = self._selectTransformations( transType = self.transType, status = status, agentType = [] )
      if not transformations['OK']:
        self.log.warn( "Could not select transformations: %s" % transformations['Message'] )
      else:
        transformationIDsAndBodies = dict( [( transformation['TransformationID'],
                                              transformation['Body'] ) for transformation in transformations['Value']] )
        for transID, body in transformationIDsAndBodies.iteritems():
          if transID in operationsOnTransformationDict:
            operationsOnTransformationDict[transID]['Operations'].append( 'updateFileStatus' )
          else:
            operationsOnTransformationDict[transID] = {'Body': body, 'Operations': ['updateFileStatus']}

    # Determine whether the checking of reserved tasks is to be performed
    enableCheckReserved = self.am_getOption( 'CheckReserved', '' )
    if not enableCheckReserved:
      self.log.verbose( "Checking of reserved tasks is disabled. To enable it, create the 'CheckReserved' option" )
    else:
      # Get the transformations for which the check of reserved tasks have to be performed
      status = self.am_getOption( 'CheckReservedStatus', ['Active', 'Completing', 'Stopped'] )
      transformations = self._selectTransformations( transType = self.transType, status = status, agentType = [] )
      if not transformations['OK']:
        self.log.warn( "Could not select transformations: %s" % transformations['Message'] )
      else:
        transformationIDsAndBodies = dict( [( transformation['TransformationID'],
                                              transformation['Body'] ) for transformation in transformations['Value']] )
        for transID, body in transformationIDsAndBodies.iteritems():
          if transID in operationsOnTransformationDict:
            operationsOnTransformationDict[transID]['Operations'].append( 'checkReservedTasks' )
          else:
            operationsOnTransformationDict[transID] = {'Body': body, 'Operations': ['checkReservedTasks']}

    # Determine whether the submission of tasks is to be performed
    enableSubmission = self.am_getOption( 'SubmitTasks', '' )
    if not enableSubmission:
      self.log.verbose( "Submission of tasks is disabled. To enable it, create the 'SubmitTasks' option" )
    else:
      # getting the credentials for submission
      res = getProxyInfo( False, False )
      if not res['OK']:
        self.log.error( "Failed to determine credentials for submission", res['Message'] )
        return res
      proxyInfo = res['Value']
      self.owner = proxyInfo['username']
      self.ownerGroup = proxyInfo['group']
      self.ownerDN = proxyInfo['identity']
      self.log.info( "Tasks will be submitted with the credentials %s:%s" % ( self.owner, self.ownerGroup ) )
      # Get the transformations for which the check of reserved tasks have to be performed
      status = self.am_getOption( 'SubmitStatus', ['Active', 'Completing'] )
      transformations = self._selectTransformations( transType = self.transType, status = status )
      if not transformations['OK']:
        self.log.warn( "Could not select transformations: %s" % transformations['Message'] )
      else:
        # Get the transformations which should be submitted
        self.tasksPerLoop = self.am_getOption( 'TasksPerLoop', self.tasksPerLoop )
        transformationIDsAndBodies = dict( [( transformation['TransformationID'],
                                              transformation['Body'] ) for transformation in transformations['Value']] )
        for transID, body in transformationIDsAndBodies.iteritems():
          if transID in operationsOnTransformationDict:
            operationsOnTransformationDict[transID]['Operations'].append( 'submitTasks' )
          else:
            operationsOnTransformationDict[transID] = {'Body': body, 'Operations': ['submitTasks']}

    self._fillTheQueue( operationsOnTransformationDict )

    return S_OK()

  def _selectTransformations( self, transType = [], status = ['Active', 'Completing'], agentType = ['Automatic'] ):
    """ get the transformations
    """
    selectCond = {}
    if status:
      selectCond['Status'] = status
    if transType:
      selectCond['Type'] = transType
    if agentType:
      selectCond['AgentType'] = agentType
    res = self.transClient.getTransformations( condDict = selectCond )
    if not res['OK']:
      self.log.error( "Failed to get transformations: %s" % res['Message'] )
    elif not res['Value']:
      self.log.verbose( "No transformations found" )
    else:
      self.log.verbose( "Obtained %d transformations" % len( res['Value'] ) )
    return res

  def _fillTheQueue( self, operationsOnTransformationsDict ):
    """ Just fill the queue with the operation to be done on a certain transformation
    """
    count = 0
    for transID, bodyAndOps in operationsOnTransformationsDict.iteritems():
      if transID not in self.transInQueue:
        count += 1
        self.transInQueue.append( transID )
        self.transQueue.put( {transID: bodyAndOps} )

    self.log.info( "Out of %d transformations, %d put in thread queue" % ( len( operationsOnTransformationsDict ),
                                                                           count ) )

  #############################################################################

  def _getClients( self ):
    """ returns the clients used in the threads - this is another function that should be extended.

        The clients provided here are defaults, and should be adapted
    """
    threadTransformationClient = TransformationClient()
    threadTaskManager = WorkflowTasks()  # this is for wms tasks, replace it with something else if needed
    threadTaskManager.pluginLocation = self.pluginLocation

    return {'TransformationClient': threadTransformationClient,
            'TaskManager': threadTaskManager}

  def _execute( self, threadID ):
    """ This is what runs inside the threads, in practice this is the function that does the real stuff
    """
    # Each thread will have its own clients
    clients = self._getClients()
    startTime = 0
    method = '_execute'

    while True:
      transIDOPBody = self.transQueue.get()
      try:
        transID = transIDOPBody.keys()[0]
        operations = transIDOPBody[transID]['Operations']
        if transID not in self.transInQueue:
          self._logWarn( "Got a transf not in transInQueue...?", method = method, transID = transID )
          break
        self.transInThread[transID] = ' [Thread%d] [%s] ' % ( threadID, str( transID ) )
        clients['TaskManager'].transInThread = self.transInThread
        for operation in operations:
          self._logInfo( "Starting processing operation %s" % operation, method = method, transID = transID )
          startTime = time.time()
          res = getattr( self, operation )( transIDOPBody, clients )
          if not res['OK']:
            self._logError( "Failed to %s: %s" % ( operation, res['Message'] ), method = method, transID = transID )
          self._logInfo( "Processed operation %s in %.1f seconds" % ( operation, time.time() - startTime if startTime else time.time() ),
                         method = method, transID = transID )
      except Exception, x:
        self._logException( 'Exception executing operation %s' % operation, lException = x, transID = transID, method = method )
      finally:
        if not transID:
          transID = 'None'
        self._logInfo( "Processed transformation in %.1f seconds" % ( time.time() - startTime if startTime else time.time() ),
                       method = method, transID = transID )
        self._logVerbose( "%d transformations still in queue" % ( len( self.transInQueue ) - 1 ),
                          method = method, transID = transID )
        self.transInThread.pop( transID, None )
        if transID in self.transInQueue:
          self.transInQueue.remove( transID )
        self._logDebug( "transInQueue = %s" % str( self.transInQueue ), method = method, transID = transID )

  #############################################################################
  # real operations done

  def updateTaskStatus( self, transIDOPBody, clients ):
    """ Updates the task status
    """
    transID = transIDOPBody.keys()[0]
    method = 'updateTaskStatus'

    # Get the tasks which are in an UPDATE state
    updateStatus = self.am_getOption( 'TaskUpdateStatus', ['Checking', 'Deleted', 'Killed', 'Staging', 'Stalled',
                                                           'Matched', 'Scheduled', 'Rescheduled', 'Completed',
                                                           'Submitted', 'Assigned', 'Received',
                                                           'Waiting', 'Running'] )
    condDict = {"TransformationID":transID, "ExternalStatus":updateStatus}
    timeStamp = str( datetime.datetime.utcnow() - datetime.timedelta( minutes = 10 ) )
    transformationTasks = clients['TransformationClient'].getTransformationTasks( condDict = condDict,
                                                                                  older = timeStamp,
                                                                                  timeStamp = 'LastUpdateTime' )
    self._logDebug( "getTransformationTasks(%s) return value: %s" % ( str( condDict ), str( transformationTasks ) ),
                    method = method, transID = transID )
    if not transformationTasks['OK']:
      self._logError( "Failed to get tasks to update: %s" % transformationTasks['Message'],
                      method = method, transID = transID )
      return transformationTasks
    if not transformationTasks['Value']:
      self._logVerbose( "No tasks found to update", method = method, transID = transID )
      return transformationTasks
    self._logVerbose( "Getting %d tasks status" % len( transformationTasks['Value'] ),
                      method = method, transID = transID )
    submittedTaskStatus = clients['TaskManager'].getSubmittedTaskStatus( transformationTasks['Value'] )
    self._logDebug( "getSubmittedTaskStatus return value: %s" % str( submittedTaskStatus ),
                    method = method, transID = transID )
    if not submittedTaskStatus['OK']:
      self._logError( "Failed to get updated task states: %s" % submittedTaskStatus['Message'],
                      method = method, transID = transID )
      return submittedTaskStatus
    statusDict = submittedTaskStatus['Value']
    if not statusDict:
      self._logInfo( "No tasks to update", method = method, transID = transID )
      return submittedTaskStatus
    else:
      for status in sorted( statusDict ):
        taskIDs = statusDict[status]
        self._logInfo( "Updating %d task(s) to %s" % ( len( taskIDs ), status ),
                       method = method, transID = transID )
        setTaskStatus = clients['TransformationClient'].setTaskStatus( transID, taskIDs, status )
        self._logDebug( "setTaskStatus return value: %s" % str( setTaskStatus ),
                        method = method, transID = transID )
        if not setTaskStatus['OK']:
          self._logError( "Failed to update task status for transformation: %s" % setTaskStatus['Message'],
                          method = method, transID = transID )
          return setTaskStatus

    return S_OK()

  def updateFileStatus( self, transIDOPBody, clients ):
    """ Update the files status
    """
    transID = transIDOPBody.keys()[0]
    method = 'updateFileStatus'

    timeStamp = str( datetime.datetime.utcnow() - datetime.timedelta( minutes = 10 ) )
    condDict = {'TransformationID' : transID, 'Status' : ['Assigned']}
    transformationFiles = clients['TransformationClient'].getTransformationFiles( condDict = condDict,
                                                                                  older = timeStamp, timeStamp = 'LastUpdate' )
    self._logDebug( "getTransformationFiles(%s) return value: %s" % ( str( condDict ), transformationFiles ),
                   method = method, transID = transID )
    if not transformationFiles['OK']:
      self._logError( "Failed to get transformation files to update: %s" % transformationFiles['Message'],
                      method = method )
      return transformationFiles
    if not transformationFiles['Value']:
      self._logInfo( "No files to be updated", transID = transID, method = method )
      return transformationFiles
    submittedFileStatus = clients['TaskManager'].getSubmittedFileStatus( transformationFiles['Value'] )
    self._logDebug( "getSubmittedFileStatus return value: %s" % submittedFileStatus,
                    method = method, transID = transID )
    if not submittedFileStatus['OK']:
      self._logError( "Failed to get updated file states for transformation: %s" % submittedFileStatus['Message'],
                      transID = transID, method = method )
      return submittedFileStatus
    statusDict = submittedFileStatus['Value']
    if not statusDict:
      self._logInfo( "No file states to be updated", transID = transID, method = method )
      return submittedFileStatus
    fileReport = FileReport( server = clients['TransformationClient'].getServer() )
    for lfn, status in statusDict.items():
      setFileStatus = fileReport.setFileStatus( transID, lfn, status )
      if not setFileStatus['OK']:
        return  setFileStatus
    commit = fileReport.commit()
    if not commit['OK']:
      self._logError( "Failed to update file states for transformation: %s" % commit['Message'],
                      transID = transID, method = method )
      return commit
    else:
      self._logInfo( "Updated the states of %d files" % len( commit['Value'] ),
                     transID = transID, method = method )

    return S_OK()

  def checkReservedTasks( self, transIDOPBody, clients ):
    """ Checking Reserved tasks
    """
    transID = transIDOPBody.keys()[0]
    method = 'checkReservedTasks'

    # Select the tasks which have been in Reserved status for more than 1 hour for selected transformations
    condDict = {"TransformationID":transID, "ExternalStatus":'Reserved'}
    time_stamp_older = str( datetime.datetime.utcnow() - datetime.timedelta( hours = 1 ) )
    time_stamp_newer = str( datetime.datetime.utcnow() - datetime.timedelta( days = 7 ) )
    res = clients['TransformationClient'].getTransformationTasks( condDict = condDict, older = time_stamp_older,
                                                                  newer = time_stamp_newer )
    self._logDebug( "getTransformationTasks(%s) return value: %s" % ( condDict, res ),
                   method = method, transID = transID )
    if not res['OK']:
      self._logError( "Failed to get Reserved tasks: %s" % res['Message'],
                      transID = transID, method = method )
      return res
    if not res['Value']:
      self._logVerbose( "No Reserved tasks found", transID = transID )
      return res
    reservedTasks = res['Value']
    res = clients['TaskManager'].updateTransformationReservedTasks( reservedTasks )
    self._logDebug( "updateTransformationReservedTasks(%s) return value: %s" % ( reservedTasks, res ),
                   method = method, transID = transID )
    if not res['OK']:
      self._logError( "Failed to update transformation reserved tasks: %s" % res['Message'],
                      transID = transID, method = method )
      return res
    noTasks = res['Value']['NoTasks']
    taskNameIDs = res['Value']['TaskNameIDs']
    # For the tasks with no associated request found re-set the status of the task in the transformationDB
    for taskName in noTasks:
      transID, taskID = taskName.split( '_' )
      self._logInfo( "Resetting status of %s to Created as no associated task found" % ( taskName ),
                     transID = transID, method = method )
      res = clients['TransformationClient'].setTaskStatus( int( transID ), int( taskID ), 'Created' )
      if not res['OK']:
        self._logError( "Failed to update task status and ID after recovery: %s %s" % ( taskName, res['Message'] ),
                        transID = transID, method = method )
        return res
    # For the tasks for which an associated request was found update the task details in the transformationDB
    for taskName, extTaskID in taskNameIDs.items():
      transID, taskID = taskName.split( '_' )
      self._logInfo( "Setting status of %s to Submitted with ID %s" % ( taskName, extTaskID ),
                     transID = transID, method = method )
      setTaskStatusAndWmsID = clients['TransformationClient'].setTaskStatusAndWmsID( int( transID ), int( taskID ),
                                                                                     'Submitted', str( extTaskID ) )
      if not setTaskStatusAndWmsID['OK']:
        self._logError( "Failed to update task status and ID after recovery: %s %s" % ( taskName,
                                                                                        setTaskStatusAndWmsID['Message'] ),
                        transID = transID, method = method )
        return setTaskStatusAndWmsID

    return S_OK()

  def submitTasks( self, transIDOPBody, clients ):
    """ Submit the tasks to an external system, using the taskManager provided
    """
    transID = transIDOPBody.keys()[0]
    transBody = transIDOPBody[transID]['Body']
    method = 'submitTasks'

    tasksToSubmit = clients['TransformationClient'].getTasksToSubmit( transID, self.tasksPerLoop )
    self._logDebug( "getTasksToSubmit(%s, %s) return value: %s" % ( transID, self.tasksPerLoop, tasksToSubmit ),
                   method = method, transID = transID )
    if not tasksToSubmit['OK']:
      self._logError( "Failed to obtain tasks: %s" % tasksToSubmit['Message'], transID = transID, method = method )
      return tasksToSubmit
    tasks = tasksToSubmit['Value']['JobDictionary']
    if not tasks:
      self._logVerbose( "No tasks found for submission", transID = transID, method = method )
      return tasksToSubmit
    self._logInfo( "Obtained %d tasks for submission" % len( tasks ), transID = transID, method = method )
    preparedTransformationTasks = clients['TaskManager'].prepareTransformationTasks( transBody, tasks,
                                                                                     self.owner, self.ownerGroup, self.ownerDN )
    self._logDebug( "prepareTransformationTasks return value: %s" % preparedTransformationTasks,
                    method = method, transID = transID )
    if not preparedTransformationTasks['OK']:
      self._logError( "Failed to prepare tasks: %s" % preparedTransformationTasks['Message'],
                      transID = transID, method = method )
      return preparedTransformationTasks

    res = self.__actualSubmit( preparedTransformationTasks, clients, transID )
    if not res['OK']:
      return res
    res = clients['TaskManager'].updateDBAfterTaskSubmission( res['Value'] )
    self._logDebug( "updateDBAfterTaskSubmission return value: %s" % res, method = method, transID = transID )
    if not res['OK']:
      self._logError( "Failed to update DB after task submission: %s" % res['Message'],
                      transID = transID, method = method )
      return res

    return S_OK()

  def __actualSubmit( self, preparedTransformationTasks, clients, transID ):
    """ This function contacts either RMS or WMS depending on the type of transformation.
    """
    method = 'submitTasks'
    res = clients['TaskManager'].submitTransformationTasks( preparedTransformationTasks['Value'] )
    self._logDebug( "submitTransformationTasks return value: %s" % res, method = method, transID = transID )
    if not res['OK']:
      self._logError( "Failed to submit prepared tasks: %s" % res['Message'],
                      transID = transID, method = method )
    return res
