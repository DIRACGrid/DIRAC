""" The TaskManagerAgentBase is the base class to submit tasks to external systems,
    monitor and update the tasks and file status in the transformation DB.
"""

import datetime
from DIRAC import S_OK, S_ERROR, gMonitor, gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/TaskManagerAgentBase'

class TaskManagerAgentBase( AgentModule ):
  """ To be extended. The extension needs to:
      - provide a taskManager object as data member
      - provide a shifterProxy (string) as data member
      - provide a transType (list of strings) as data member
  """

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    AgentModule.__init__( self, *args, **kwargs )

    self.taskManager = None
    self.shifterProxy = ''
    self.transClient = TransformationClient()
    self.transType = []

  #############################################################################

  def initialize( self ):
    """ agent initialization
    """
    if not self.taskManager:
      return S_ERROR( 'No task manager provided!' )

    if not self.shifterProxy:
      return S_ERROR( 'No shifter proxy provided!' )
    self.am_setOption( 'shifterProxy', self.shifterProxy )

    if not self.transType:
      return S_ERROR( 'No transformation types to look for!' )
    gLogger.info( "Looking for %s" % self.transType )

    gMonitor.registerActivity( "SubmittedTasks", "Automatically submitted tasks", "Transformation Monitoring", "Tasks",
                               gMonitor.OP_ACUM )

    return S_OK()

  #############################################################################

  def execute( self ):
    """ The TaskManagerBase execution method.
    """

    # Determine whether the task status is to be monitored and updated
    enableTaskMonitor = self.am_getOption( 'MonitorTasks', '' )
    if not enableTaskMonitor:
      gLogger.info( "execute: Monitoring of tasks is disabled." )
      gLogger.info( "execute: To enable create the 'MonitorTasks' option" )
    else:
      res = self.updateTaskStatus()
      if not res['OK']:
        gLogger.warn( 'execute: Failed to update task states', res['Message'] )

    # Determine whether the task files status is to be monitored and updated
    enableFileMonitor = self.am_getOption( 'MonitorFiles', '' )
    if not enableFileMonitor:
      gLogger.info( "execute: Monitoring of files is disabled." )
      gLogger.info( "execute: To enable create the 'MonitorFiles' option" )
    else:
      res = self.updateFileStatus()
      if not res['OK']:
        gLogger.warn( 'execute: Failed to update file states', res['Message'] )

    # Determine whether the checking of reserved tasks is to be performed
    enableCheckReserved = self.am_getOption( 'CheckReserved', '' )
    if not enableCheckReserved:
      gLogger.info( "execute: Checking of reserved tasks is disabled." )
      gLogger.info( "execute: To enable create the 'CheckReserved' option" )
    else:
      res = self.checkReservedTasks()
      if not res['OK']:
        gLogger.warn( 'execute: Failed to checked reserved tasks', res['Message'] )

    # Determine whether the submission of tasks is to be executed
    enableSubmission = self.am_getOption( 'SubmitTasks', '' )
    if not enableSubmission:
      gLogger.info( "execute: Submission of tasks is disabled." )
      gLogger.info( "execute: To enable create the 'SubmitTasks' option" )
    else:
      res = self.submitTasks()
      if not res['OK']:
        gLogger.warn( 'execute: Failed to submit created tasks', res['Message'] )

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
      gLogger.error( "_selectTransformations: Failed to get transformations for selection.", res['Message'] )
    elif not res['Value']:
      gLogger.verbose( "_selectTransformations: No transformations found for selection." )
    else:
      gLogger.verbose( "_selectTransformations: Obtained %d transformations for selection" % len( res['Value'] ) )
    return res

  def updateTaskStatus( self ):
    """ Updates the task status
    """
    gLogger.info( "updateTaskStatus: Updating the Status of tasks" )
    # Get the transformations to be updated
    status = self.am_getOption( 'UpdateTasksStatus', ['Active', 'Completing', 'Stopped'] )
    res = self._selectTransformations( transType = self.transType, status = status, agentType = [] )
    if not res['OK']:
      return res
    for transformation in res['Value']:
      transID = transformation['TransformationID']
      # Get the tasks which are in a UPDATE state
      updateStatus = self.am_getOption( 'TaskUpdateStatus', ['Checking', 'Deleted', 'Killed', 'Staging', 'Stalled',
                                                             'Matched', 'Scheduled', 'Rescheduled', 'Completed', 'Submitted',
                                                             'Assigned', 'Received', 'Waiting', 'Running'] )
      condDict = {"TransformationID":transID, "ExternalStatus":updateStatus}
      timeStamp = str( datetime.datetime.utcnow() - datetime.timedelta( minutes = 10 ) )
      res = self.transClient.getTransformationTasks( condDict = condDict,
                                                     older = timeStamp,
                                                     timeStamp = 'LastUpdateTime' )
      if not res['OK']:
        gLogger.error( "updateTaskStatus: Failed to get tasks to update for transformation", "%s %s" % ( transID,
                                                                                                     res['Message'] ) )
        continue
      if not res['Value']:
        gLogger.verbose( "updateTaskStatus: No tasks found to update for transformation %s" % transID )
        continue
      gLogger.verbose( "updateTaskStatus: getting %d tasks status of transformation %s" % ( len( res['Value'] ),
                                                                                            transID ) )
      res = self.taskManager.getSubmittedTaskStatus( res['Value'] )
      if not res['OK']:
        gLogger.error( "updateTaskStatus: Failed to get updated task statuses for transformation", "%s %s" % ( transID,
                                                                                                     res['Message'] ) )
        continue
      statusDict = res['Value']
      if not statusDict:
        gLogger.info( "updateTaskStatus: No tasks to update for transformation %d" % transID )
      else:
        for status in sorted( statusDict ):
          taskIDs = statusDict[status]
          gLogger.info( "updateTaskStatus: Updating %d task(s) from transformation %d to %s" % ( len( taskIDs ),
                                                                                                 transID, status ) )
          res = self.transClient.setTaskStatus( transID, taskIDs, status )
          if not res['OK']:
            gLogger.error( "updateTaskStatus: Failed to update task status for transformation", "%s %s" % ( transID,
                                                                                                       res['Message'] ) )

    gLogger.info( "updateTaskStatus: Transformation task status update complete" )
    return S_OK()

  def updateFileStatus( self ):
    """ Update the files status
    """
    gLogger.info( "updateFileStatus: Updating Status of task files" )
    # Get the transformations to be updated
    status = self.am_getOption( 'UpdateFilesStatus', ['Active', 'Completing', 'Stopped'] )
    res = self._selectTransformations( transType = self.transType, status = status, agentType = [] )
    if not res['OK']:
      return res
    for transformation in res['Value']:
      transID = transformation['TransformationID']
      timeStamp = str( datetime.datetime.utcnow() - datetime.timedelta( minutes = 10 ) )
      condDict = {'TransformationID' : transID, 'Status' : ['Assigned']}
      res = self.transClient.getTransformationFiles( condDict = condDict, older = timeStamp, timeStamp = 'LastUpdate' )
      if not res['OK']:
        gLogger.error( "updateFileStatus: Failed to get transformation files to update.", res['Message'] )
        continue
      if not res['Value']:
        gLogger.info( "updateFileStatus: No files to be updated for transformation %s." % transID )
        continue
      res = self.taskManager.getSubmittedFileStatus( res['Value'] )
      if not res['OK']:
        gLogger.error( "updateFileStatus: Failed to get updated file statuses for transformation", "%s %s" % ( transID,
                                                                                                     res['Message'] ) )
        continue
      statusDict = res['Value']
      if not statusDict:
        gLogger.info( "updateFileStatus: No file statuses to be updated for transformation %s." % transID )
        continue
      fileReport = FileReport( server = self.transClient.getServer() )
      for lfn, status in statusDict.items():
        fileReport.setFileStatus( int( transID ), lfn, status )
      res = fileReport.commit()
      if not res['OK']:
        gLogger.error( "updateFileStatus: Failed to update file status for transformation", "%s %s" % ( transID,
                                                                                                      res['Message'] ) )
      else:
        gLogger.info( "updateFileStatus: Updated  the status of %d files for transformation %s" % ( len( res['Value'] ),
                                                                                                    transID ) )
    gLogger.info( "updateFileStatus: Transformation file status update complete" )
    return S_OK()

  def checkReservedTasks( self ):
    gLogger.info( "checkReservedTasks: Checking Reserved tasks" )
    # Get the transformations which should be checked
    status = self.am_getOption( 'CheckReservedStatus', ['Active', 'Completing', 'Stopped'] )
    res = self._selectTransformations( transType = self.transType, status = status, agentType = [] )
    if not res['OK']:
      return res
    for transformation in res['Value']:
      transID = transformation['TransformationID']
      # Select the tasks which have been in Reserved status for more than 1 hour for selected transformations
      condDict = {"TransformationID":transID, "ExternalStatus":'Reserved'}
      time_stamp_older = str( datetime.datetime.utcnow() - datetime.timedelta( hours = 1 ) )
      time_stamp_newer = str( datetime.datetime.utcnow() - datetime.timedelta( days = 7 ) )
      res = self.transClient.getTransformationTasks( condDict = condDict, older = time_stamp_older,
                                                     newer = time_stamp_newer )
      if not res['OK']:
        gLogger.error( "checkReservedTasks: Failed to get Reserved tasks for transformation", "%s %s" % ( transID,
                                                                                                     res['Message'] ) )
        continue
      if not res['Value']:
        gLogger.verbose( "checkReservedTasks: No Reserved tasks found for transformation %s" % transID )
        continue
      res = self.taskManager.updateTransformationReservedTasks( res['Value'] )
      if not res['OK']:
        gLogger.info( "checkReservedTasks: No Reserved tasks found for transformation %s" % transID )
        continue
      noTasks = res['Value']['NoTasks']
      taskNameIDs = res['Value']['TaskNameIDs']
      # For the tasks with no associated request found re-set the status of the task in the transformationDB
      for taskName in noTasks:
        transID, taskID = taskName.split( '_' )
        gLogger.info( "checkReservedTasks: Resetting status of %s to Created as no associated task found" % ( taskName ) )
        res = self.transClient.setTaskStatus( int( transID ), int( taskID ), 'Created' )
        if not res['OK']:
          gLogger.warn( "checkReservedTasks: Failed to update task status and ID after recovery", "%s %s" % ( taskName,
                                                                                                      res['Message'] ) )
      # For the tasks for which an associated request was found update the task details in the transformationDB
      for taskName, extTaskID in taskNameIDs.items():
        transID, taskID = taskName.split( '_' )
        gLogger.info( "checkReservedTasks: Resetting status of %s to Created with ID %s" % ( taskName, extTaskID ) )
        res = self.transClient.setTaskStatusAndWmsID( int( transID ), int( taskID ), 'Submitted', str( extTaskID ) )
        if not res['OK']:
          gLogger.warn( "checkReservedTasks: Failed to update task status and ID after recovery", "%s %s" % ( taskName,
                                                                                                      res['Message'] ) )
    gLogger.info( "checkReservedTasks: Updating of reserved tasks complete" )
    return S_OK()

  def submitTasks( self ):
    """ Submit the tasks to an external system, using the taskManager provided
    """
    gLogger.info( "submitTasks: Submitting tasks for transformations" )
    res = getProxyInfo( False, False )
    if not res['OK']:
      gLogger.error( "submitTasks: Failed to determine credentials for submission", res['Message'] )
      return res
    proxyInfo = res['Value']
    owner = proxyInfo['username']
    ownerGroup = proxyInfo['group']
    ownerDN = proxyInfo['identity']
    gLogger.info( "submitTasks: Tasks will be submitted with the credentials %s:%s" % ( owner, ownerGroup ) )
    # Get the transformations which should be submitted
    tasksPerLoop = self.am_getOption( 'TasksPerLoop', 50 )
    status = self.am_getOption( 'SubmitStatus', ['Active', 'Completing'] )
    res = self._selectTransformations( transType = self.transType, status = status )
    if not res['OK']:
      return res
    for transformation in res['Value']:
      transID = transformation['TransformationID']
      transBody = transformation['Body']
      res = self.transClient.getTasksToSubmit( transID, tasksPerLoop )
      if not res['OK']:
        gLogger.error( "submitTasks: Failed to obtain tasks for transformation", "%s %s" % ( transID, res['Message'] ) )
        continue
      tasks = res['Value']['JobDictionary']
      if not tasks:
        gLogger.verbose( "submitTasks: No tasks found for submission for transformation %s" % transID )
        continue
      gLogger.info( "submitTasks: Obtained %d tasks for submission for transformation %s" % ( len( tasks ), transID ) )
      res = self.taskManager.prepareTransformationTasks( transBody, tasks, owner, ownerGroup, ownerDN )
      if not res['OK']:
        gLogger.error( "submitTasks: Failed to prepare tasks for transformation", "%s %s" % ( transID,
                                                                                              res['Message'] ) )
        continue
      res = self.taskManager.submitTransformationTasks( res['Value'] )
      if not res['OK']:
        gLogger.error( "submitTasks: Failed to submit prepared tasks for transformation", "%s %s" % ( transID,
                                                                                                      res['Message'] ) )
        continue
      res = self.taskManager.updateDBAfterTaskSubmission( res['Value'] )
      if not res['OK']:
        gLogger.error( "submitTasks: Failed to update DB after task submission for transformation", "%s %s" % ( transID,
                                                                                                     res['Message'] ) )
        continue
    gLogger.info( "submitTasks: Submission of transformation tasks complete" )
    return S_OK()
