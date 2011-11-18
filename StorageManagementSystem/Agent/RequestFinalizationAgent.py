# $HeadURL$

__RCSID__ = "$Id$"

from DIRAC import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath

from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.StorageManagementSystem.Client.StorageManagerClient  import StorageManagerClient
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
import time, os, sys, re
from types import *
from DIRAC.StorageManagementSystem.DB.StorageManagementDB       import StorageManagementDB

AGENT_NAME = 'StorageManagement/RequestFinalizationAgent'

class RequestFinalizationAgent( AgentModule ):

  def initialize( self ):

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )
    self.storageDB = StorageManagementDB()
    #self.stagerClient = StorageManagerClient()
    return S_OK()

  def execute( self ):
    res = self.clearFailedTasks()
    res = self.callbackStagedTasks()
    res = self.removeUnlinkedReplicas()
    res = self.setOldTasksAsFailed( self.am_getOption( 'FailIntervalDay', 3 ) )
    return res

  def clearFailedTasks( self ):
    """ This obtains the tasks which are marked as Failed and remove all the associated records
    """
    res = self.storageDB.getTasksWithStatus( 'Failed' )
    if not res['OK']:
      gLogger.fatal( "RequestFinalization.clearFailedTasks: Failed to get Failed Tasks from StagerDB.", res['Message'] )
      return res
    failedTasks = res['Value']
    gLogger.info( "RequestFinalization.clearFailedTasks: Obtained %s tasks in the 'Failed' status." % len( failedTasks ) )
    for taskID, ( source, callback, sourceTask ) in failedTasks.items():
      if ( callback and sourceTask ):
        res = self.__performCallback( 'Failed', callback, sourceTask )
        if not res['OK']:
          failedTasks.pop( taskID )
    if not failedTasks:
      gLogger.info( "RequestFinalization.clearFailedTasks: No tasks to remove." )
      return S_OK()
    gLogger.info( "RequestFinalization.clearFailedTasks: Removing %s tasks..." % len( failedTasks ) )
    res = self.storageDB.removeTasks( failedTasks.keys() )
    if not res['OK']:
      gLogger.error( "RequestFinalization.clearFailedTasks: Failed to remove tasks.", res['Message'] )
      return res
    gLogger.info( "RequestFinalization.clearFailedTasks: ...removed." )
    return S_OK()

  def callbackDoneTasks( self ):
    """ This issues the call back message for the Tasks with a State='Done'
    """
    res = self.storageDB.getTasksWithStatus( 'Done' )
    if not res['OK']:
      gLogger.fatal( "RequestFinalization.callbackDoneTasks: Failed to get Done Tasks from StorageManagementDB.", res['Message'] )
      return res
    doneTasks = res['Value']
    gLogger.info( "RequestFinalization.callbackDoneTasks: Obtained %s tasks in the 'Done' status." % len( doneTasks ) )
    for taskID, ( source, callback, sourceTask ) in doneTasks.items():
      if ( callback and sourceTask ):
        res = self.__performCallback( 'Done', callback, sourceTask )
        if not res['OK']:
          doneTasks.pop( taskID )
    if not doneTasks:
      gLogger.info( "RequestFinalization.callbackDoneTasks: No tasks to update to Done." )
      return S_OK()
    res = self.storageDB.removeTasks( doneTasks.keys() )
    if not res['OK']:
      gLogger.fatal( "RequestFinalization.callbackDoneTasks: Failed to remove Done tasks.", res['Message'] )
    return res

  def callbackStagedTasks( self ):
    """ This updates the status of the Tasks to Done then issues the call back message
    """
    res = self.storageDB.getTasksWithStatus( 'Staged' )
    if not res['OK']:
      gLogger.fatal( "RequestFinalization.callbackStagedTasks: Failed to get Staged Tasks from StagerDB.", res['Message'] )
      return res
    stagedTasks = res['Value']
    gLogger.info( "RequestFinalization.callbackStagedTasks: Obtained %s tasks in the 'Staged' status." % len( stagedTasks ) )
    for taskID, ( source, callback, sourceTask ) in stagedTasks.items():
      if ( callback and sourceTask ):
        res = self.__performCallback( 'Done', callback, sourceTask )
        if not res['OK']:
          stagedTasks.pop( taskID )
        else:
          gLogger.info( "RequestFinalization.callbackStagedTasks, Task = %s: %s" % ( sourceTask, res['Value'] ) )

    if not stagedTasks:
      gLogger.info( "RequestFinalization.callbackStagedTasks: No tasks to update to Done." )
      return S_OK()
    # Daniela: Why is the line below commented out?
    #res = self.stagerClient.setTasksDone(stagedTasks.keys())
    res = self.storageDB.removeTasks( stagedTasks.keys() )
    if not res['OK']:
      gLogger.fatal( "RequestFinalization.callbackStagedTasks: Failed to remove staged Tasks.", res['Message'] )
    return res

  def __performCallback( self, status, callback, sourceTask ):
    method, service = callback.split( '@' )
    gLogger.debug( "RequestFinalization.__performCallback: Attempting to perform call back for %s with %s status" % ( sourceTask, status ) )
    client = RPCClient( service )
    gLogger.debug( "RequestFinalization.__performCallback: Created RPCClient to %s" % service )
    execString = "res = client.%s('%s','%s')" % ( method, sourceTask, status )
    gLogger.debug( "RequestFinalization.__performCallback: Attempting to invoke %s service method" % method )
    exec( execString )
    if not res['OK']:
      gLogger.error( "RequestFinalization.__performCallback: Failed to perform callback", res['Message'] )
    else:
      gLogger.info( "RequestFinalization.__performCallback: Successfully issued callback to %s for %s with %s status" % ( callback, sourceTask, status ) )
    return res

  def removeUnlinkedReplicas( self ):
    gLogger.info( "RequestFinalization.removeUnlinkedReplicas: Attempting to cleanup unlinked Replicas." )
    res = self.storageDB.removeUnlinkedReplicas()
    if not res['OK']:
      gLogger.error( "RequestFinalization.removeUnlinkedReplicas: Failed to cleanup unlinked Replicas.", res['Message'] )
    else:
      gLogger.info( "RequestFinalization.removeUnlinkedReplicas: Successfully removed unlinked Replicas." )
    return res

  def clearReleasedTasks( self ):
    # TODO: issue release of the pins associated to this task
    res = self.storageDB.getTasksWithStatus( 'Released' )
    if not res['OK']:
      gLogger.fatal( "RequestFinalization.clearReleasedTasks: Failed to get Released Tasks from StagerDB.", res['Message'] )
      return res
    stagedTasks = res['Value']
    gLogger.info( "RequestFinalization.clearReleasedTasks: Removing %s tasks..." % len( stagedTasks ) )
    res = self.storageDB.removeTasks( stagedTasks.keys() )
    if not res['OK']:
      gLogger.error( "RequestFinalization.clearReleasedTasks: Failed to remove tasks.", res['Message'] )
      return res
    gLogger.info( "RequestFinalization.clearReleasedTasks: ...removed." )
    return S_OK()

  def setOldTasksAsFailed( self, daysOld ):
    gLogger.debug( "RequestFinalization.setOldTasksAsFailed: Attempting...." )
    res = self.storageDB.setOldTasksAsFailed( daysOld )
    if not res['OK']:
      gLogger.error( "RequestFinalization.setOldTasksAsFailed: Failed to set old tasks to a Failed state.", res['Message'] )
      return res
    return S_OK()
