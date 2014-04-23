""" :mod: RegisterReplica
    ==================

    .. module: RegisterReplica
    :synopsis: register replica handler

    RegisterReplica operation handler
"""

__RCSID__ = "$Id $"

from DIRAC import gMonitor, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Agent.RequestOperations.DMSRequestOperationsBase  import DMSRequestOperationsBase

########################################################################
class RegisterReplica( DMSRequestOperationsBase ):
  """
  .. class:: RegisterReplica

  RegisterReplica operation handler
  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    DMSRequestOperationsBase.__init__( self, operation, csPath )
    # # RegisterReplica specific monitor info
    gMonitor.registerActivity( "RegisterReplicaAtt", "Attempted replicas registrations",
                               "RequestExecutingAgent", "Replicas/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterReplicaOK", "Successful replicas registrations",
                               "RequestExecutingAgent", "Replicas/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterReplicaFail", "Failed replicas registrations",
                               "RequestExecutingAgent", "Replicas/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ call me maybe """
    # # counter for failed replicas

    failedReplicas = 0
    # # catalog to use
    catalog = self.operation.Catalog
    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # loop over files
    registerOperations = {}
    for opFile in waitingFiles:

      gMonitor.addMark( "RegisterReplicaAtt", 1 )

      # # get LFN
      lfn = opFile.LFN
      # # and others
      targetSE = self.operation.targetSEList[0]
      replicaTuple = ( lfn , opFile.PFN, targetSE )
      # # call ReplicaManager
      registerReplica = self.dm.registerReplica( replicaTuple, catalog )
      # # check results
      if not registerReplica["OK"] or lfn in registerReplica["Value"]["Failed"]:
        # There have been some errors
        gMonitor.addMark( "RegisterReplicaFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "RegisterReplicaFail", catalog, "", "RegisterReplica" )

        reason = registerReplica.get( "Message", registerReplica.get( "Value", {} ).get( "Failed", {} ).get( lfn, 'Unknown' ) )
        errorStr = "failed to register LFN %s: %s" % ( lfn, reason )
        if lfn in registerReplica["Value"].get( "Successful", {} ) and type( reason ) == type( {} ):
          # As we managed, let's create a new operation for just the remaining registration
          errorStr += ' - adding registerReplica operations to request'
          for failedCatalog in reason.keys():
            key = '%s/%s' % ( targetSE, failedCatalog )
            newOperation = self.getRegisterOperation( opFile, targetSE, type = 'RegisterReplica', catalog = failedCatalog )
            if key not in registerOperations:
              registerOperations[key] = newOperation
            else:
              registerOperations[key].addFile( newOperation[0] )
          opFile.Status = 'Done'
        else:
          opFile.Error = errorStr
          # If one targets explicitly a catalog and it fails
          if catalog and ( 'file does not exist' in opFile.Error.lower() or 'no such file' in opFile.Error.lower() ) :
            opFile.Status = 'Failed'
          failedReplicas += 1
        self.log.warn( errorStr )

      else:
        # All is OK
        gMonitor.addMark( "RegisterReplicaOK", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "RegisterReplicaOK", catalog, "", "RegisterReplica" )

        self.log.info( "Replica %s has been registered at %s" % ( lfn, catalog ) )
        opFile.Status = "Done"

    # # if we have new replications to take place, put them at the end
    if registerOperations:
      self.log.info( "adding %d operations to the request" % len( registerOperations ) )
    for operation in registerOperations.values():
      self.operation._parent.addOperation( operation )
    # # final check
    if failedReplicas:
      self.log.info( "all replicas processed, %s replicas failed to register" % failedReplicas )
      self.operation.Error = "some replicas failed to register"
      return S_ERROR( self.operation.Error )

    return S_OK()

