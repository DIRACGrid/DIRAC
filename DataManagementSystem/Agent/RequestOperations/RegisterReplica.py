""" :mod: RegisterReplica
    ==================

    .. module: RegisterReplica
    :synopsis: register replica handler

    RegisterReplica operation handler
"""

__RCSID__ = "$Id $"

from DIRAC import gMonitor, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase

########################################################################
class RegisterReplica( OperationHandlerBase ):
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
    OperationHandlerBase.__init__( self, operation, csPath )
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

    print "YEAHHH"
    import time
    time.sleep( 2 )
    failedReplicas = 0
    # # catalog to use
    catalog = self.operation.Catalog
    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # loop over files
    for opFile in waitingFiles:

      gMonitor.addMark( "RegisterReplicaAtt", 1 )

      # # get LFN
      lfn = opFile.LFN
      # # and others
      replicaTuple = ( lfn , opFile.PFN, self.operation.targetSEList[0] )
      # # call ReplicaManager
      registerReplica = self.replicaManager().registerReplica( replicaTuple, catalog )
      # # check results
      if registerReplica["OK"] and lfn in registerReplica["Value"]["Successful"]:

        gMonitor.addMark( "RegisterReplicaOK", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "RegisterReplicaOK", catalog, "", "RegisterReplica" )

        self.log.info( "Replica %s has been registered at %s" % ( lfn, catalog ) )
        opFile.Status = "Done"
      else:

        gMonitor.addMark( "RegisterReplicaFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "RegisterReplicaFail", catalog, "", "RegisterReplica" )

        reason = registerReplica.get( "Message",
                                     registerReplica.get( ( "Value",
                                                          {} ).registerReplica.get( "Failed",
                                                                                  {} ).get( lfn ) ) )
        errorStr = "failed to register LFN %s: %s" % ( lfn, reason )
        opFile.Error = errorStr
        self.log.warn( errorStr )
        failedReplicas += 1

    # # final check
    if failedReplicas:
      self.log.info( "all replicas processed, %s replicas failed to register" % failedReplicas )
      self.operation.Error = "some replicas failed to register"
      return S_ERROR( self.operation.Error )

    return S_OK()



