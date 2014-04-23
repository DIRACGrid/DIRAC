""" :mod: DMSRequestOperationsBase
    ====================

    Just a collector of common functions
"""

__RCSID__ = "$Id $"

from DIRAC import S_OK, S_ERROR

from DIRAC.RequestManagementSystem.Client.Operation             import Operation
from DIRAC.RequestManagementSystem.Client.File                  import File
from DIRAC.Resources.Storage.StorageElement                     import StorageElement
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase

class DMSRequestOperationsBase( OperationHandlerBase ):

  def __init__( self, operation = None, csPath = None ):
    OperationHandlerBase.__init__( self, operation, csPath )


  def checkSEsRSS( self, targetSEs = None, access = 'WriteAccess' ):
    """ check SEs.
        By default, we check the targetSEs for WriteAccess, but it is configurable
    """
    if not targetSEs:
      targetSEs = self.operation.targetSEList
    elif type( targetSEs ) == str:
      targetSEs = [targetSEs]

    bannedTargets = []
    for targetSE in targetSEs:
      writeStatus = self.rssSEStatus( targetSE, access, retries = 5 )
      if not writeStatus["OK"]:
        self.log.error( writeStatus["Message"] )
        for opFile in self.operation:
          opFile.Error = "unknown targetSE: %s" % targetSE
        self.operation.Error = "unknown targetSE: %s" % targetSE
        return S_ERROR( self.operation.Error )

      if not writeStatus["Value"]:
        self.log.info( "TargetSE %s is banned for %s right now" % ( targetSE, access ) )
        bannedTargets.append( targetSE )
        self.operation.Error = "banned targetSE: %s;" % targetSE

    return S_OK( bannedTargets )


  def getRegisterOperation( self, opFile, targetSE, type = 'RegisterFile', catalog = None ):
    """ add RegisterReplica operation for file

    :param File opFile: operation file
    :param str targetSE: target SE
    """
    # # add RegisterReplica operation
    registerOperation = Operation()
    registerOperation.Type = type
    registerOperation.TargetSE = targetSE
    if catalog:
      registerOperation.Catalog = catalog

    registerFile = File()
    registerFile.LFN = opFile.LFN
    registerFile.PFN = StorageElement( targetSE ).getPfnForLfn( opFile.LFN ).get( 'Value', {} ).get( 'Successful', {} ).get( opFile.LFN )
    registerFile.GUID = opFile.GUID
    registerFile.Checksum = opFile.Checksum
    registerFile.ChecksumType = opFile.ChecksumType
    registerFile.Size = opFile.Size

    registerOperation.addFile( registerFile )
    return registerOperation
