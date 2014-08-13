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


  def checkSEsRSS( self, checkSEs = None, access = 'WriteAccess' ):
    """ check SEs.
        By default, we check the SEs for WriteAccess, but it is configurable
    """
    if not checkSEs:
      checkSEs = self.operation.targetSEList
    elif type( checkSEs ) == str:
      checkSEs = [checkSEs]

    if access == 'ReadAccess':
      seType = 'sourceSE'
    else:
      seType = 'targetSE'
    bannedSEs = []
    for checkSE in checkSEs:
      seStatus = self.rssSEStatus( checkSE, access, retries = 5 )
      if not seStatus["OK"]:
        self.log.error( seStatus["Message"] )
        error = "unknown %s: %s" % ( seType, checkSE )
        for opFile in self.operation:
          opFile.Error = error
        self.operation.Error = error
        return S_ERROR( error )

      if not seStatus["Value"]:
        self.log.info( "%s %s is banned for %s right now" % ( seType.capitalize(), checkSE, access ) )
        bannedSEs.append( checkSE )
        self.operation.Error = "banned %s: %s;" % ( seType, checkSE )

    return S_OK( bannedSEs )


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
