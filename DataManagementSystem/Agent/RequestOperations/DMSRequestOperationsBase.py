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
from DIRAC.DataManagementSystem.Utilities.DMSHelpers            import DMSHelpers
from DIRAC.ResourceStatusSystem.Client.ResourceStatus           import ResourceStatus

class DMSRequestOperationsBase( OperationHandlerBase ):

  def __init__( self, operation = None, csPath = None ):
    OperationHandlerBase.__init__( self, operation, csPath )
    self.registrationProtocols = DMSHelpers().getRegistrationProtocols()
    self.rssFlag = ResourceStatus().rssFlag


  def checkSEsRSS( self, checkSEs = None, access = 'WriteAccess' ):
    """ check SEs, returning a list of banned SEs.
        By default, we check the SEs for WriteAccess, but it is configurable

        :param checkSEs: SEs to check, by default the operation target list is used.
        :param access: The type of access to check for ('WriteAccess' or 'ReadAccess').
        :type checkSEs: list
        :type access: str
        :return: A list of banned SE names
        :rtype: list
    """
    # Only do these checks if RSS is enabled, otherwise return an empty list
    # (to indicate no SEs have been banned by RSS)
    if not self.rssFlag:
      return S_OK( [] )

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
        self.log.error( 'Failed to get SE status', seStatus["Message"] )
        error = "unknown %s: %s" % ( seType, checkSE )
        for opFile in self.operation:
          opFile.Error = error
        self.operation.Error = error
        return S_ERROR( error )

      if not seStatus["Value"]:
        self.log.info( "%s %s is banned for %s right now" % ( seType.capitalize(), checkSE, access ) )
        bannedSEs.append( checkSE )
        self.operation.Error = "banned %s: %s;" % ( seType, checkSE )


    if bannedSEs:
      alwaysBannedSEs = []
      for seName in bannedSEs:
        res = self.rssClient().isStorageElementAlwaysBanned( seName, access )
        if not res['OK']:
          continue

        # The SE will always be banned
        if res['Value']:
          alwaysBannedSEs.append( seName )


      # If Some SE are always banned, we fail the request
      if alwaysBannedSEs:
        self.log.info( "Some storages are always banned, failing the request", alwaysBannedSEs )
        for opFile in self.operation:
          opFile.Error = "%s always banned" % alwaysBannedSEs
          opFile.Status = "Failed"
        self.operation.Error = "%s always banned" % alwaysBannedSEs

      # If it is temporary, we wait an hour
      else:
        self.log.info( "Banning is temporary, next attempt in an hour" )
        self.operation.Error = "%s currently banned" % bannedSEs
        self.request.delayNextExecution( 60 )


    return S_OK( bannedSEs )


  def getRegisterOperation( self, opFile, targetSE, type = 'RegisterFile', catalog = None ):
    """ add RegisterReplica operation for file

    :param ~DIRAC.RequestManagementSystem.Client.File.File opFile: operation file
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
    registerFile.PFN = StorageElement( targetSE ).getURL( opFile.LFN, protocol = self.registrationProtocols ).get( 'Value', {} ).get( 'Successful', {} ).get( opFile.LFN )
    registerFile.GUID = opFile.GUID
    registerFile.Checksum = opFile.Checksum
    registerFile.ChecksumType = opFile.ChecksumType
    registerFile.Size = opFile.Size

    registerOperation.addFile( registerFile )
    return registerOperation
