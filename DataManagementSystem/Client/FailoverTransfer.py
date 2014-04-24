""" Failover Transfer

    The failover transfer client exposes the following methods:
    - transferAndRegisterFile()
    - transferAndRegisterFileFailover()
    - getRequestObject()

    Initially these methods were developed inside workflow modules but
    have evolved to a generic 'transfer file with failover' client.

    The transferAndRegisterFile() method will correctly set registration
    requests in case of failure.

    The transferAndRegisterFileFailover() method will attempt to upload
    a file to a list of alternative SEs and set appropriate replication
    to the original target SE as well as the removal request for the
    temporary replica.

    getRequestObject() allows to retrieve the modified request object
    after transfer operations.
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger


from DIRAC.DataManagementSystem.Client.DataManager          import DataManager
from DIRAC.Resources.Storage.StorageElement                 import StorageElement
from DIRAC.RequestManagementSystem.Client.Request           import Request
from DIRAC.RequestManagementSystem.Client.Operation         import Operation
from DIRAC.RequestManagementSystem.Client.File              import File
from DIRAC.RequestManagementSystem.private.RequestValidator import gRequestValidator
from DIRAC.RequestManagementSystem.Client.ReqClient         import ReqClient


class FailoverTransfer( object ):
  """ .. class:: FailoverTransfer

  """

  #############################################################################
  def __init__( self, requestObject = None, log = None, defaultChecksumType = 'ADLER32' ):
    """ Constructor function, can specify request object to instantiate
        FailoverTransfer or a new request object is created.
    """
    self.log = log
    if not self.log:
      self.log = gLogger.getSubLogger( "FailoverTransfer" )
    
    self.request = requestObject
    if not self.request:
      self.request = Request()
      self.request.RequestName = 'noname_request'
      self.request.SourceComponent = 'FailoverTransfer'

    self.defaultChecksumType = defaultChecksumType

  #############################################################################
  def transferAndRegisterFile( self,
                               fileName,
                               localPath,
                               lfn,
                               destinationSEList,
                               fileMetaDict,
                               fileCatalog = None ):
    """Performs the transfer and register operation with failover.
    """
    errorList = []
    fileGUID = fileMetaDict.get( "GUID", None )

    for se in destinationSEList:
      self.log.info( "Attempting rm.putAndRegister('%s','%s','%s',guid='%s',catalog='%s')" % ( lfn,
                                                                                               localPath,
                                                                                               se,
                                                                                               fileGUID,
                                                                                               fileCatalog ) )

      result = DataManager( catalogs = fileCatalog ).putAndRegister( lfn, localPath, se, guid = fileGUID )
      self.log.verbose( result )
      if not result['OK']:
        self.log.error( 'rm.putAndRegister failed with message', result['Message'] )
        errorList.append( result['Message'] )
        continue

      if not result['Value']['Failed']:
        self.log.info( 'rm.putAndRegister successfully uploaded and registered %s to %s' % ( fileName, se ) )
        return S_OK( {'uploadedSE':se, 'lfn':lfn} )

      # Now we know something went wrong
      self.log.warn( "Didn't manage to do everything, now adding requests for the missing operation" )

      errorDict = result['Value']['Failed'][lfn]
      if 'register' not in errorDict:
        self.log.error( 'rm.putAndRegister failed with unknown error', str( errorDict ) )
        errorList.append( 'Unknown error while attempting upload to %s' % se )
        continue

      # fileDict = errorDict['register']
      # Therefore the registration failed but the upload was successful
      if not fileCatalog:
        fileCatalog = ''

      result = self._setRegistrationRequest( lfn, se, fileMetaDict, fileCatalog )
      if not result['OK']:
        self.log.error( 'Failed to set registration request for: SE %s and metadata: \n%s' % ( se, fileMetaDict ) )
        errorList.append( 'Failed to set registration request for: SE %s and metadata: \n%s' % ( se, fileMetaDict ) )
        continue
      else:
        self.log.info( 'Successfully set registration request for: SE %s and metadata: \n%s' % ( se, fileMetaDict ) )
        metadata = {}
        metadata['filedict'] = fileMetaDict
        metadata['uploadedSE'] = se
        metadata['lfn'] = lfn
        metadata['registration'] = 'request'
        return S_OK( metadata )

    self.log.error( 'Encountered %s errors during attempts to upload output data' % len( errorList ) )
    return S_ERROR( 'Failed to upload output data file' )

  #############################################################################
  def transferAndRegisterFileFailover( self,
                                       fileName,
                                       localPath,
                                       lfn,
                                       targetSE,
                                       failoverSEList,
                                       fileMetaDict,
                                       fileCatalog = None ):
    """Performs the transfer and register operation to failover storage and sets the
       necessary replication and removal requests to recover.
    """
    failover = self.transferAndRegisterFile( fileName, localPath, lfn, failoverSEList, fileMetaDict, fileCatalog )
    if not failover['OK']:
      self.log.error( 'Could not upload file to failover SEs', failover['Message'] )
      return failover

    # set removal requests and replication requests
    result = self._setFileReplicationRequest( lfn, targetSE, fileMetaDict, sourceSE = failover['Value']['uploadedSE'] )
    if not result['OK']:
      self.log.error( 'Could not set file replication request', result['Message'] )
      return result

    lfn = failover['Value']['lfn']
    failoverSE = failover['Value']['uploadedSE']
    self.log.info( 'Attempting to set replica removal request for LFN %s at failover SE %s' % ( lfn, failoverSE ) )
    result = self._setReplicaRemovalRequest( lfn, failoverSE )
    if not result['OK']:
      self.log.error( 'Could not set removal request', result['Message'] )
      return result

    return S_OK( {'uploadedSE':failoverSE, 'lfn':lfn} )

  def getRequest( self ):
    """ get the accumulated request object
    """
    return self.request

  def commitRequest( self ):
    """ Send request to the Request Management Service
    """
    if self.request.isEmpty():
      return S_OK()

    isValid = gRequestValidator.validate( self.request )
    if not isValid["OK"]:
      return S_ERROR( "Failover request is not valid: %s" % isValid["Message"] )
    else:
      requestClient = ReqClient()
      result = requestClient.putRequest( self.request )
      return result

  #############################################################################
  def _setFileReplicationRequest( self, lfn, targetSE, fileMetaDict, sourceSE = '' ):
    """ Sets a registration request.
    """
    self.log.info( 'Setting replication request for %s to %s' % ( lfn, targetSE ) )

    transfer = Operation()
    transfer.Type = "ReplicateAndRegister"
    transfer.TargetSE = targetSE
    if sourceSE:
      transfer.SourceSE = sourceSE

    trFile = File()
    trFile.LFN = lfn

    cksm = fileMetaDict.get( "Checksum", None )
    cksmType = fileMetaDict.get( "ChecksumType", self.defaultChecksumType )
    if cksm and cksmType:
      trFile.Checksum = cksm
      trFile.ChecksumType = cksmType
    size = fileMetaDict.get( "Size", 0 )
    if size:
      trFile.Size = size
    guid = fileMetaDict.get( "GUID", "" )
    if guid:
      trFile.GUID = guid

    transfer.addFile( trFile )

    self.request.addOperation( transfer )

    return S_OK()

  #############################################################################
  def _setRegistrationRequest( self, lfn, targetSE, fileDict, catalog ):
    """ Sets a registration request

    :param str lfn: LFN
    :param list se: list of SE (or just string)
    :param list catalog: list (or string) of catalogs to use
    :param dict fileDict: file metadata
    """
    self.log.info( 'Setting registration request for %s at %s.' % ( lfn, targetSE ) )

    if not type( catalog ) == type( [] ):
      catalog = [catalog]

    for cat in catalog:

      register = Operation()
      register.Type = "RegisterFile"
      register.Catalog = cat
      register.TargetSE = targetSE

      regFile = File()
      regFile.LFN = lfn
      regFile.Checksum = fileDict.get( "Checksum", "" )
      regFile.ChecksumType = fileDict.get( "ChecksumType", self.defaultChecksumType )
      regFile.Size = fileDict.get( "Size", 0 )
      regFile.GUID = fileDict.get( "GUID", "" )

      se = StorageElement( targetSE )
      pfn = se.getPfnForLfn( lfn )
      if not pfn["OK"] or lfn not in pfn["Value"]['Successful']:
        self.log.error( "unable to get PFN for LFN: %s" % pfn.get( 'Message', pfn.get( 'Value', {} ).get( 'Failed', {} ).get( lfn ) ) )
        return pfn
      regFile.PFN = pfn["Value"]['Successful'][lfn]

      register.addFile( regFile )
      self.request.addOperation( register )

    return S_OK()

  #############################################################################
  def _setReplicaRemovalRequest( self, lfn, se ):
    """ Sets a removal request for a replica.

    :param str lfn: LFN
    :param se:
    """
    if type( se ) == str:
      se = ",".join( [ se.strip() for se in se.split( "," ) if se.strip() ] )

    removeReplica = Operation()

    removeReplica.Type = "RemoveReplica"
    removeReplica.TargetSE = se

    replicaToRemove = File()
    replicaToRemove.LFN = lfn

    removeReplica.addFile( replicaToRemove )

    self.request.addOperation( removeReplica )
    return S_OK()

  #############################################################################
  def _setFileRemovalRequest( self, lfn, se = '', pfn = '' ):
    """ Sets a removal request for a file including all replicas.
    """
    remove = Operation()
    remove.Type = "RemoveFile"
    if se:
      remove.TargetSE = se
    rmFile = File()
    rmFile.LFN = lfn
    if pfn:
      rmFile.PFN = pfn
    remove.addFile( rmFile )
    self.request.addOperation( remove )
    return S_OK()
