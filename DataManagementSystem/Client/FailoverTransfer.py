########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/Client/FailoverTransfer $
########################################################################

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

    WHAT IT IS? CONFUSING ABANDONWARE, OBSOLETE
    K.C.

"""

__RCSID__ = "$Id: FailoverTransfer 18161 2009-11-11 12:07:09Z acasajus $"

from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

from DIRAC import S_OK, S_ERROR, gLogger

import types

class FailoverTransfer:

  #############################################################################
  def __init__( self, requestObject = False ):
    """ Constructor function, can specify request object to instantiate 
        FailoverTransfer or a new request object is created.
    """
    self.log = gLogger.getSubLogger( "FailoverTransfer" )
    self.rm = ReplicaManager()
    self.request = requestObject

    if not self.request:
      self.request = RequestContainer()
      self.request.setRequestName( 'default_request.xml' )
      self.request.setSourceComponent( 'FailoverTransfer' )

  #############################################################################
  def transferAndRegisterFile( self, fileName, localPath, lfn, destinationSEList, fileGUID = None, fileCatalog = None ):
    """Performs the transfer and register operation with failover.
    """
    errorList = []
    for se in destinationSEList:
      self.log.info( 'Attempting rm.putAndRegister("%s","%s","%s",guid="%s",catalog="%s")' % ( lfn, localPath, se, fileGUID, fileCatalog ) )
      result = self.rm.putAndRegister( lfn, localPath, se, guid = fileGUID, catalog = fileCatalog )
      self.log.verbose( result )
      if not result['OK']:
        self.log.error( 'rm.putAndRegister failed with message', result['Message'] )
        errorList.append( result['Message'] )
        continue

      if not result['Value']['Failed']:
        self.log.info( 'rm.putAndRegister successfully uploaded %s to %s' % ( fileName, se ) )
        return S_OK( {'uploadedSE':se, 'lfn':lfn} )

      #Now we know something went wrong
      errorDict = result['Value']['Failed'][lfn]
      if not errorDict.has_key( 'register' ):
        self.log.error( 'rm.putAndRegister failed with unknown error', str( errorDict ) )
        errorList.append( 'Unknown error while attempting upload to %s' % se )
        continue

      fileDict = errorDict['register']
      #Therefore the registration failed but the upload was successful
      if not fileCatalog:
        fileCatalog = ''

      result = self.__setRegistrationRequest( fileDict['LFN'], se, fileCatalog, fileDict )
      if not result['OK']:
        self.log.error( 'Failed to set registration request for: SE %s and metadata: \n%s' % ( se, fileDict ) )
        errorList.append( 'Failed to set registration request for: SE %s and metadata: \n%s' % ( se, fileDict ) )
        continue
      else:
        self.log.info( 'Successfully set registration request for: SE %s and metadata: \n%s' % ( se, fileDict ) )
        metadata = {}
        metadata['filedict'] = fileDict
        metadata['uploadedSE'] = se
        metadata['lfn'] = lfn
        metadata['registration'] = 'request'
        return S_OK( metadata )

    self.log.error( 'Encountered %s errors during attempts to upload output data' % len( errorList ) )
    return S_ERROR( 'Failed to upload output data file' )

  #############################################################################
  def transferAndRegisterFileFailover( self, fileName, localPath, lfn, targetSE, failoverSEList, fileGUID = None, fileCatalog = None ):
    """Performs the transfer and register operation to failover storage and sets the
       necessary replication and removal requests to recover.
    """
    failover = self.transferAndRegisterFile( fileName, localPath, lfn, failoverSEList, fileGUID, fileCatalog )
    if not failover['OK']:
      self.log.error( 'Could not upload file to failover SEs', failover['Message'] )
      return failover

    #set removal requests and replication requests
    result = self.__setFileReplicationRequest( lfn, targetSE )
    if not result['OK']:
      self.log.error( 'Could not set file replication request', result['Message'] )
      return result

    lfn = failover['Value']['lfn']
    failoverSE = failover['Value']['uploadedSE']
    self.log.info( 'Attempting to set replica removal request for LFN %s at failover SE %s' % ( lfn, failoverSE ) )
    result = self.__setReplicaRemovalRequest( lfn, failoverSE )
    if not result['OK']:
      self.log.error( 'Could not set removal request', result['Message'] )
      return result

    return S_OK( '%s uploaded to a failover SE' % fileName )

  #############################################################################
  def getRequestObject( self ):
    """Returns the potentially modified request object in order to propagate changes.
    """
    return S_OK( self.request )

  #############################################################################
  def __setFileReplicationRequest( self, lfn, se ):
    """ Sets a registration request.
    """
    self.log.info( 'Setting replication request for %s to %s' % ( lfn, se ) )
    lastOperationOnFile = self.request._getLastOrder( lfn )
    result = self.request.addSubRequest( {'Attributes':{'Operation':'replicateAndRegister',
                                                       'TargetSE':se, 'ExecutionOrder':lastOperationOnFile + 1}},
                                         'transfer' )
    if not result['OK']:
      return result

    index = result['Value']
    fileDict = {'LFN':lfn, 'Status':'Waiting'}
    self.request.setSubRequestFiles( index, 'transfer', [fileDict] )

    return S_OK()

  #############################################################################
  def __setRegistrationRequest( self, lfn, se, catalog, fileDict ):
    """ Sets a registration request.
    """
    self.log.info( 'Setting registration request for %s at %s.' % ( lfn, se ) )
    if not type( catalog ) == types.ListType:
      catalog = [catalog]

    lastOperationOnFile = self.request._getLastOrder( lfn )
    i_catalog = lastOperationOnFile + 1
    for cat in catalog:
      result = self.request.addSubRequest( {'Attributes':{'Operation':'registerFile', 'ExecutionOrder':i_catalog,
                                                         'TargetSE':se, 'Catalogue':cat}}, 'register' )
      if not result['OK']:
        return result

      i_catalog += 1

      index = result['Value']
      if not fileDict.has_key( 'Status' ):
        fileDict['Status'] = 'Waiting'
      self.request.setSubRequestFiles( index, 'register', [fileDict] )

    return S_OK()

  #############################################################################
  def __setReplicaRemovalRequest( self, lfn, se ):
    """ Sets a removal request for a replica.
    """
    lastOperationOnFile = self.request._getLastOrder( lfn )
    result = self.request.addSubRequest( {'Attributes':{'Operation':'replicaRemoval',
                                                       'TargetSE':se, 'ExecutionOrder':lastOperationOnFile + 1}},
                                         'removal' )
    index = result['Value']
    fileDict = {'LFN':lfn, 'Status':'Waiting'}
    self.request.setSubRequestFiles( index, 'removal', [fileDict] )

    return S_OK()

  #############################################################################
  def __setFileRemovalRequest( self, lfn, se = '', pfn = '' ):
    """ Sets a removal request for a file including all replicas.
    """
    lastOperationOnFile = self.request._getLastOrder( lfn )
    result = self.request.addSubRequest( {'Attributes':{'Operation':'removeFile',
                                                       'TargetSE':se, 'ExecutionOrder':lastOperationOnFile + 1}},
                                         'removal' )
    index = result['Value']
    fileDict = {'LFN':lfn, 'PFN':pfn, 'Status':'Waiting'}
    self.request.setSubRequestFiles( index, 'removal', [fileDict] )

    return S_OK()
