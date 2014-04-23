########################################################################
# $Id$
########################################################################
""" SandboxHandler is the implementation of the Sandbox service
    in the DISET framework
"""
__RCSID__ = "$Id$"

import os
import time
import random
import types
import threading
import tempfile
from DIRAC import gLogger, S_OK, S_ERROR  # , gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.WorkloadManagementSystem.DB.SandboxMetadataDB import SandboxMetadataDB
from DIRAC.DataManagementSystem.Client.DataManager  import DataManager
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Utilities import Utils
from DIRAC.Core.Security import Properties
# from DIRAC.Core.Utilities import List

sandboxDB = False

def initializeSandboxStoreHandler( serviceInfo ):
  global sandboxDB, gSBDeletionPool
  random.seed()
  sandboxDB = SandboxMetadataDB()
  print sandboxDB
  return S_OK()

class SandboxStoreHandler( RequestHandler ):

  __purgeCount = -1
  __purgeLock = threading.Lock()
  __purgeWorking = False

  def initialize( self ):
    self.__backend = self.getCSOption( "Backend", "local" )
    self.__localSEName = self.getCSOption( "LocalSE", "SandboxSE" )
    self.__maxUploadBytes = self.getCSOption( "MaxSandboxSizeMiB", 10 ) * 1048576
    if self.__backend.lower() == "local" or self.__backend == self.__localSEName:
      self.__useLocalStorage = True
      self.__seNameToUse = self.__localSEName
    else:
      self.__useLocalStorage = False
      self.__externalSEName = self.__backend
      self.__seNameToUse = self.__backend
    # Execute the purge once every 100 calls
    SandboxStoreHandler.__purgeCount += 1
    if SandboxStoreHandler.__purgeCount > self.getCSOption( "QueriesBeforePurge", 1000 ):
      SandboxStoreHandler.__purgeCount = 0
    if SandboxStoreHandler.__purgeCount == 0:
      threading.Thread( target = self.purgeUnusedSandboxes ).start()

  def __getSandboxPath( self, md5 ):
    """ Generate the sandbox path
    """
    prefix = self.getCSOption( "SandboxPrefix", "SandBox" )
    credDict = self.getRemoteCredentials()
    if Properties.JOB_SHARING in credDict[ 'properties' ]:
      idField = credDict[ 'group' ]
    else:
      idField = "%s.%s" % ( credDict[ 'username' ], credDict[ 'group' ] )
    pathItems = [ "/", prefix, idField[0], idField ]
    pathItems.extend( [ md5[0:3], md5[3:6], md5 ] )
    return os.path.join( *pathItems )


  def transfer_fromClient( self, fileId, token, fileSize, fileHelper ):
    """
    Receive a file as a sandbox
    """

    if self.__maxUploadBytes and fileSize > self.__maxUploadBytes:
      fileHelper.markAsTransferred()
      return S_ERROR( "Sandbox is too big. Please upload it to a grid storage element" )

    if type( fileId ) in ( types.ListType, types.TupleType ):
      if len( fileId ) > 1:
        assignTo = fileId[1]
        fileId = fileId[0]
      else:
        return S_ERROR( "File identified tuple has to have length greater than 1" )
    else:
      assignTo = {}

    extPos = fileId.find( ".tar" )
    if extPos > -1:
      extension = fileId[ extPos + 1: ]
      aHash = fileId[ :extPos ]
    else:
      extension = ""
      aHash = fileId
    gLogger.info( "Upload requested for %s [%s]" % ( aHash, extension ) )

    credDict = self.getRemoteCredentials()
    sbPath = self.__getSandboxPath( "%s.%s" % ( aHash, extension ) )
    # Generate the location
    result = self.__generateLocation( sbPath )
    if not result[ 'OK' ]:
      return result
    seName, sePFN = result[ 'Value' ]

    result = sandboxDB.getSandboxId( seName, sePFN, credDict[ 'username' ], credDict[ 'group' ] )
    if result[ 'OK' ]:
      gLogger.info( "Sandbox already exists. Skipping upload" )
      fileHelper.markAsTransferred()
      sbURL = "SB:%s|%s" % ( seName, sePFN )
      assignTo = dict( [ ( key, [ ( sbURL, assignTo[ key ] ) ] ) for key in assignTo ] )
      result = self.export_assignSandboxesToEntities( assignTo )
      if not result[ 'OK' ]:
        return result
      return S_OK( sbURL )

    if self.__useLocalStorage:
      hdPath = self.__sbToHDPath( sbPath )
    else:
      hdPath = False
    # Write to local file
    result = self.__networkToFile( fileHelper, hdPath )
    if not result[ 'OK' ]:
      gLogger.error( "Error while receiving file: %s" % result['Message'] )
      return result
    hdPath = result[ 'Value' ]
    gLogger.info( "Wrote sandbox to file %s" % hdPath )
    # Check hash!
    if fileHelper.getHash() != aHash:
      self.__secureUnlinkFile( hdPath )
      gLogger.error( "Hashes don't match! Client defined hash is different with received data hash!" )
      return S_ERROR( "Hashes don't match!" )
    # If using remote storage, copy there!
    if not self.__useLocalStorage:
      gLogger.info( "Uploading sandbox to external storage" )
      result = self.__copyToExternalSE( hdPath, sbPath )
      self.__secureUnlinkFile( hdPath )
      if not result[ 'OK' ]:
        return result
      sbPath = result[ 'Value' ][1]
    # Register!
    gLogger.info( "Registering sandbox in the DB with", "SB:%s|%s" % ( self.__seNameToUse, sbPath ) )
    result = sandboxDB.registerAndGetSandbox( credDict[ 'username' ], credDict[ 'DN' ], credDict[ 'group' ],
                                              self.__seNameToUse, sbPath, fileHelper.getTransferedBytes() )
    if not result[ 'OK' ]:
      self.__secureUnlinkFile( hdPath )
      return result

    sbURL = "SB:%s|%s" % ( self.__seNameToUse, sbPath )
    assignTo = dict( [ ( key, [ ( sbURL, assignTo[ key ] ) ] ) for key in assignTo ] )
    result = self.export_assignSandboxesToEntities( assignTo )
    if not result[ 'OK' ]:
      return result
    return S_OK( sbURL )

  def transfer_bulkFromClient( self, fileId, token, fileSize, fileHelper ):
    """ Receive files packed into a tar archive by the fileHelper logic.
        token is used for access rights confirmation.
    """
    result = self.__networkToFile( fileHelper )
    if not result[ 'OK' ]:
      return result
    tmpFilePath = result[ 'OK' ]
    gLogger.info( "Got Sandbox to local storage", tmpFilePath )

    extension = fileId[ fileId.find( ".tar" ) + 1: ]
    sbPath = "%s.%s" % ( self.__getSandboxPath( fileHelper.getHash() ), extension )
    gLogger.info( "Sandbox path will be", sbPath )
    # Generate the location
    result = self.__generateLocation( sbPath )
    if not result[ 'OK' ]:
      return result
    seName, sePFN = result[ 'Value' ]
    # Register in DB
    credDict = self.getRemoteCredentials()
    result = sandboxDB.getSandboxId( seName, sePFN, credDict[ 'username' ], credDict[ 'group' ] )
    if result[ 'OK' ]:
      return S_OK( "SB:%s|%s" % ( seName, sePFN ) )

    result = sandboxDB.registerAndGetSandbox( credDict[ 'username' ], credDict[ 'DN' ], credDict[ 'group' ],
                                              seName, sePFN, fileHelper.getTransferedBytes() )
    if not result[ 'OK' ]:
      self.__secureUnlinkFile( tmpFilePath )
      return result
    sbid, newSandbox = result[ 'Value' ]
    gLogger.info( "Registered in DB", "with SBId %s" % sbid )

    result = self.__moveToFinalLocation( tmpFilePath, sbPath )
    self.__secureUnlinkFile( tmpFilePath )
    if not result[ 'OK' ]:
      gLogger.error( "Could not move sandbox to final destination", result[ 'Message' ] )
      return result

    gLogger.info( "Moved to final destination" )

    # Unlink temporal file if it's there
    self.__secureUnlinkFile( tmpFilePath )
    return S_OK( "SB:%s|%s" % ( seName, sePFN ) )

  def __generateLocation( self, sbPath ):
    """
    Generate the location string
    """
    if self.__useLocalStorage:
      return S_OK( ( self.__localSEName, sbPath ) )
    # It's external storage
    storageElement = StorageElement( self.__externalSEName )
    res = storageElement.isValid()
    if not res['OK']:
      errStr = "Failed to instantiate destination StorageElement"
      gLogger.error( errStr, self.__externalSEName )
      return S_ERROR( errStr )
    result = storageElement.getPfnForLfn( sbPath )
    if not result['OK'] or sbPath not in result['Value']['Successful']:
      errStr = "Failed to generate PFN"
      gLogger.error( errStr, self.__externalSEName )
      return S_ERROR( errStr )
    destPfn = result['Value']['Successful'][sbPath]
    return S_OK( ( self.__externalSEName, destPfn ) )

  def __sbToHDPath( self, sbPath ):
    while sbPath and sbPath[0] == "/":
      sbPath = sbPath[1:]
    basePath = self.getCSOption( "BasePath", "/opt/dirac/storage/sandboxes" )
    return os.path.join( basePath, sbPath )

  def __networkToFile( self, fileHelper, destFileName = False ):
    """
    Dump incoming network data to temporal file
    """
    tfd = False
    if not destFileName:
      try:
        tfd, destFileName = tempfile.mkstemp( prefix = "DSB." )
      except Exception, e:
        return S_ERROR( "Cannot create temporal file: %s" % str( e ) )
    destFileName = os.path.realpath( destFileName )
    try:
      os.makedirs( os.path.dirname( destFileName ) )
    except:
      pass
    try:
      fd = open( destFileName, "wb" )
    except Exception, e:
      return S_ERROR( "Cannot open to write destination file %s" % destFileName )
    result = fileHelper.networkToDataSink( fd, maxFileSize = self.__maxUploadBytes )
    if not result[ 'OK' ]:
      return result
    if tfd:
      os.close( tfd )
    fd.close()
    return S_OK( destFileName )

  def __secureUnlinkFile( self, filePath ):
    try:
      os.unlink( filePath )
    except Exception, e:
      gLogger.warn( " Could not unlink file %s: %s" % ( filePath, str( e ) ) )
      return False
    return True

  def __moveToFinalLocation( self, localFilePath, sbPath ):
    if self.__useLocalStorage:
      hdFilePath = self.__sbToHDPath( sbPath )
      result = S_OK( ( self.__localSEName, sbPath ) )
      if os.path.isfile( hdFilePath ):
        gLogger.info( "There was already a sandbox with that name, skipping copy", sbPath )
      else:
        hdDirPath = os.path.dirname( hdFilePath )
        if not os.path.isdir( hdDirPath ):
          try:
            os.makedirs( hdDirPath )
          except:
            pass
        try:
          os.rename( localFilePath, hdFilePath )
        except Exception, e:
          errMsg = "Cannot move temporal file to final path"
          gLogger.error( errMsg, str( e ) )
          result = S_ERROR( "%s: %s" % ( errMsg, str( e ) ) )
    else:
      result = self.__copyToExternalSE( localFilePath, sbPath )

    return result

  def __copyToExternalSE( self, localFilePath, sbPath ):
    """
    Copy uploaded file to external SE
    """
    try:
      dm = DataManager()
      result = dm.put( sbPath, localFilePath, self.__externalSEName )
      if not result[ 'OK' ]:
        return result
      if 'Successful' not in result[ 'Value' ]:
        gLogger.verbose( "Oops, no successful transfers there", str( result ) )
        return S_ERROR( "RM returned OK to the action but no successful transfers were there" )
      okTrans = result[ 'Value' ][ 'Successful' ]
      if sbPath not in okTrans:
        gLogger.verbose( "Ooops, SB transfer wasn't in the successful ones", str( result ) )
        return S_ERROR( "RM returned OK to the action but SB transfer wasn't in the successful ones" )
      return S_OK( ( self.__externalSEName, okTrans[ sbPath ] ) )
    except Exception, e:
      return S_ERROR( "Error while moving sandbox to SE: %s" % str( e ) )

  ##################
  # Assigning sbs to jobs

  types_assignSandboxesToEntities = [ types.DictType ]
  def export_assignSandboxesToEntities( self, enDict, ownerName = "", ownerGroup = "", entitySetup = False ):
    """
    Assign sandboxes to jobs.
    Expects a dict of { entityId : [ ( SB, SBType ), ... ] }
    """
    if not entitySetup:
      entitySetup = self.serviceInfoDict[ 'clientSetup' ]
    credDict = self.getRemoteCredentials()
    return sandboxDB.assignSandboxesToEntities( enDict, credDict[ 'username' ], credDict[ 'group' ], entitySetup,
                                                ownerName, ownerGroup )

  ##################
  # Unassign sbs to jobs

  types_unassignEntities = [ ( types.ListType, types.TupleType ) ]
  def export_unassignEntities( self, entitiesList, entitiesSetup = False ):
    """
    Unassign a list of jobs
    """
    if not entitiesSetup:
      entitiesSetup = self.serviceInfoDict[ 'clientSetup' ]
    credDict = self.getRemoteCredentials()
    return sandboxDB.unassignEntities( { entitiesSetup : entitiesList }, credDict[ 'username' ], credDict[ 'group' ] )

  ##################
  # Getting assigned sandboxes

  types_getSandboxesAssignedToEntity = [ types.StringType ]
  def export_getSandboxesAssignedToEntity( self, entityId, entitySetup = False ):
    """
    Get the sandboxes associated to a job and the association type
    """
    if not entitySetup:
      entitySetup = self.serviceInfoDict[ 'clientSetup' ]
    credDict = self.getRemoteCredentials()
    result = sandboxDB.getSandboxesAssignedToEntity( entityId, entitySetup,
                                                     credDict[ 'username' ], credDict[ 'group' ] )
    if not result[ 'OK' ]:
      return result
    sbDict = {}
    for SEName, SEPFN, SBType in result[ 'Value' ]:
      if SBType not in sbDict:
        sbDict[ SBType ] = []
      sbDict[ SBType ].append( "SB:%s|%s" % ( SEName, SEPFN ) )
    return S_OK( sbDict )


  ##################
  # Download sandboxes

  def transfer_toClient( self, fileID, token, fileHelper ):
    """ Method to send files to clients.
        fileID is the local file name in the SE.
        token is used for access rights confirmation.
    """
    credDict = self.getRemoteCredentials()
    result = sandboxDB.getSandboxId( self.__localSEName, fileID, credDict[ 'username' ], credDict[ 'group' ] )
    if not result[ 'OK' ]:
      return result
    sbId = result[ 'Value' ]
    sandboxDB.accessedSandboxById( sbId )
    # If it's a local file
    hdPath = self.__sbToHDPath( fileID )
    if not os.path.isfile( hdPath ):
      return S_ERROR( "Sandbox does not exist" )
    result = fileHelper.getFileDescriptor( hdPath, 'rb' )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get file descriptor: %s' % result[ 'Message' ] )
    fd = result[ 'Value' ]
    result = fileHelper.FDToNetwork( fd )
    fileHelper.oFile.close()
    return result

  ##################
  # Purge sandboxes

  def purgeUnusedSandboxes( self ):
    # If a purge is already working skip
    SandboxStoreHandler.__purgeLock.acquire()
    try:
      if SandboxStoreHandler.__purgeWorking:
        if time.time() - SandboxStoreHandler.__purgeWorking < 86400:
          gLogger.info( "Sandbox purge still working" )
          return S_OK()
      SandboxStoreHandler.__purgeWorking = time.time()
    finally:
      SandboxStoreHandler.__purgeLock.release()

    gLogger.info( "Purging sandboxes" )
    result = sandboxDB.getUnusedSandboxes()
    if not result[ 'OK' ]:
      gLogger.error( "Error while retrieving sandboxes to purge", result[ 'Message' ] )
      SandboxStoreHandler.__purgeWorking = False
      return result
    sbList = result[ 'Value' ]
    gLogger.info( "Got %s sandboxes to purge" % len( sbList ) )
    deletedFromSE = []
    for sbId, SEName, SEPFN in sbList:
      self.__purgeSandbox( sbId, SEName, SEPFN )

    SandboxStoreHandler.__purgeWorking = False
    return S_OK()

  def __purgeSandbox( self, sbId, SEName, SEPFN ):
    result = self.__deleteSandboxFromBackend( SEName, SEPFN )
    if not result[ 'OK' ]:
      gLogger.error( "Cannot delete sandbox from backend", result[ 'Message' ] )
      return
    result = sandboxDB.deleteSandboxes( [ sbId ] )
    if not result[ 'OK' ]:
      gLogger.error( "Cannot delete sandbox from DB", result[ 'Message' ] )

  def __deleteSandboxFromBackend( self, SEName, SEPFN ):
    gLogger.info( "Purging sandbox" "SB:%s|%s" % ( SEName, SEPFN ) )
    if SEName != self.__localSEName:
      return self.__deleteSandboxFromExternalBackend( SEName, SEPFN )
    else:
      hdPath = self.__sbToHDPath( SEPFN )
      if not os.path.isfile( hdPath ):
        return S_OK()
      try:
        os.unlink( hdPath )
      except Exception, e:
        gLogger.error( "Cannot delete local sandbox", "%s : %s" % ( hdPath, str( e ) ) )
      while hdPath:
        hdPath = os.path.dirname( hdPath )
        gLogger.info( "Checking if dir %s is empty" % hdPath )
        if not os.path.isdir( hdPath ):
          break
        if len( os.listdir( hdPath ) ) > 0:
          break
        gLogger.info( "Trying to clean dir %s" % hdPath )
        # Empty dir!
        try:
          os.rmdir( hdPath )
        except Exception, e:
          gLogger.error( "Cannot clean empty directory", "%s : %s" % ( hdPath, str( e ) ) )
          break
    return S_OK()

  def __deleteSandboxFromExternalBackend( self, SEName, SEPFN ):
    if self.getCSOption( "DelayedExternalDeletion", True ):
      gLogger.info( "Setting deletion request" )
      try:

        request = Request()
        request.RequestName = "RemoteSBDeletion:%s|%s:%s" % ( SEName, SEPFN, time.time() )
        physicalRemoval = Operation()
        physicalRemoval.Type = "PhysicalRemoval"
        physicalRemoval.TargetSE = SEName
        fileToRemove = File()
        fileToRemove.PFN = SEPFN
        physicalRemoval.addFile( fileToRemove )
        request.addOperation( physicalRemoval )
        return ReqClient().putRequest( request )
      except Exception, e:
        gLogger.exception( "Exception while setting deletion request" )
        return S_ERROR( "Cannot set deletion request: %s" % str( e ) )
    else:
      gLogger.info( "Deleting external Sandbox" )
      try:
        return StorageElement( SEName ).removeFile( SEPFN )
      except Exception, e:
        gLogger.exception( "RM raised an exception while trying to delete a remote sandbox" )
        return S_ERROR( "RM raised an exception while trying to delete a remote sandbox" )
