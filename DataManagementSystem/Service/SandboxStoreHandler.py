########################################################################
# $Id: SandboxStoreHandler.py,v 1.1 2009/04/28 16:25:09 acasajus Exp $
########################################################################

""" SandboxHandler is the implementation of the Sandbox service
    in the DISET framework

"""

__RCSID__ = "$Id: SandboxStoreHandler.py,v 1.1 2009/04/28 16:25:09 acasajus Exp $"

from types import *
import os
import md5
import time
import random
import types
import tempfile
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.DataManagementSystem.DB.SandboxMetadataDB import SandboxMetadataDB
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ThreadPool import ThreadPool

sandboxDB = False
gSBDeletionPool = False

def initializeSandboxStoreHandler( serviceInfo ):
  global sandboxDB, gSBDeletionPool
  random.seed()
  gSBDeletionPool = ThreadPool( 1, 1 )
  gSBDeletionPool.daemonize()
  sandboxDB = SandboxMetadataDB()
  return S_OK()

class SandboxStoreHandler( RequestHandler ):

  __purgeCount = -1

  def initialize( self ):
    self.__backend = self.getCSOption( "Backend", "local" )
    self.__localSEName = self.getCSOption( "LocalSE", "SandboxSE" )
    if self.__backend.lower() == "local" or self.__backend == self.__localSEName:
      self.__useLocalStorage = True
      self.__seNameToUse = self.__localSEName
    else:
      self.__useLocalStorage = False
      self.__externalSEName = self.__backend
      self.__seNameToUse = self.__backend
    #Execute the purge once every 100 calls
    SandboxStoreHandler.__purgeCount += 1
    if SandboxStoreHandler.__purgeCount > 100:
      SandboxStoreHandler.__purgeCount = 0
    if SandboxStoreHandler.__purgeCount == 0:
      self.purgeUnusedSandboxes()

  def __getSandboxPath( self, md5 ):
    """ Generate the sandbox path
    """
    prefix = self.getCSOption( "SandboxPrefix", "SandBox" )
    credDict = self.getRemoteCredentials()
    if Properties.JOB_SHARING in credDict[ 'properties' ]:
      idField = credDict[ 'group' ]
    else:
      idField = credDict[ 'username' ]
    pathItems = [ "/", prefix, idField[0], idField ]
    pathItems.extend( [ md5[0:3], md5[3:6], md5 ] )
    return os.path.join( *pathItems )


  def transfer_fromClient( self, fileId, token, fileSize, fileHelper ):
    """
    Receive a file as a sandbox
    """
    extPos = fileId.find( ".tar" )
    if extPos > -1:
      extension = fileId[ extPos + 1: ]
      hash = fileId[ :extPos ]
    else:
      extension = ""
      hash = fileId
    gLogger.info( "Upload requested for %s [%s]" % ( hash, extension ) )

    credDict = self.getRemoteCredentials()
    sbPath = self.__getSandboxPath( "%s.%s" % ( hash, extension ) )
    #Generate the location
    result = self.__generateLocation( sbPath )
    if not result[ 'OK' ]:
      return result
    seName, sePFN = result[ 'Value' ]

    result = sandboxDB.getSandboxId( seName, sePFN, credDict[ 'username' ], credDict[ 'group' ] )
    if result[ 'OK' ]:
      gLogger.info( "Sandbox already exists. Skipping upload" )
      fileHelper.markAsTransferred()
      return S_OK( "%s:%s" % ( seName, sePFN ) )

    if self.__useLocalStorage:
      hdPath = self.__sbToHDPath( sbPath )
    else:
      hdPath = False
    #Write to local file
    result = self.__networkToFile( fileHelper, hdPath )
    if not result[ 'OK' ]:
      gLogger.error( "Error while receiving file: %s" % result['Message'] )
      return result
    hdPath = result[ 'Value' ]
    gLogger.info( "Wrote sandbox to file %s" % hdPath )
    #Check hash!
    if fileHelper.getHash() != hash:
      self.__secureUnlinkFile( hdPath )
      gLogger.error( "Hashes don't match! Client defined hash is different with received data hash!" )
      return S_ERROR( "Hashes don't match!" )
    #If using remote storage, copy there!
    if not self.__useLocalStorage:
      gLogger.info( "Uploading sandbox to external storage" )
      result = self.__copyToExternalSE( hdPath, sbPath )
      self.__secureUnlinkFile( hdPath )
      if not result[ 'OK' ]:
        return result
      sbPath = result[ 'Value' ][1]
    #Register!
    gLogger.info( "Registering sandbox in the DB with", "%s:%s" % ( self.__seNameToUse, sbPath ) )
    result = sandboxDB.registerAndGetSandbox( credDict[ 'username' ], credDict[ 'DN' ], credDict[ 'group' ],
                                              self.__seNameToUse, sbPath, fileHelper.getTransferedBytes() )
    if not result[ 'OK' ]:
      self.__secureUnlinkFile( hdPath )
      return result
    return S_OK( "%s:%s" % ( self.__seNameToUse, sbPath ) )


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
    #Generate the location
    result = self.__generateLocation( sbPath )
    if not result[ 'OK' ]:
      return result
    seName, sePFN = result[ 'Value' ]
    #Register in DB
    credDict = self.getRemoteCredentials()
    result = sandboxDB.getSandboxId( seName, sePFN, credDict[ 'username' ], credDict[ 'group' ] )
    if result[ 'OK' ]:
      return S_OK( "%s:%s" % ( seName, sePFN ) )

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

    #Unlink temporal file if it's there
    self.__secureUnlinkFile( tmpFilePath )
    return S_OK( "%s:%s" % ( seName, sePFN ) )

  def __generateLocation( self, sbPath ):
    """
    Generate the location string
    """
    if self.__useLocalStorage:
      return S_OK( ( self.__localSEName, sbPath ) )
    #It's external storage
    storageElement = StorageElement( self.__externalSEName )
    if not storageElement.isValid()['Value']:
      errStr = "Failed to instantiate destination StorageElement"
      gLogger.error( errStr, self.__externalSEName )
      return S_ERROR( errStr )
    result = storageElement.getPfnForLfn( sbPath )
    if not result['OK']:
      errStr = "Failed to generate PFN"
      gLogger.error( errStr, self.__externalSEName )
      return S_ERROR( errStr )
    destPfn = result['Value']
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
        tfd, destFileName = tempfile.mkstemp( prefix="DSB." )
      except Exception, e:
        return S_ERROR( "Cannot create temporal file: %s" % str(e) )
    destFileName = os.path.realpath( destFileName )
    try:
      os.makedirs( os.path.dirname( destFileName ) )
    except:
      pass
    try:
      fd = open( destFileName, "wb" )
    except Exception, e:
      return S_ERROR( "Cannot open destination file %s" % destFileName )
    result = fileHelper.networkToDataSink( fd )
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
      gLogger.warn(" Could not unlink file %s: %s" % ( filePath, str( e ) ) )
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
          gLogger.error( errMsg, str(e) )
          result = S_ERROR( "%s: %s" % ( errMsg, str(e) ) )
    else:
      result = self.__copyToExternalSE( localFilePath, sbPath )

    return result

  def __copyToExternalSE( self, localFilePath, sbPath ):
    """
    Copy uploaded file to external SE
    """
    try:
      rm = ReplicaManager()
      result = rm.put( sbPath, localFilePath, self.__externalSEName )
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
    except Except, e:
      return S_ERROR( "Error while moving sandbox to SE: %s" % str(e) )

  ##################
  # Assigning sbs to jobs

  types_assignSandboxesToEntities = [ types.DictType ]
  def export_assignSandboxesToEntities( self, enDict, entitySetup = False ):
    """
    Assign sandboxes to jobs.
    Expects a dict of { entityId : [ ( SB, SBType ), ... ] }
    """
    if not entitySetup:
      entitySetup = self.serviceInfoDict[ 'clientSetup' ]
    assignList = []
    for entityId in enDict:
      for sbTuple in enDict[ entityId ]:
        if type( sbTuple ) not in ( types.TupleType, types.ListType ):
          return S_ERROR( "Entry for job %s is not a iterable of tuples/lists" % jobId )
        if len( sbTuple ) != 2:
          return S_ERROR( "SB definition is not ( SBLocation, Type )! It's '%s'" % str( sbTuple ) )
        splitted = List.fromChar( sbTuple[0], ":" )
        if len( splitted ) < 2:
          return S_ERROR( "SB Location has to have SEName:SEPFN form" )
        SEName = splitted[0]
        SEPFN = ":".join( splitted[1:] )
        assignList.append( ( entityId, entitySetup, sbTuple[1], SEName, SEPFN ) )
    if not assignList:
      return S_OK()
    credDict = self.getRemoteCredentials()
    return sandboxDB.assignSandboxesToEntities( assignList, credDict[ 'username' ], credDict[ 'group' ] )

  ##################
  # Unassign sbs to jobs

  types_unassignEntities = [ ( types.ListType, types.TupleType ) ]
  def export_unassignEntities( self, entitiesList, entitiesSetup = False ):
    """
    Unassign a list of jobs
    """
    if not entitiesSetup:
      entitiesSetup = self.serviceInfoDict[ 'clientSetup' ]
    return sandboxDB.unassignEntities( { entitiesSetup : entitiesList } )

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
    result = sandboxDB.getSandboxesAssignedToEntity( entityId, entitySetup, credDict[ 'username' ], credDict[ 'group' ] )
    if not result[ 'OK' ]:
      return result
    sbDict = {}
    for SEName, SEPFN, SBType in result[ 'Value' ]:
      if SBType not in sbDict:
        sbDict[ SBType ] = []
      sbDict[ SBType ].append( "%s:%s" % ( SEName, SEPFN ) )
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
    #If it's a local file
    hdPath = self.__sbToHDPath( fileID )
    if not os.path.isfile( hdPath ):
      return S_ERROR( "Sandbox does not exist" )
    result = fileHelper.getFileDescriptor( hdPath, 'rb' )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get file descriptor: %s' % result[ 'Message' ] )
    fd = result[ 'Value' ]
    return fileHelper.FDToNetwork( fd )

  ##################
  # Purge sandboxes

  def purgeUnusedSandboxes( self ):
    gLogger.info( "Purging sandboxes" )
    result = sandboxDB.getUnusedSandboxes()
    if not result[ 'OK' ]:
      gLogger.error( "Error while retrieving sandboxes to purge", result[ 'Message' ] )
      return result
    sbList = result[ 'Value' ]
    gLogger.info( "Got %s sandboxes to purge" % len( sbList ) )
    deletedFromSE = []
    for sbId, SEName, SEPFN in sbList:
      #self.__purgeSandbox( sbId, SEName, SEPFN )
      gSBDeletionPool.generateJobAndQueueIt( self.__purgeSandbox, args = ( sbId, SEName, SEPFN ) )

  def __purgeSandbox( self, sbId, SEName, SEPFN ):
    result = self.__deleteSandboxFromBackend( SEName, SEPFN )
    if not result[ 'OK' ]:
      gLogger.error( "Cannot delete sandbox from backend", result[ 'Message' ] )
      return
    result = sandboxDB.deleteSandboxes( [ sbId ] )
    if not result[ 'OK' ]:
      gLogger.error( "Cannot delete sandbox from DB", result[ 'Message' ] )

  def __deleteSandboxFromBackend( self, SEName, SEPFN ):
    gLogger.info( "Purging sandbox" "%s:%s" % ( SEName, SEPFN ) )
    if SEName != self.__localSEName:
     return self.__deleteSandboxFromExternalBackend( SEName, SEPFN )
    else:
      hdPath =  self.__sbToHDPath( SEPFN )
      if not os.path.isfile( hdPath ):
        return S_OK()
      try:
        os.unlink( hdPath )
      except Exception, e:
        gLogger.error( "Cannot delete local sandbox", "%s : %s" % ( hdPath, str(e) ) )
      while hdPath:
        hdPath = os.path.dirname( hdPath )
        gLogger.info( "Checking if dir %s is empty" % hdPath )
        if not os.path.isdir( hdPath ):
          break
        if len( os.listdir( hdPath ) ) > 0:
          break
        gLogger.info( "Trying to clean dir %s" % hdPath )
        #Empty dir!
        try:
          os.rmdir( hdPath )
        except Exception, e:
          gLogger.error( "Cannot clean empty directory", "%s : %s" % ( hdPath, str(e) ) )
          break
    return S_OK()

  def __deleteSandboxFromExternalBackend( self, SEName, SEPFN ):
    if self.getCSOption( "DelayedExternalDeletion", True ):
      gLogger.info( "Setting deletion request" )
      try:
        request = RequestContainer()
        result = request.addSubRequest( { 'Attributes' : { 'Operation' : 'removePhysicalFile',
                                                           'TargetSE' : SEName,
                                                           'ExecutionOrder' : 1
                                                          } },
                                         'removal' )
        index = result['Value']
        fileDict = { 'PFN' : SEPFN, 'Status' : 'Waiting' }
        request.setSubRequestFiles( index, 'removal', [ fileDict ] )
        return RequestClient().setRequest( "RemoteSBDeletion:%s:%s:%s" % ( SEName, SEPFN, time.time() ),
                                    request.toXML()[ 'Value' ] )
      except Exception, e:
        gLogger.exception( "Exception while setting deletion request" )
        return S_ERROR( "Cannot set deletion request: %s" % str(e) )
    else:
      gLogger.info( "Deleting external Sandbox" )
      try:
        rm = ReplicaManager()
        return rm.removePhysicalFile( SEPFN, SEName )
      except Exception, e:
        gLogger.exception( "RM raised an exception while trying to delete a remote sandbox" )
        return S_ERROR( "RM raised an exception while trying to delete a remote sandbox" )