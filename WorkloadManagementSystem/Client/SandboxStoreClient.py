""" Client for the SandboxStore.
    Will connect to the WorkloadManagement/SandboxStore service.
"""

__RCSID__ = "$Id$"

import os
import tarfile
import hashlib
import tempfile
import re
import StringIO

from DIRAC import gLogger, S_OK, S_ERROR, gConfig

from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Core.Utilities.File import getGlobbedTotalSize
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup

class SandboxStoreClient( object ):

  __validSandboxTypes = ( 'Input', 'Output' )
  __smdb = None

  def __init__( self, rpcClient = None, transferClient = None, **kwargs ):

    self.__serviceName = "WorkloadManagement/SandboxStore"
    self.__rpcClient = rpcClient
    self.__transferClient = transferClient
    self.__kwargs = kwargs
    self.__vo = None
    if 'delegatedGroup' in kwargs:
      self.__vo = getVOForGroup( kwargs['delegatedGroup'] )
    if SandboxStoreClient.__smdb == None:
      try:
        from DIRAC.WorkloadManagementSystem.DB.SandboxMetadataDB import SandboxMetadataDB
        SandboxStoreClient.__smdb = SandboxMetadataDB()
        result = SandboxStoreClient.__smdb._getConnection()
        if not result[ 'OK' ]:
          SandboxStoreClient.__smdb = False
        else:
          result[ 'Value' ].close()
      except ( ImportError, RuntimeError, AttributeError ):
        SandboxStoreClient.__smdb = False

  def __getRPCClient( self ):
    if self.__rpcClient:
      return self.__rpcClient
    else:
      return RPCClient( self.__serviceName, **self.__kwargs )

  def __getTransferClient( self ):
    if self.__transferClient:
      return self.__transferClient
    else:
      return TransferClient( self.__serviceName, **self.__kwargs )

  # Upload sandbox to jobs and pilots

  def uploadFilesAsSandboxForJob( self, fileList, jobId, sbType, sizeLimit = 0 ):
    if sbType not in self.__validSandboxTypes:
      return S_ERROR( "Invalid Sandbox type %s" % sbType )
    return self.uploadFilesAsSandbox( fileList, sizeLimit, assignTo = { "Job:%s" % jobId: sbType } )

  def uploadFilesAsSandboxForPilot( self, fileList, jobId, sbType, sizeLimit = 0 ):
    if sbType not in self.__validSandboxTypes:
      return S_ERROR( "Invalid Sandbox type %s" % sbType )
    return self.uploadFilesAsSandbox( fileList, sizeLimit, assignTo = { "Pilot:%s" % jobId: sbType } )

  # Upload generic sandbox

  def uploadFilesAsSandbox( self, fileList, sizeLimit = 0, assignTo = {} ):
    """ Send files in the fileList to a Sandbox service for the given jobID.
        This is the preferable method to upload sandboxes.

        a fileList item can be:
          - a string, which is an lfn name
          - a file name (real), that is supposed to be on disk, in the current directory
          - a fileObject that should be a StringIO.StringIO type of object

        Parameters:
          - assignTo : Dict containing { 'Job:<jobid>' : '<sbType>', ... }
    """
    errorFiles = []
    files2Upload = []

    for key in assignTo:
      if assignTo[ key ] not in self.__validSandboxTypes:
        return S_ERROR( "Invalid sandbox type %s" % assignTo[ key ] )

    if not isinstance( fileList, ( list, tuple ) ):
      return S_ERROR( "fileList must be a list or tuple!" )

    for sFile in fileList:
      if isinstance( sFile, basestring ):
        if re.search( '^lfn:', sFile, flags = re.IGNORECASE ):
          pass
        else:
          if os.path.exists( sFile ):
            files2Upload.append( sFile )
          else:
            errorFiles.append( sFile )
      
      elif isinstance( sFile, StringIO.StringIO ):
        files2Upload.append( sFile )
      else:
        return S_ERROR("Objects of type %s can't be part of InputSandbox" % type( sFile ) )

    if errorFiles:
      return S_ERROR( "Failed to locate files: %s" % ", ".join( errorFiles ) )

    try:
      fd, tmpFilePath = tempfile.mkstemp( prefix = "LDSB." )
      os.close( fd )
    except Exception as e:
      return S_ERROR( "Cannot create temporal file: %s" % str( e ) )

    tf = tarfile.open( name = tmpFilePath, mode = "w|bz2" )
    for sFile in files2Upload:
      if isinstance( sFile, basestring ):
        tf.add( os.path.realpath( sFile ), os.path.basename( sFile ), recursive = True )
      elif isinstance( sFile, StringIO.StringIO ):
        tarInfo = tarfile.TarInfo( name = 'jobDescription.xml' )
        tarInfo.size = len( sFile.buf )
        tf.addfile( tarinfo = tarInfo, fileobj = sFile )
    tf.close()

    if sizeLimit > 0:
      # Evaluate the compressed size of the sandbox
      if getGlobbedTotalSize( tmpFilePath ) > sizeLimit:
        result = S_ERROR( "Size over the limit" )
        result[ 'SandboxFileName' ] = tmpFilePath
        return result

    oMD5 = hashlib.md5()
    with open( tmpFilePath, "rb" ) as fd:
      bData = fd.read( 10240 )
      while bData:
        oMD5.update( bData )
        bData = fd.read( 10240 )

    transferClient = self.__getTransferClient()
    result = transferClient.sendFile( tmpFilePath, ( "%s.tar.bz2" % oMD5.hexdigest(), assignTo ) )
    result[ 'SandboxFileName' ] = tmpFilePath
    try:
      if result['OK']:
        os.unlink( tmpFilePath )
    except:
      pass
    return result

  ##############
  # Download sandbox

  def downloadSandbox( self, sbLocation, destinationDir = "", inMemory = False, unpack = True ):
    """
    Download a sandbox file and keep it in bundled form
    """
    if sbLocation.find( "SB:" ) != 0:
      return S_ERROR( "Invalid sandbox URL" )
    sbLocation = sbLocation[ 3: ]
    sbSplit = sbLocation.split( "|" )
    if len( sbSplit ) < 2:
      return S_ERROR( "Invalid sandbox URL" )
    SEName = sbSplit[0]
    SEPFN = "|".join( sbSplit[1:] )
    # If destination dir is not specified use current working dir
    # If its defined ensure the dir structure is there
    if not destinationDir:
      destinationDir = os.getcwd()
    else:
      try:
        os.makedirs( destinationDir )
      except:
        pass

    try:
      tmpSBDir = tempfile.mkdtemp( prefix = "TMSB." )
    except Exception as e:
      return S_ERROR( "Cannot create temporal file: %s" % str( e ) )

    se = StorageElement( SEName, vo = self.__vo )
    result = returnSingleResult( se.getFile( SEPFN, localPath = tmpSBDir ) )

    if not result[ 'OK' ]:
      return result
    sbFileName = os.path.basename( SEPFN )

    result = S_OK()
    tarFileName = os.path.join( tmpSBDir, sbFileName )

    if inMemory:
      try:
        tfile = open( tarFileName, 'r' )
        data = tfile.read()
        tfile.close()
        os.unlink( tarFileName )
        os.rmdir( tmpSBDir )
      except Exception as e:
        os.unlink( tarFileName )
        os.rmdir( tmpSBDir )
        return S_ERROR( 'Failed to read the sandbox archive: %s' % str( e ) )
      return S_OK( data )

    if not unpack:
      result[ 'Value' ] = tarFileName
      return result

    try:
      sandboxSize = 0
      tf = tarfile.open( name = tarFileName, mode = "r" )
      for tarinfo in tf:
        tf.extract( tarinfo, path = destinationDir )
        sandboxSize += tarinfo.size
      tf.close()
      result[ 'Value' ] = sandboxSize
    except Exception as e:
      result = S_ERROR( "Could not open bundle: %s" % str( e ) )

    try:
      os.unlink( tarFileName )
      os.rmdir( tmpSBDir )
    except Exception as e:
      gLogger.warn( "Could not remove temporary dir %s: %s" % ( tmpSBDir, str( e ) ) )

    return result

  ##############
  # Jobs

  def getSandboxesForJob( self, jobId ):
    return self.__getSandboxesForEntity( "Job:%s" % jobId )

  def assignSandboxesToJob( self, jobId, sbList, ownerName = "", ownerGroup = "", eSetup = "" ):
    return self.__assignSandboxesToEntity( "Job:%s" % jobId, sbList, ownerName, ownerGroup, eSetup )

  def assignSandboxToJob( self, jobId, sbLocation, sbType, ownerName = "", ownerGroup = "", eSetup = "" ):
    return self.__assignSandboxToEntity( "Job:%s" % jobId, sbLocation, sbType, ownerName, ownerGroup, eSetup )

  def unassignJobs( self, jobIdList ):
    if isinstance( jobIdList, ( int, long ) ):
      jobIdList = [ jobIdList ]
    entitiesList = []
    for jobId in jobIdList:
      entitiesList.append( "Job:%s" % jobId )
    return self.__unassignEntities( entitiesList )

  def downloadSandboxForJob( self, jobId, sbType, destinationPath = "", inMemory = False ):
    result = self.__getSandboxesForEntity( "Job:%s" % jobId )
    if not result[ 'OK' ]:
      return result
    sbDict = result[ 'Value' ]
    if sbType not in sbDict:
      return S_ERROR( "No %s sandbox registered for job %s" % ( sbType, jobId ) )
    for sbLocation in sbDict[ sbType ]:
      result = self.downloadSandbox( sbLocation, destinationPath, inMemory )
      if not result[ 'OK' ]:
        return result
      if inMemory:
        return result
    return S_OK()

  ##############
  # Pilots

  def getSandboxesForPilot( self, pilotId ):
    return self.__getSandboxesForEntity( "Pilot:%s" % pilotId )

  def assignSandboxesToPilot( self, pilotId, sbList, ownerName = "", ownerGroup = "", eSetup = "" ):
    return self.__assignSandboxesToEntity( "Pilot:%s" % pilotId, sbList, ownerName, ownerGroup, eSetup )

  def assignSandboxToPilot( self, pilotId, sbLocation, sbType, ownerName = "", ownerGroup = "", eSetup = "" ):
    return self.__assignSandboxToEntity( "Pilot:%s" % pilotId, sbLocation, sbType, ownerName, ownerGroup, eSetup )

  def unassignPilots( self, pilotIdIdList ):
    if isinstance( pilotIdIdList, ( int, long ) ):
      pilotIdIdList = [ pilotIdIdList ]
    entitiesList = []
    for pilotId in pilotIdIdList:
      entitiesList.append( "Pilot:%s" % pilotId )
    return self.__unassignEntities( entitiesList )

  def downloadSandboxForPilot( self, jobId, sbType, destinationPath = "" ):
    result = self.__getSandboxesForEntity( "Pilot:%s" % jobId )
    if not result[ 'OK' ]:
      return result
    sbDict = result[ 'Value' ]
    if sbType not in sbDict:
      return S_ERROR( "No %s sandbox registered for pilot %s" % ( sbType, jobId ) )
    for sbLocation in sbDict[ sbType ]:
      result = self.downloadSandbox( sbLocation, destinationPath )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  ##############
  # Entities

  def __getSandboxesForEntity( self, eId ):
    """
    Get the sandboxes assigned to jobs and the relation type
    """
    rpcClient = self.__getRPCClient()
    return rpcClient.getSandboxesAssignedToEntity( eId )


  def __assignSandboxesToEntity( self, eId, sbList, ownerName = "", ownerGroup = "", eSetup = "" ):
    """
    Assign sandboxes to a job.
    sbList must be a list of sandboxes and relation types
      sbList = [ ( "SB:SEName|SEPFN", "Input" ), ( "SB:SEName|SEPFN", "Output" ) ]
    """
    for sbT in sbList:
      if sbT[1] not in self.__validSandboxTypes:
        return S_ERROR( "Invalid Sandbox type %s" % sbT[1] )
    if SandboxStoreClient.__smdb and ownerName and ownerGroup:
      if not eSetup:
        eSetup = gConfig.getValue( "/DIRAC/Setup", "Production" )
      return SandboxStoreClient.__smdb.assignSandboxesToEntities( { eId : sbList }, ownerName, ownerGroup, eSetup )
    rpcClient = self.__getRPCClient()
    return rpcClient.assignSandboxesToEntities( { eId : sbList }, ownerName, ownerGroup, eSetup )

  def __assignSandboxToEntity( self, eId, sbLocation, sbType, ownerName = "", ownerGroup = "", eSetup = "" ):
    """
    Assign a sandbox to a job
      sbLocation is "SEName:SEPFN"
      sbType is Input or Output
    """
    return self.__assignSandboxesToEntity( eId, [ ( sbLocation, sbType ) ], ownerName, ownerGroup, eSetup )

  def __unassignEntities( self, eIdList ):
    """
    Unassign a list of jobs of their respective sandboxes
    """
    rpcClient = self.__getRPCClient()
    return rpcClient.unassignEntities( eIdList )
