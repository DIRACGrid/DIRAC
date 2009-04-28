import os
import tarfile
import md5
import tempfile
import types
import re
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.Utilities.File import getSize, getGlobbedTotalSize
from DIRAC.Core.Utilities import List
from DIRAC import gLogger, S_OK, S_ERROR

class SandboxClient:

  def __init__( self ):
    self.__serviceName = "DataManagement/SandboxStore"

  def __getRPCClient( self ):
    return RPCClient( self.__serviceName )

  def __getTransferClient( self ):
    return TransferClient( self.__serviceName )

  def uploadFilesAsSandbox( self, fileList, sizeLimit = 0 ):
    """ Send files in the fileList to a Sandbox service for the given jobID.
        This is the preferable method to upload sandboxes. fileList can contain
        both files and directories
    """
    errorFiles = []
    files2Upload = []
    for file in fileList:
      if re.search( '^lfn:', file ) or re.search( '^LFN:', file ):
        pass
      else:
        if os.path.exists( file ):
          files2Upload.append( file )
        else:
          errorFiles.append( file )

    if errorFiles:
      return S_ERROR( "Failed to locate files: %s" % ", ".join( errorFiles ) )

    try:
      fd, tmpFilePath = tempfile.mkstemp( prefix="LDSB." )
    except Exception, e:
      return S_ERROR( "Cannot create temporal file: %s" % str(e) )

    tf = tarfile.open( name = tmpFilePath, mode = "w|bz2" )
    for file in files2Upload:
      tf.add( os.path.realpath( file ), os.path.basename( file ), recursive = True )
    tf.close()

    if sizeLimit > 0:
      # Evaluate the compressed size of the sandbox
      if getGlobbedTotalSize( tmpFilePath ) > sizeLimit:
        result = S_ERROR( "Size over the limit" )
        result[ 'SandboxFileName' ] = tmpFilePath
        return result

    oMD5 = md5.new()
    fd = open( tmpFilePath, "rb" )
    bData = fd.read( 10240 )
    while bData:
      oMD5.update( bData )
      bData = fd.read( 10240 )
    fd.close()

    transferClient = self.__getTransferClient()
    result = transferClient.sendFile( tmpFilePath, "%s.tar.bz2" % oMD5.hexdigest() )
    try:
      os.unlink( tmpFilePath )
    except:
      pass
    return result

  ##############
  # Download sandbox

  def downloadSandboxAsBundle( self, sbLocation, destinationDir ):
    """
    Download a sandbox file and keep it in bundled form
    """
    split = List.fromChar( sbLocation, ":" )
    if len( split ) < 2:
      return S_ERROR( "The Sandbox must be in the form SEName:SEPFN" )
    try:
      os.makedirs( destinationDir )
    except:
      pass
    SEName = split[0]
    SEPFN = ":".join( split[1:] )
    print SEName, SEPFN
    print "."*10
    rm = ReplicaManager()
    return rm.getPhysicalFile( SEPFN, SEName, destinationDir, singleFile = True )

  ##############
  # Jobs

  def getSandboxesForJob( self, jobId ):
    return self.__getSandboxesForEntity( "Job:%s" % jobId )

  def assignSandboxesToJob( self, jobId, sbList ):
    return self.__assignSandboxesToEntity( "Job:%s" % jobId, sbList )

  def assignSandboxToJob( self, jobId, sbLocation, sbType ):
    return self.__assignSandboxToEntity( "Job:%s" % jobId, sbLocation, sbType )

  def unassignJobs( self, jobIdList ):
    if type( jobIdList ) in ( types.IntType, types.LongType ):
      jobIdList = [ jobIdList ]
    entitiesList = []
    for jobId in jobIdList:
      entitiesList.append( "Job:%s" % jobId )
    return self.__unassignEntities( entitiesList )

  ##############
  # Pilots

  def getSandboxesForPilot( self, pilotId ):
    return self.__getSandboxesForEntity( "Pilot:%s" % pilotId )

  def assignSandboxesToPilot( self, pilotId, sbList ):
    return self.__assignSandboxesToEntity( "Pilot:%s" % pilotId, sbList )

  def assignSandboxToPilot( self, pilotId, sbLocation, sbType ):
    return self.__assignSandboxToEntity( "Pilot:%s" % pilotId, sbLocation, sbType )

  def unassignPilots( self, pilotIdIdList ):
    if type( pilotIdIdList ) in ( types.IntType, types.LongType ):
      pilotIdIdList = [ pilotIdIdList ]
    entitiesList = []
    for pilotId in pilotIdIdList:
      entitiesList.append( "Pilot:%s" % pilotId )
    return self.__unassignEntities( entitiesList )

  ##############
  # Entities

  def __getSandboxesForEntity( self, eId ):
    """
    Get the sandboxes assigned to jobs and the relation type
    """
    return self.__getRPCClient().getSandboxesAssignedToEntity( eId )

  def __assignSandboxesToEntity( self, eId, sbList ):
    """
    Assign sandboxes to a job.
    sbList must be a list of sandboxes and relation types
      sbList = [ ( "SEName:SEPFN", "Input" ), ( "SEName:SEPFN", "Output" ) ]
    """
    return self.__getRPCClient().assignSandboxesToEntities( { eId : sbList } )

  def __assignSandboxToEntity( self, eId, sbLocation, sbType ):
    """
    Assign a sandbox to a job
      sbLocation is "SEName:SEPFN"
      sbType is Input or Output
    """
    return self.__assignSandboxesToEntity( eId, [ ( sbLocation, sbType ) ] )

  def __unassignEntities( self, eIdList ):
    """
    Unassign a list of jobs of their respective sandboxes
    """
    return self.__getRPCClient().unassignEntities( eIdList )


