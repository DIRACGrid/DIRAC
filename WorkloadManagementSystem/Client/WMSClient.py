""" DIRAC Workload Management System Client class encapsulates all the
    methods necessary to communicate with the Workload Management System
"""

import os
import StringIO

from DIRAC                                     import S_OK, S_ERROR, gLogger

from DIRAC.Core.DISET.RPCClient                import RPCClient
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities                      import File
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient  import SandboxStoreClient

__RCSID__ = "$Id$"

class WMSClient( object ):

  def __init__( self, jobManagerClient = None, sbRPCClient = None, sbTransferClient = None,
                useCertificates = False, timeout = 600 ):
    """ WMS Client constructor

        Here we also initialize the needed clients and connections
    """

    self.useCertificates = useCertificates
    self.timeout = timeout
    self.jobManager = jobManagerClient
    self.sandboxClient = None
    if sbRPCClient and sbTransferClient:
      self.sandboxClient = SandboxStoreClient( rpcClient = sbRPCClient,
                                               transferClient = sbTransferClient,
                                               useCertificates = useCertificates )

###############################################################################

  def __getInputSandboxEntries( self, classAdJob ):
    if classAdJob.lookupAttribute( "InputSandbox" ):
      inputSandbox = classAdJob.get_expression( "InputSandbox" )
      inputSandbox = inputSandbox.replace( '","', "\n" )
      inputSandbox = inputSandbox.replace( '{', "" )
      inputSandbox = inputSandbox.replace( '}', "" )
      inputSandbox = inputSandbox.replace( '"', "" )
      inputSandbox = inputSandbox.replace( ',', "" )
      inputSandbox = inputSandbox.split()
    else:
      inputSandbox = []

    return inputSandbox

  def __uploadInputSandbox( self, classAdJob, jobDescriptionObject = None ):
    """Checks the validity of the job Input Sandbox.
       The function returns the list of Input Sandbox files.
       The total volume of the input sandbox is evaluated
    """
    inputSandbox = self.__getInputSandboxEntries( classAdJob )

    realFiles = []
    badFiles = []
    diskFiles = []

    for isFile in inputSandbox:
      if not isFile.startswith( ( 'lfn:', 'LFN:', 'SB:', '%s', '%(' ) ):
        realFiles.append( isFile )

    stringIOFiles = []
    stringIOFilesSize = 0
    if jobDescriptionObject is not None:
      if isinstance( jobDescriptionObject, StringIO.StringIO ):
        stringIOFiles = [jobDescriptionObject]
        stringIOFilesSize = len( jobDescriptionObject.buf )
        gLogger.debug( "Size of the stringIOFiles: " + str( stringIOFilesSize ) )
      else:
        return S_ERROR( "jobDescriptionObject is not a StringIO object" )

    # Check real files
    for isFile in realFiles:
      if not os.path.exists( isFile ):  # we are passing in real files, we expect them to be on disk
        badFiles.append( isFile )
        gLogger.warn( "inputSandbox file/directory " + isFile + " not found. Keep looking for the others" )
        continue
      diskFiles.append( isFile )

    diskFilesSize = File.getGlobbedTotalSize( diskFiles )
    gLogger.debug( "Size of the diskFiles: " + str( diskFilesSize ) )
    totalSize = diskFilesSize + stringIOFilesSize
    gLogger.verbose( "Total size of the inputSandbox: " + str( totalSize ) )

    okFiles = stringIOFiles + diskFiles
    if badFiles:
      result = S_ERROR( 'Input Sandbox is not valid' )
      result['BadFile'] = badFiles
      result['TotalSize'] = totalSize
      return result

    if okFiles:
      if not self.sandboxClient:
        self.sandboxClient = SandboxStoreClient( useCertificates = self.useCertificates )
      result = self.sandboxClient.uploadFilesAsSandbox( okFiles )
      if not result[ 'OK' ]:
        return result
      inputSandbox.append( result[ 'Value' ] )
      classAdJob.insertAttributeVectorString( "InputSandbox", inputSandbox )

    return S_OK()

  def submitJob( self, jdl, jobDescriptionObject = None ):
    """ Submit one job specified by its JDL to WMS
    """

    if os.path.exists( jdl ):
      fic = open ( jdl, "r" )
      jdlString = fic.read()
      fic.close()
    else:
      # If file JDL does not exist, assume that the JDL is passed as a string
      jdlString = jdl

    jdlString = jdlString.strip()

    # Strip of comments in the jdl string
    newJdlList = []
    for line in jdlString.split('\n'):
      if not line.strip().startswith( '#' ):
        newJdlList.append( line )
    jdlString = '\n'.join( newJdlList )

    # Check the validity of the input JDL
    if jdlString.find( "[" ) != 0:
      jdlString = "[%s]" % jdlString
    classAdJob = ClassAd( jdlString )
    if not classAdJob.isOK():
      return S_ERROR( 'Invalid job JDL' )

    # Check the size and the contents of the input sandbox
    result = self.__uploadInputSandbox( classAdJob, jobDescriptionObject )
    if not result['OK']:
      return result

    # Submit the job now and get the new job ID
    if not self.jobManager:
      self.jobManager = RPCClient( 'WorkloadManagement/JobManager',
                                    useCertificates = self.useCertificates,
                                    timeout = self.timeout )
    result = self.jobManager.submitJob( classAdJob.asJDL() )
    if 'requireProxyUpload' in result and result['requireProxyUpload']:
      gLogger.warn( "Need to upload the proxy" )
    return result

  def killJob( self, jobID ):
    """ Kill running job.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    if not self.jobManager:
      self.jobManager = RPCClient( 'WorkloadManagement/JobManager',
                                    useCertificates = self.useCertificates,
                                    timeout = self.timeout )
    return self.jobManager.killJob( jobID )

  def deleteJob( self, jobID ):
    """ Delete job(s) from the WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    if not self.jobManager:
      self.jobManager = RPCClient( 'WorkloadManagement/JobManager',
                                    useCertificates = self.useCertificates,
                                    timeout = self.timeout )
    return self.jobManager.deleteJob( jobID )

  def rescheduleJob( self, jobID ):
    """ Reschedule job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    if not self.jobManager:
      self.jobManager = RPCClient( 'WorkloadManagement/JobManager',
                                    useCertificates = self.useCertificates,
                                    timeout = self.timeout )
    return self.jobManager.rescheduleJob( jobID )

  def resetJob( self, jobID ):
    """ Reset job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    if not self.jobManager:
      self.jobManager = RPCClient( 'WorkloadManagement/JobManager',
                                    useCertificates = self.useCertificates,
                                    timeout = self.timeout )
    return self.jobManager.resetJob( jobID )
