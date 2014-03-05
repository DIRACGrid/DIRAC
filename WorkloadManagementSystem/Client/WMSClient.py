""" DIRAC Workload Management System Client class encapsulates all the
    methods necessary to communicate with the Workload Management System
"""

from DIRAC                                     import S_OK, S_ERROR, gLogger

from DIRAC.Core.DISET.RPCClient                import RPCClient
from DIRAC.Core.DISET.TransferClient           import TransferClient
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities                      import File
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient  import SandboxStoreClient

import os

__RCSID__ = "$Id$"

class WMSClient( object ):

  def __init__( self, jobManagerClient = None, sbRPCClient = None, sbTransferClient = None,
                useCertificates = False, timeout = 600 ):
    """ WMS Client constructor

        Here we also initialize the needed clients and connections
    """
    self.timeout = timeout

    if not jobManagerClient:
      self.jobManager = RPCClient( 'WorkloadManagement/JobManager', useCertificates = useCertificates,
                                   timeout = self.timeout )
    else:
      self.jobManager = jobManagerClient

    if not sbRPCClient:
      sbRPCClient = RPCClient( 'WorkloadManagement/SandboxStore', useCertificates = useCertificates )

    if not sbTransferClient:
      sbTransferClient = TransferClient( 'WorkloadManagement/SandboxStore', useCertificates = useCertificates )

    self.sandboxClient = SandboxStoreClient( rpcClient = sbRPCClient,
                                             transferClient = sbTransferClient,
                                             useCertificates = useCertificates )

    self.jobManagerSafe = RPCClient( 'WorkloadManagement/JobManager', useCertificates = False, timeout = self.timeout )

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

  def __uploadInputSandbox( self, classAdJob ):
    """Checks the validity of the job Input Sandbox.
       The function returns the list of Input Sandbox files.
       The total volume of the input sandbox is evaluated
    """
    inputSandbox = self.__getInputSandboxEntries( classAdJob )

    badFiles = []
    okFiles = []
    realFiles = []
    for isFile in inputSandbox:
      valid = True
      for tag  in ( 'lfn:', 'LFN:', 'SB:', '%s' ):  # in case of parametric input sandbox, there is %s passed, so have to ignore it also
        if isFile.find( tag ) == 0:
          valid = False
          break
      if valid:
        realFiles.append( isFile )
    # If there are no files, skip!
    if not realFiles:
      return S_OK()
    # Check real files
    for isFile in realFiles:
      if not os.path.exists( isFile ):
        badFiles.append( isFile )
        gLogger.warn( "inputSandbox file/directory " + isFile + " not found. Keep looking for the others" )
        continue
      okFiles.append( isFile )

    totalSize = File.getGlobbedTotalSize( okFiles )
    gLogger.verbose( "Total size of the inputSandbox: " + str( totalSize ) )
    if badFiles:
      result = S_ERROR( 'Input Sandbox is not valid' )
      result['BadFile'] = badFiles
      result['TotalSize'] = totalSize
      return result

    if okFiles:
      result = self.sandboxClient.uploadFilesAsSandbox( okFiles )
      if not result[ 'OK' ]:
        return result
      inputSandbox.append( result[ 'Value' ] )
      classAdJob.insertAttributeVectorString( "InputSandbox", inputSandbox )

    return S_OK()

  def submitJob( self, jdl ):
    """ Submit one job specified by its JDL to WMS
    """

    if os.path.exists( jdl ):
      fic = open ( jdl, "r" )
      jdlString = fic.read()
      fic.close()
    else:
      # If file JDL does not exist, assume that the JDL is passed as a string
      jdlString = jdl

    # Check the validity of the input JDL
    jdlString = jdlString.strip()
    if jdlString.find( "[" ) != 0:
      jdlString = "[%s]" % jdlString
    classAdJob = ClassAd( jdlString )
    if not classAdJob.isOK():
      return S_ERROR( 'Invalid job JDL' )

    # Check the size and the contents of the input sandbox
    result = self.__uploadInputSandbox( classAdJob )
    if not result['OK']:
      return result

    # Submit the job now and get the new job ID
    result = self.jobManager.submitJob( classAdJob.asJDL() )
    if 'requireProxyUpload' in result and result['requireProxyUpload']:
      gLogger.warn( "Need to upload the proxy" )
    return result

  def killJob( self, jobID ):
    """ Kill running job.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    return self.jobManagerSafe.killJob( jobID )

  def deleteJob( self, jobID ):
    """ Delete job(s) from the WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    return self.jobManagerSafe.deleteJob( jobID )

  def rescheduleJob( self, jobID ):
    """ Reschedule job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    return self.jobManagerSafe.rescheduleJob( jobID )

  def resetJob( self, jobID ):
    """ Reset job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    return self.jobManagerSafe.resetJob( jobID )
