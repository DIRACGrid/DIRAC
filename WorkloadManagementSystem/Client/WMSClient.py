""" DIRAC Workload Management System Client class encapsulates all the
    methods necessary to communicate with the Workload Management System
"""

from DIRAC.Core.DISET.RPCClient                import RPCClient
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC                                     import S_OK, S_ERROR
from DIRAC.Core.Utilities                      import File
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient  import SandboxStoreClient

import os, commands

class WMSClient:

  def __init__( self, jobManagerClient = False, sbRPCClient = False, sbTransferClient = False,
                useCertificates = False, timeout = 120 ):
    """ WMS Client constructor
    """

    self.timeout = timeout

    if not jobManagerClient:
      self.jobManagerClient = RPCClient( 'WorkloadManagement/JobManager', useCertificates = useCertificates,
                                         timeout = self.timeout )
    else:
      self.jobManagerClient = jobManagerClient

    self.sandboxClient = SandboxStoreClient( useCertificates = useCertificates, rpcClient = sbRPCClient,
                                             transferClient = sbTransferClient )

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

  #This are the NEW methods

  def __uploadInputSandbox( self, classAdJob ):
    """Checks the validity of the job Input Sandbox.
       The function returns the list of Input Sandbox files.
       The total volume of the input sandbox is evaluated
    """
    inputSandbox = self.__getInputSandboxEntries( classAdJob )

    realFiles = []
    badFiles = []
    okFiles = []
    realFiles = []
    for file in inputSandbox:
      valid = True
      for tag  in ( 'lfn:', 'LFN:', 'SB:', '%s' ):#in case of parametric input sandbox, there is %s passed, so have to ignore it also
        if file.find( tag ) == 0:
          valid = False
          break
      if valid:
        realFiles.append( file )
    #If there are no files, skip!
    if not realFiles:
      return S_OK()
    #Check real files
    for file in realFiles:
      if not os.path.exists( file ):
        badFiles.append( file )
        print "inputSandbox file/directory " + file + " not found"
        continue
      okFiles.append( file )

    #print "Total size of the inputSandbox: "+str(totalSize)
    totalSize = File.getGlobbedTotalSize( okFiles )
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

  def __assignSandboxesToJob( self, jobID, classAdJob ):
    sandboxClient = SandboxStoreClient()
    inputSandboxes = self.__getInputSandboxEntries( classAdJob )
    sbToAssign = []
    for isb in inputSandboxes:
      if isb.find( "SB:" ) == 0:
        sbToAssign.append( isb )
    if sbToAssign:
      assignList = [ ( isb, 'Input' ) for isb in sbToAssign ]
      result = sandboxClient.assignSandboxesToJob( jobID, assignList )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def submitJob( self, jdl ):
    """ Submit one job specified by its JDL to WMS
    """

    if os.path.exists( jdl ):
      fic = open ( jdl, "r" )
      jdlString = fic.read()
      fic.close()
    else:
      # If file JDL does not exist, assume that the JDL is
      # passed as a string
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
    result = self.jobManagerClient.submitJob( classAdJob.asJDL() )

    if not result['OK']:
      return result
    jobID = result['Value']
    if 'requireProxyUpload' in result and result[ 'requireProxyUpload' ]:
      #TODO: We should notify the user to upload a proxy with proxy-upload
      pass


    #print "Sandbox uploading"
    return S_OK( jobID )

  #This is the OLD method

  def __checkInputSandbox( self, classAdJob ):
    """Checks the validity of the job Input Sandbox.
       The function returns the list of Input Sandbox files.
       The total volume of the input sandbox is evaluated
    """

    inputSandbox = self.__getInputSandboxEntries( classAdJob )
    if inputSandbox:
      ok = 1
      #print inputSandbox
      # Check the Input Sandbox files

      totalSize = 0
      for file in inputSandbox:
        if file.find( 'lfn:' ) != 0 and file.find( 'LFN:' ) != 0 and file.find( "SB:" ) != 0:
          if not os.path.exists( file ):
            badfile = file
            print "inputSandbox file/directory " + file + " not found"
            ok = 0
          else:
            if os.path.isdir( file ):
              comm = 'du -b -s ' + file
              status, out = commands.getstatusoutput( comm )
              try:
                dirSize = int( out.split()[0] )
              except Exception, x:
                print "Input Sandbox directory name", file, "is not valid !"
                print str( x )
                badfile = file
                ok = 0
              totalSize = totalSize + dirSize
            else:
              totalSize = int( os.stat( file )[6] ) + totalSize

      #print "Total size of the inputSandbox: "+str(totalSize)
      if not ok:
        result = S_ERROR( 'Input Sandbox is not valid' )
        result['BadFile'] = file
        result['TotalSize'] = totalSize
        return result

      result = S_OK()
      result['InputSandbox'] = inputSandbox
      result['TotalSize'] = totalSize
      return result
    else:
      #print "No input sandbox defined for this job."
      result = S_OK()
      result['TotalSize'] = 0
      result['InputSandbox'] = None
      return result

  def killJob( self, jobID ):
    """ Kill running job.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    jobManager = RPCClient( 'WorkloadManagement/JobManager', useCertificates = False, timeout = self.timeout )
    result = jobManager.killJob( jobID )
    return result

  def deleteJob( self, jobID ):
    """ Delete job(s) from the WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    jobManager = RPCClient( 'WorkloadManagement/JobManager', useCertificates = False, timeout = self.timeout )
    result = jobManager.deleteJob( jobID )
    return result

  def rescheduleJob( self, jobID ):
    """ Reschedule job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    jobManager = RPCClient( 'WorkloadManagement/JobManager', useCertificates = False, timeout = self.timeout )
    result = jobManager.rescheduleJob( jobID )
    return result

  def resetJob( self, jobID ):
    """ Reset job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    jobManager = RPCClient( 'WorkloadManagement/JobManager', useCertificates = False, timeout = self.timeout )
    result = jobManager.resetJob( jobID )
    return result
