"""
   DIRAC API Class

   All DIRAC functionality is exposed through the DIRAC API and this
   serves as a source of documentation for the project via EpyDoc.

   The DIRAC API provides the following functionality:
    - A transparent and secure way for users
      to submit jobs to the Grid, monitor them and
      retrieve outputs
    - Interaction with Grid storage and file catalogues
      via the DataManagement public interfaces (more to be added)
    - Local execution of workflows for testing purposes.

"""
__RCSID__ = "$Id$"

import re, os, sys, time, shutil, types, tempfile, glob, tarfile, urllib
import DIRAC

from DIRAC.Core.Base.API                                 import API
from DIRAC.Interfaces.API.JobRepository                  import JobRepository
from DIRAC.Core.Utilities.ClassAd.ClassAdLight           import ClassAd
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.Utilities.ModuleFactory                  import ModuleFactory
from DIRAC.WorkloadManagementSystem.Client.WMSClient     import WMSClient
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient     import SandboxStoreClient
# from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataManager       import DataManager
from DIRAC.Resources.Storage.StorageElement              import StorageElement
from DIRAC.Resources.Catalog.FileCatalog                 import FileCatalog
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.ConfigurationSystem.Client.PathFinder         import getSystemSection, getServiceURL
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry   import getVOForGroup
from DIRAC.Core.Utilities.Time                           import toString
from DIRAC.Core.Utilities.List                           import breakListIntoChunks
from DIRAC.Core.Utilities.SiteSEMapping                  import getSEsForSite
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.Core.Base.AgentReactor                        import AgentReactor
from DIRAC.Core.Security.X509Chain                       import X509Chain
from DIRAC.Core.Security                                 import Locations
from DIRAC.Core.Utilities                                import Time
from DIRAC                                               import gConfig, gLogger, S_OK, S_ERROR

COMPONENT_NAME = 'DiracAPI'

class Dirac( API ):
  """
   DIRAC API Class
  """

  #############################################################################
  def __init__( self, withRepo = False, repoLocation = '', useCertificates = False ):
    """Internal initialization of the DIRAC API.
    """
    super( Dirac, self ).__init__()

    self.section = '/LocalSite/'

    self.jobRepo = False
    if withRepo:
      self.jobRepo = JobRepository( repoLocation )
      if not self.jobRepo.isOK():
        gLogger.error( "Unable to write to supplied repository location" )
        self.jobRepo = False

    self.scratchDir = gConfig.getValue( self.section + 'ScratchDir', '/tmp' )
    self.useCertificates = useCertificates

    # Determine the default file catalog
    self.defaultFileCatalog = gConfig.getValue( self.section + '/FileCatalog', None )

    self.__clients = None

  #############################################################################
  # Repository specific methods
  #############################################################################
  def getRepositoryJobs( self, printOutput = False ):
    """ Retireve all the jobs in the repository

       Example Usage:

       >>> print dirac.getRepositoryJobs()
       {'OK': True, 'Value': [1,2,3,4]}

       :return S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn( "No repository is initialised" )
      return S_OK()
    jobs = self.jobRepo.readRepository()['Value']
    jobIDs = jobs.keys()
    if printOutput:
      print self.pPrint.pformat( jobIDs )
    return S_OK( jobIDs )

  def monitorRepository( self, printOutput = False ):
    """Monitor the jobs present in the repository

       Example Usage:

       >>> print dirac.monitorRepository()
       {'OK': True, 'Value': ''}

       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn( "No repository is initialised" )
      return S_OK()
    jobs = self.jobRepo.readRepository()['Value']
    jobIDs = jobs.keys()
    res = self.status( jobIDs )
    if not res['OK']:
      return self._errorReport( res['Message'], 'Failed to get status of jobs from WMS' )

    jobs = self.jobRepo.readRepository()['Value']
    statusDict = {}
    for jobDict in jobs.values():
      state = 'Unknown'
      if jobDict.has_key( 'State' ):
        state = jobDict['State']
      if not statusDict.has_key( state ):
        statusDict[state] = 0
      statusDict[state] += 1
    if printOutput:
      print self.pPrint.pformat( statusDict )
    return S_OK( statusDict )

  def retrieveRepositorySandboxes( self, requestedStates = None, destinationDirectory = '' ):
    """ Obtain the output sandbox for the jobs in requested states in the repository

       Example Usage:

       >>> print dirac.retrieveRepositorySandboxes(requestedStates=['Done','Failed'],destinationDirectory='sandboxes')
       {'OK': True, 'Value': ''}

       :param requestedStates: List of jobs states to be considered
       :type requestedStates: list of strings
       :param destinationDirectory: The target directory to place sandboxes (each jobID will have a directory created beneath this)
       :type destinationDirectory: string
       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn( "No repository is initialised" )
      return S_OK()
    if requestedStates == None:
      requestedStates = ['Done', 'Failed', 'Completed']  # because users dont care about completed
    jobs = self.jobRepo.readRepository()['Value']
    for jobID in sorted( jobs ):
      jobDict = jobs[jobID]
      if jobDict.has_key( 'State' ) and ( jobDict['State'] in requestedStates ):
        if ( jobDict.has_key( 'Retrieved' ) and ( not int( jobDict['Retrieved'] ) ) ) \
           or ( not jobDict.has_key( 'Retrieved' ) ):
          self.getOutputSandbox( jobID, destinationDirectory )
    return S_OK()

  def retrieveRepositoryData( self, requestedStates = None, destinationDirectory = '' ):
    """ Obtain the output data for the jobs in requested states in the repository

       Example Usage:

       >>> print dirac.retrieveRepositoryData(requestedStates=['Done'],destinationDirectory='outputData')
       {'OK': True, 'Value': ''}

       :param requestedStates: List of jobs states to be considered
       :type requestedStates: list of strings
       :param destinationDirectory: The target directory to place sandboxes (a directory is created for each JobID)
       :type destinationDirectory: string
       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn( "No repository is initialised" )
      return S_OK()
    if requestedStates == None:
      requestedStates = ['Done']
    jobs = self.jobRepo.readRepository()['Value']
    for jobID in sorted( jobs ):
      jobDict = jobs[jobID]
      if jobDict.has_key( 'State' ) and ( jobDict['State'] in requestedStates ):
        if ( jobDict.has_key( 'OutputData' ) and ( not int( jobDict['OutputData'] ) ) ) \
           or ( not jobDict.has_key( 'OutputData' ) ):
          destDir = jobID
          if destinationDirectory:
            destDir = "%s/%s" % ( destinationDirectory, jobID )
          self.getJobOutputData( jobID, destinationDir = destDir )
    return S_OK()

  def removeRepository( self ):
    """ Removes the job repository and all sandboxes and output data retrieved

       Example Usage:

       >>> print dirac.removeRepository()
       {'OK': True, 'Value': ''}

       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn( "No repository is initialised" )
      return S_OK()
    jobs = self.jobRepo.readRepository()['Value']
    for jobID in sorted( jobs ):
      jobDict = jobs[jobID]
      if jobDict.has_key( 'Sandbox' ) and os.path.exists( jobDict['Sandbox'] ):
        shutil.rmtree( jobDict['Sandbox'], ignore_errors = True )
      if jobDict.has_key( 'OutputFiles' ):
        for fileName in eval( jobDict['OutputFiles'] ):
          if os.path.exists( fileName ):
            os.remove( fileName )
    self.delete( sorted( jobs ) )
    os.remove( self.jobRepo.getLocation()['Value'] )
    self.jobRepo = False
    return S_OK()

  def resetRepository( self, jobIDs = None ):
    """ Reset all the status of the (optionally supplied) jobs in the repository

       Example Usage:

       >>> print dirac.resetRepository(jobIDs = [1111,2222,'3333'])
       {'OK': True, 'Value': ''}

       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn( "No repository is initialised" )
      return S_OK()
    if jobIDs == None:
      jobIDs = []
    if not type( jobIDs ) == types.ListType:
      return self._errorReport( 'The jobIDs must be a list of (strings or ints).' )
    self.jobRepo.resetRepository( jobIDs = jobIDs )
    return S_OK()

  #############################################################################
  def submit( self, job, mode = 'wms' ):
    """Submit jobs to DIRAC WMS.
       These can be either:

        - Instances of the Job Class
           - VO Application Jobs
           - Inline scripts
           - Scripts as executables
           - Scripts inside an application environment

        - JDL File
        - JDL String

       Example usage:

       >>> print dirac.submit(job)
       {'OK': True, 'Value': '12345'}

       :param job: Instance of Job class or JDL string
       :type job: Job() or string
       :param mode: Submit job locally with mode = 'wms' (default), 'local' to run workflow or 'agent' to run full Job Wrapper locally
       :type mode: string
       :returns: S_OK,S_ERROR
    """
    self.__printInfo()

    cleanPath = ''
    jobDescription = ''

    if type( job ) in types.StringTypes:
      if os.path.exists( job ):
        self.log.verbose( 'Found job JDL file %s' % ( job ) )
        jdl = job
      else:
        ( fd, jdl ) = tempfile.mkstemp( prefix = 'DIRAC_', suffix = '.jdl', text = True )
        self.log.verbose( 'Job is a JDL string' )
        os.write( fd, job )
        os.close( fd )
        cleanPath = jdl
    else:
      try:
        formulationErrors = job.errorDict
      except Exception, x:
        self.log.verbose( 'Could not obtain job errors:%s' % ( x ) )
        formulationErrors = {}

      if formulationErrors:
        for method, errorList in formulationErrors.items():
          self.log.error( '>>>> Error in %s() <<<<\n%s' % ( method, '\n'.join( errorList ) ) )
        return S_ERROR( formulationErrors )

      # Run any VO specific checks if desired prior to submission, this may or may not be overidden
      # in a derived class for example
      try:
        result = self.preSubmissionChecks( job, mode )
        if not result['OK']:
          self.log.error( 'Pre-submission checks failed for job with message: "%s"' % ( result['Message'] ) )
          return result
      except Exception, x:
        msg = 'Error in VO specific function preSubmissionChecks: "%s"' % ( x )
        self.log.error( msg )
        return S_ERROR( msg )

      tmpdir = tempfile.mkdtemp( prefix = 'DIRAC_' )
      self.log.verbose( 'Created temporary directory for submission %s' % ( tmpdir ) )

      jobDescription = tmpdir + '/jobDescription.xml'
      fd = os.open( jobDescription, os.O_RDWR | os.O_CREAT )
      os.write( fd, job._toXML() )
      os.close( fd )

      jdl = tmpdir + '/job.jdl'
      fd = os.open( jdl, os.O_RDWR | os.O_CREAT )
      os.write( fd, job._toJDL( xmlFile = jobDescription ) )
      os.close( fd )
      cleanPath = tmpdir

    if mode:
      if mode.lower() == 'local':
        self.log.info( 'Executing workflow locally without WMS submission' )
        curDir = os.getcwd()

        stopCopies = False
        if gConfig.getValue( '/LocalSite/DisableLocalJobDirectory', '' ):
          stopCopies = True
        else:
          jobDir = tempfile.mkdtemp( suffix = '_JobDir', prefix = 'Local_', dir = curDir )
          os.chdir( jobDir )

        stopCallback = False
        if gConfig.getValue( '/LocalSite/DisableLocalModeCallback', '' ):
          stopCallback = True

        self.log.info( 'Executing at', os.getcwd() )
        result = self.runLocal( jdl, jobDescription, curDir,
                                disableCopies = stopCopies,
                                disableCallback = stopCallback )
        os.chdir( curDir )
      if mode.lower() == 'agent':
        self.log.info( 'Executing workflow locally with full WMS submission and DIRAC Job Agent' )
        result = self.runLocalAgent( jdl )
      if mode.lower() == 'wms':
        self.log.verbose( 'Will submit job to WMS' )  # this will happen by default anyway
        result = self._sendJob( jdl )
        if not result['OK']:
          self.log.error( 'Job submission failure', result['Message'] )
        elif self.jobRepo:
          jobIDList = result['Value']
          if type( jobIDList ) != types.ListType:
            jobIDList = [ jobIDList ]
          for jobID in jobIDList:
            result = self.jobRepo.addJob( jobID, 'Submitted' )

    self.log.verbose( 'Cleaning up %s...' % cleanPath )
    self.__cleanTmp( cleanPath )
    return result

  #############################################################################
  def __cleanTmp( self, cleanPath ):
    """Remove tmp file or directory
    """
    if not cleanPath:
      return
    if os.path.isfile( cleanPath ):
      os.unlink( cleanPath )
      return
    if os.path.isdir( cleanPath ):
      shutil.rmtree( cleanPath, ignore_errors = True )
      return
    self.__printOutput( sys.stdout, 'Could not remove %s' % str( cleanPath ) )
    return

  #############################################################################
  def preSubmissionChecks( self, job, mode ):
    """Internal function.  The pre-submission checks method allows VOs to
       make their own checks before job submission. To make use of this the
       method should be overridden in a derived VO-specific Dirac class.
    """
    return S_OK( 'Nothing to do' )

  #############################################################################
  def runLocalAgent( self, jdl ):
    """Internal function.  This method is equivalent to submit(job,mode='Agent').
       All output files are written to a <jobID> directory where <jobID> is the
       result of submission to the WMS.  Please note that the job must be eligible to the
       site it is submitted from.
    """

    jdl = self.__forceLocal( jdl )

    jobID = self._sendJob( jdl )

    if not jobID['OK']:
      self.log.error( 'Job submission failure', jobID['Message'] )
      return S_ERROR( 'Could not submit job to WMS' )

    jobID = int( jobID['Value'] )
    self.log.info( 'The job has been submitted to the WMS with jobID = %s, monitoring starts.' % jobID )
    result = self.__monitorSubmittedJob( jobID )
    if not result['OK']:
      self.log.info( result['Message'] )
      return result

    self.log.info( 'Job %s is now eligible to be picked up from the WMS by a local job agent' % jobID )

    # now run job agent targetted to pick up this job
    result = self.__runJobAgent( jobID )

    return result

  @classmethod
  def __forceLocal( self, job ):
    """Update Job description to avoid pilot submission by WMS
    """
    if os.path.exists( job ):
      jdlFile = open( job, 'r' )
      jdl = jdlFile.read()
      jdlFile.close()
    else:
      jdl = job

    if not re.search( '\[', jdl ):
      jdl = '[' + jdl + ']'
    classAdJob = ClassAd( jdl )

    classAdJob.insertAttributeString( 'Site', DIRAC.siteName() )
    classAdJob.insertAttributeString( 'SubmitPools', 'Local' )
    classAdJob.insertAttributeString( 'PilotTypes', 'private' )

    return classAdJob.asJDL()

  #############################################################################
  def __runJobAgent( self, jobID ):
    """ This internal method runs a tailored job agent for the local execution
        of a previously submitted WMS job. The type of CEUniqueID can be overidden
        via the configuration.

        Currently must unset CMTPROJECTPATH to get this to work.
    """
    agentName = 'WorkloadManagement/JobAgent'
    self.log.verbose( 'In case being booted from a DIRAC script,'
                      ' now resetting sys arguments to null from: \n%s' % ( sys.argv ) )
    sys.argv = []
    localCfg = LocalConfiguration()
    ceType = gConfig.getValue( '/LocalSite/LocalCE', 'InProcess' )
    localCfg.addDefaultEntry( 'CEUniqueID', ceType )
    localCfg.addDefaultEntry( 'ControlDirectory', os.getcwd() )
    localCfg.addDefaultEntry( 'MaxCycles', 1 )
    localCfg.addDefaultEntry( '/LocalSite/WorkingDirectory', os.getcwd() )
    localCfg.addDefaultEntry( '/LocalSite/TotalCPUs', 1 )
    localCfg.addDefaultEntry( '/LocalSite/MaxCPUTime', 300000 )
    localCfg.addDefaultEntry( '/LocalSite/CPUTime', 300000 )
    localCfg.addDefaultEntry( '/LocalSite/OwnerGroup', self.__getCurrentGroup() )
    localCfg.addDefaultEntry( '/LocalSite/MaxRunningJobs', 1 )
    localCfg.addDefaultEntry( '/LocalSite/MaxTotalJobs', 1 )
#    if os.environ.has_key('VO_LHCB_SW_DIR'):
#      localCfg.addDefaultEntry('/LocalSite/SharedArea',os.environ['VO_LHCB_SW_DIR'])
    # Running twice in the same process, the second time it use the initial JobID.
    ( fd, jobidCfg ) = tempfile.mkstemp( '.cfg', 'DIRAC_JobId', text = True )
    os.write( fd, 'AgentJobRequirements\n {\n  JobID = %s\n }\n' % jobID )
    os.close( fd )
    gConfig.loadFile( jobidCfg )
    self.__cleanTmp( jobidCfg )
    localCfg.addDefaultEntry( '/AgentJobRequirements/PilotType', 'private' )
    ownerDN = self.__getCurrentDN()
    ownerGroup = self.__getCurrentGroup()
#    localCfg.addDefaultEntry('OwnerDN',ownerDN)
#    localCfg.addDefaultEntry('OwnerGroup',ownerGroup)
#    localCfg.addDefaultEntry('JobID',jobID)
    localCfg.addDefaultEntry( '/AgentJobRequirements/OwnerDN', ownerDN )
    localCfg.addDefaultEntry( '/AgentJobRequirements/OwnerGroup', ownerGroup )
    localCfg.addDefaultEntry( '/Resources/Computing/%s/PilotType' % ceType, 'private' )
    localCfg.addDefaultEntry( '/Resources/Computing/%s/OwnerDN' % ceType, ownerDN )
    localCfg.addDefaultEntry( '/Resources/Computing/%s/OwnerGroup' % ceType, ownerGroup )
    # localCfg.addDefaultEntry('/Resources/Computing/%s/JobID' %ceType,jobID)

    # SKP can add compatible platforms here
    localCfg.setConfigurationForAgent( agentName )
    result = localCfg.loadUserData()
    if not result[ 'OK' ]:
      self.log.error( 'There were errors when loading configuration', result['Message'] )
      return S_ERROR( 'Could not start DIRAC Job Agent' )

    agent = AgentReactor( agentName )
    result = agent.runNumCycles( agentName, numCycles = 1 )
    if not result['OK']:
      self.log.error( 'Job Agent execution completed with errors', result['Message'] )

    return result

  #############################################################################
  def __getCurrentGroup( self ):
    """Simple function to return current DIRAC group.
    """
    proxy = Locations.getProxyLocation()
    if not proxy:
      return S_ERROR( 'No proxy found in local environment' )
    else:
      self.log.verbose( 'Current proxy is %s' % proxy )

    chain = X509Chain()
    result = chain.loadProxyFromFile( proxy )
    if not result[ 'OK' ]:
      return result

    result = chain.getDIRACGroup()
    if not result[ 'OK' ]:
      return result
    group = result[ 'Value' ]
    self.log.verbose( 'Current group is %s' % group )
    return group

  #############################################################################
  def __getCurrentDN( self ):
    """Simple function to return current DN.
    """
    proxy = Locations.getProxyLocation()
    if not proxy:
      return S_ERROR( 'No proxy found in local environment' )
    else:
      self.log.verbose( 'Current proxy is %s' % proxy )

    chain = X509Chain()
    result = chain.loadProxyFromFile( proxy )
    if not result[ 'OK' ]:
      return result

    result = chain.getIssuerCert()
    if not result[ 'OK' ]:
      return result
    issuerCert = result[ 'Value' ]
    dn = issuerCert.getSubjectDN()[ 'Value' ]
    return dn

  #############################################################################
  def _runLocalJobAgent( self, jobID ):
    """Developer function.  In case something goes wrong with 'agent' submission, after
       successful WMS submission, this takes the jobID and allows to retry the job agent
       running.
    """

    result = self.__monitorSubmittedJob( jobID )
    if not result['OK']:
      self.log.info( result['Message'] )
      return result

    self.log.info( 'Job %s is now eligible to be picked up from the WMS by a local job agent' % jobID )
    # now run job agent targetted to pick up this job
    result = self.__runJobAgent( jobID )
    return result

  #############################################################################
  def __monitorSubmittedJob( self, jobID ):
    """Internal function.  Monitors a submitted job until it is eligible to be
       retrieved or enters a failed state.
    """
    pollingTime = 10  # seconds
    maxWaitingTime = 600  # seconds

    start = time.time()
    finalState = False
    while not finalState:
      jobStatus = self.status( jobID )
      self.log.verbose( jobStatus )
      if not jobStatus['OK']:
        self.log.error( 'Could not monitor job status, will retry in %s seconds' % pollingTime, jobStatus['Message'] )
      else:
        jobStatus = jobStatus['Value'][jobID]['Status']
        if jobStatus.lower() == 'waiting':
          finalState = True
          return S_OK( 'Job is eligible to be picked up' )
        if jobStatus.lower() == 'failed':
          finalState = True
          return S_ERROR( 'Problem with job %s definition, WMS status is Failed' % jobID )
        self.log.info( 'Current status for job %s is %s will retry in %s seconds' % ( jobID, jobStatus, pollingTime ) )
      current = time.time()
      if current - start > maxWaitingTime:
        finalState = True
        return S_ERROR( 'Exceeded max waiting time of %s seconds for job %s to enter Waiting state,'
                        ' exiting.' % ( maxWaitingTime, jobID ) )
      time.sleep( pollingTime )

  #############################################################################
  @classmethod
  def __getVOPolicyModule( self, module ):
    """ Utility to get the VO Policy module name
    """

    moduleName = ''
    setup = gConfig.getValue( '/DIRAC/Setup', '' )
    vo = None
    ret = getProxyInfo( disableVOMS = True )
    if ret['OK'] and 'group' in ret['Value']:
      vo = getVOForGroup( ret['Value']['group'] )
    if setup and vo:
      moduleName = gConfig.getValue( 'DIRAC/VOPolicy/%s/%s/%s' % ( vo, setup, module ), '' )
      if not moduleName:
        moduleName = gConfig.getValue( 'DIRAC/VOPolicy/%s' % module, '' )

    return moduleName

  #############################################################################
  def getInputDataCatalog( self, lfns, siteName = '', fileName = 'pool_xml_catalog.xml', ignoreMissing = False ):
    """This utility will create a pool xml catalogue slice for the specified LFNs using
       the full input data resolution policy plugins for the VO.

       If not specified the site is assumed to be the DIRAC.siteName() from the local
       configuration.  The fileName can be a full path.

       Example usage:

       >>> print print d.getInputDataCatalog('/lhcb/production/DC06/phys-v2-lumi5/00001680/DST/0000/00001680_00000490_5.dst',None,'myCat.xml')
       {'Successful': {'<LFN>': {'pfntype': 'ROOT_All', 'protocol': 'SRM2',
        'pfn': '<PFN>', 'turl': '<TURL>', 'guid': '3E3E097D-0AC0-DB11-9C0A-00188B770645',
        'se': 'CERN-disk'}}, 'Failed': [], 'OK': True, 'Value': ''}

       :param lfns: Logical File Name(s) to query
       :type lfns: LFN string or list []
       :param siteName: DIRAC site name
       :type siteName: string
       :param fileName: Catalogue name (can include path)
       :type fileName: string
       :returns: S_OK,S_ERROR

    """
    if type( lfns ) == type( " " ):
      lfns = [lfns.replace( 'LFN:', '' )]
    elif type( lfns ) == type( [] ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    if not siteName:
      siteName = DIRAC.siteName()

    if ignoreMissing:
      self.log.verbose( 'Ignore missing flag is enabled' )

    localSEList = getSEsForSite( siteName )
    if not localSEList['OK']:
      return localSEList

    self.log.verbose( localSEList )

    inputDataPolicy = self.__getVOPolicyModule( 'InputDataModule' )
    if not inputDataPolicy:
      return self._errorReport( 'Could not retrieve DIRAC/VOPolicy/InputDataModule for VO' )

    catalogFailed = {}
    self.log.info( 'Attempting to resolve data for %s' % siteName )
    self.log.verbose( '%s' % ( '\n'.join( lfns ) ) )
    replicaDict = self.getReplicas( lfns )
    if not replicaDict['OK']:
      return replicaDict
    if replicaDict['Value'].has_key( 'Failed' ):
      catalogFailed = replicaDict['Value']['Failed']

    guidDict = self.getMetadata( lfns )
    if not guidDict['OK']:
      return guidDict
    for lfn, reps in replicaDict['Value']['Successful'].items():
      guidDict['Value']['Successful'][lfn].update( reps )
    resolvedData = guidDict
    diskSE = gConfig.getValue( self.section + '/DiskSE', ['-disk', '-DST', '-USER', '-FREEZER'] )
    tapeSE = gConfig.getValue( self.section + '/TapeSE', ['-tape', '-RDST', '-RAW'] )
    # Add catalog path / name here as well as site name to override the standard policy of resolving automatically
    configDict = { 'JobID':None,
                   'LocalSEList':localSEList['Value'],
                   'DiskSEList':diskSE,
                   'TapeSEList':tapeSE,
                   'SiteName':siteName,
                   'CatalogName':fileName
                 }

    self.log.verbose( configDict )
    argumentsDict = {'FileCatalog':resolvedData, 'Configuration':configDict, 'InputData':lfns}
    if ignoreMissing:
      argumentsDict['IgnoreMissing'] = True
    self.log.verbose( argumentsDict )
    moduleFactory = ModuleFactory()
    self.log.verbose( 'Input Data Policy Module: %s' % inputDataPolicy )
    moduleInstance = moduleFactory.getModule( inputDataPolicy, argumentsDict )
    if not moduleInstance['OK']:
      self.log.warn( 'Could not create InputDataModule' )
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute()
    self.log.debug( result )
    if not result['OK']:
      if result.has_key( 'Failed' ):
        self.log.error( 'Input data resolution failed for the following files:\n', '\n'.join( result['Failed'] ) )

    if catalogFailed:
      self.log.error( 'Replicas not found for the following files:' )
      for key, value in catalogFailed.items():
        self.log.error( '%s %s' % ( key, value ) )
      if result.has_key( 'Failed' ):
        failedKeys = catalogFailed.keys()
        result['Failed'] = failedKeys

    return result

  #############################################################################
  def _runInputDataResolution( self, inputData, site = None ):
    """ Run the VO plugin input data resolution mechanism.
    """
    localSEList = gConfig.getValue( '/LocalSite/LocalSE', '' )
    if not localSEList:
      return self._errorReport( 'LocalSite/LocalSE should be defined in your config file' )
    if re.search( ',', localSEList ):
      localSEList = localSEList.replace( ' ', '' ).split( ',' )
    else:
      localSEList = [localSEList.replace( ' ', '' )]
    self.log.verbose( 'Local SEs:', localSEList )
    inputDataModule = self.__getVOPolicyModule( 'InputDataModule' )
    if not inputDataModule:
      return self._errorReport( 'Could not retrieve DIRAC/VOPolicy/InputDataModule for VO' )

    self.log.info( 'Job has input data requirement, will attempt to resolve data for %s' % DIRAC.siteName() )
    self.log.verbose( '\n'.join( inputData ) )
    replicaDict = self.getReplicas( inputData )
    if not replicaDict['OK']:
      return replicaDict
    catalogFailed = {}
    if replicaDict['Value'].has_key( 'Failed' ):
      catalogFailed = replicaDict['Value']['Failed']

    guidDict = self.getMetadata( inputData )
    if not guidDict['OK']:
      return guidDict
    for lfn, reps in replicaDict['Value']['Successful'].items():
      guidDict['Value']['Successful'][lfn].update( reps )
    resolvedData = guidDict
    diskSE = gConfig.getValue( self.section + '/DiskSE', ['-disk', '-DST', '-USER', '-FREEZER'] )
    tapeSE = gConfig.getValue( self.section + '/TapeSE', ['-tape', '-RDST', '-RAW'] )
    configDict = {'JobID':None, 'LocalSEList':localSEList, 'DiskSEList':diskSE, 'TapeSEList':tapeSE}
    self.log.debug( configDict )
    if site:
      configDict.update( {'SiteName':site} )
    argumentsDict = {'FileCatalog':resolvedData, 'Configuration':configDict, 'InputData':inputData}
    self.log.debug( argumentsDict )
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule( inputDataModule, argumentsDict )
    if not moduleInstance['OK']:
      self.log.warn( 'Could not create InputDataModule' )
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute()
    if not result['OK']:
      self.log.error( 'Input data resolution failed' )

    if catalogFailed:
      self.log.error( 'Replicas not found for the following files:' )
      for key, value in catalogFailed.items():
        self.log.error( '%s %s' % ( key, value ) )
      if result.has_key( 'Failed' ):
        failedKeys = catalogFailed.keys()
        result['Failed'] = failedKeys

    return result

  #############################################################################
  def runLocal( self, jobJDL, jobXML, baseDir, disableCopies = False, disableCallback = False ):
    """Internal function.  This method is equivalent to submit(job,mode='Local').
       All output files are written to the local directory.
    """
    # FIXME: Better create an unique local directory for this job
    # FIXME: This has to reviewed. Probably some of the things here are not needed at all

    if disableCopies:
      self.log.verbose( 'DisableLocalJobDirectory is set, leaving everything in local dir' )
      shutil.copy( jobXML, '%s/%s' % ( os.getcwd(), os.path.basename( jobXML ) ) )

    # If not set differently in the CS use the root from the current DIRAC installation
    siteRoot = gConfig.getValue( '/LocalSite/Root', DIRAC.rootPath )

    self.log.info( 'Preparing environment for site %s to execute job' % DIRAC.siteName() )

    os.environ['DIRACROOT'] = siteRoot
    self.log.verbose( 'DIRACROOT = %s' % ( siteRoot ) )
    os.environ['DIRACPYTHON'] = sys.executable
    self.log.verbose( 'DIRACPYTHON = %s' % ( sys.executable ) )
    self.log.verbose( 'JDL file is: %s' % jobJDL )
    self.log.verbose( 'Job XML file description is: %s' % jobXML )

    parameters = self.__getJDLParameters( jobJDL )
    if not parameters['OK']:
      self.log.warn( 'Could not extract job parameters from JDL file %s' % ( jobJDL ) )
      return parameters

    self.log.verbose( parameters )
    inputData = None
    if parameters['Value'].has_key( 'InputData' ):
      if parameters['Value']['InputData']:
        inputData = parameters['Value']['InputData']
        if type( inputData ) == type( " " ):
          inputData = [inputData]

    jobParamsDict = {'Job':parameters['Value']}

    if inputData:
      localSEList = gConfig.getValue( '/LocalSite/LocalSE', '' )
      if not localSEList:
        return self._errorReport( 'LocalSite/LocalSE should be defined in your config file' )
      if re.search( ',', localSEList ):
        localSEList = localSEList.replace( ' ', '' ).split( ',' )
      else:
        localSEList = [localSEList.replace( ' ', '' )]
      self.log.verbose( localSEList )
      inputDataPolicy = self.__getVOPolicyModule( 'InputDataModule' )
      if not inputDataPolicy:
        return self._errorReport( 'Could not retrieve DIRAC/VOPolicy/InputDataModule for VO' )

      self.log.info( 'Job has input data requirement, will attempt to resolve data for %s' % DIRAC.siteName() )
      self.log.verbose( '\n'.join( inputData ) )
      replicaDict = self.getReplicas( inputData )
      if not replicaDict['OK']:
        return replicaDict
      guidDict = self.getMetadata( inputData )
      if not guidDict['OK']:
        return guidDict
      for lfn, reps in replicaDict['Value']['Successful'].items():
        guidDict['Value']['Successful'][lfn].update( reps )
      resolvedData = guidDict
      diskSE = gConfig.getValue( self.section + '/DiskSE', ['-disk', '-DST', '-USER', '-FREEZER'] )
      tapeSE = gConfig.getValue( self.section + '/TapeSE', ['-tape', '-RDST', '-RAW'] )
      configDict = { 'JobID':        None,
                     'LocalSEList':  localSEList,
                     'DiskSEList':   diskSE,
                     'TapeSEList':   tapeSE
                   }
      self.log.verbose( configDict )
      argumentsDict = { 'FileCatalog':   resolvedData,
                        'Configuration': configDict,
                        'InputData':     inputData,
                        'Job':           parameters['Value']
                      }
      self.log.verbose( argumentsDict )
      moduleFactory = ModuleFactory()
      moduleInstance = moduleFactory.getModule( inputDataPolicy, argumentsDict )
      if not moduleInstance['OK']:
        self.log.warn( 'Could not create InputDataModule' )
        return moduleInstance

      module = moduleInstance['Value']
      result = module.execute()
      if not result['OK']:
        self.log.warn( 'Input data resolution failed' )
        return result

    softwarePolicy = self.__getVOPolicyModule( 'SoftwareDistModule' )
    if softwarePolicy:
      moduleFactory = ModuleFactory()
      moduleInstance = moduleFactory.getModule( softwarePolicy, jobParamsDict )
      if not moduleInstance['OK']:
        self.log.warn( 'Could not create SoftwareDistModule' )
        return moduleInstance

      module = moduleInstance['Value']
      result = module.execute()
      if not result['OK']:
        self.log.warn( 'Software installation failed with result:\n%s' % ( result ) )
        return result
    else:
      self.log.verbose( 'Could not retrieve DIRAC/VOPolicy/SoftwareDistModule for VO' )
      # return self._errorReport( 'Could not retrieve DIRAC/VOPolicy/SoftwareDistModule for VO' )

    if parameters['Value'].has_key( 'InputSandbox' ):
      sandbox = parameters['Value']['InputSandbox']
      if type( sandbox ) in types.StringTypes:
        sandbox = [sandbox]
      for isFile in sandbox:
        if disableCopies:
          break
        if not os.path.isabs( isFile ):
          # if a relative path, it is relative to the user working directory
          isFile = os.path.join( baseDir, isFile )

        # Attempt to copy into job working directory
        if os.path.isdir( isFile ):
          shutil.copytree( isFile, os.path.basename( isFile ), symlinks = True )
        elif os.path.exists( isFile ):
          shutil.copy( isFile, os.getcwd() )
        else:
          # perhaps the file is in an LFN attempt to download it.
          getFile = self.getFile( isFile )
          if not getFile['OK']:
            self.log.warn( 'Failed to download %s with error:%s' % ( isFile, getFile['Message'] ) )
            return S_ERROR( 'Can not copy InputSandbox file %s' % isFile )
        basefname = os.path.basename( isFile )
        try:
          if tarfile.is_tarfile( basefname ):
            tarFile = tarfile.open( basefname, 'r' )
            for member in tarFile.getmembers():
              tarFile.extract( member, os.getcwd() )
        except Exception, x :
          return S_ERROR( 'Could not untar %s with exception %s' % ( basefname, str( x ) ) )

    self.log.info( 'Attempting to submit job to local site: %s' % DIRAC.siteName() )

    if parameters['Value'].has_key( 'Executable' ):
      executable = os.path.expandvars( parameters['Value']['Executable'] )
    else:
      return self._errorReport( 'Missing job "Executable"' )

    arguments = ''
    if parameters['Value'].has_key( 'Arguments' ):
      arguments = parameters['Value']['Arguments']

    command = '%s %s' % ( executable, arguments )

    self.log.info( 'Executing: %s' % command )
    executionEnv = dict( os.environ )
    if parameters['Value'].has_key( 'ExecutionEnvironment' ):
      self.log.verbose( 'Adding variables to execution environment' )
      variableList = parameters['Value']['ExecutionEnvironment']
      if type( variableList ) == type( " " ):
        variableList = [variableList]
      for var in variableList:
        nameEnv = var.split( '=' )[0]
        valEnv = urllib.unquote( var.split( '=' )[1] )  # this is needed to make the value contain strange things
        executionEnv[nameEnv] = valEnv
        self.log.verbose( '%s = %s' % ( nameEnv, valEnv ) )

    cbFunction = self.__printOutput
    if disableCallback:
      cbFunction = None

    result = shellCall( 0, command, env = executionEnv, callbackFunction = cbFunction )
    if not result['OK']:
      return result

    status = result['Value'][0]
    self.log.verbose( 'Status after execution is %s' % ( status ) )

    outputFileName = None
    errorFileName = None
    # FIXME: if there is an callbackFunction, StdOutput and StdError will be empty soon
    if parameters['Value'].has_key( 'StdOutput' ):
      outputFileName = parameters['Value']['StdOutput']
    if parameters['Value'].has_key( 'StdError' ):
      errorFileName = parameters['Value']['StdError']

    if outputFileName:
      stdout = result['Value'][1]
      if os.path.exists( outputFileName ):
        os.remove( outputFileName )
      self.log.info( 'Standard output written to %s' % ( outputFileName ) )
      outputFile = open( outputFileName, 'w' )
      print >> outputFile, stdout
      outputFile.close()
    else:
      self.log.warn( 'Job JDL has no StdOutput file parameter defined' )

    if errorFileName:
      stderr = result['Value'][2]
      if os.path.exists( errorFileName ):
        os.remove( errorFileName )
      self.log.verbose( 'Standard error written to %s' % ( errorFileName ) )
      errorFile = open( errorFileName, 'w' )
      print >> errorFile, stderr
      errorFile.close()
    else:
      self.log.warn( 'Job JDL has no StdError file parameter defined' )

      if parameters['Value'].has_key( 'OutputSandbox' ):
        sandbox = parameters['Value']['OutputSandbox']
        if type( sandbox ) in types.StringTypes:
          sandbox = [sandbox]

    if parameters['Value'].has_key( 'OutputSandbox' ):
      sandbox = parameters['Value']['OutputSandbox']
      if type( sandbox ) in types.StringTypes:
        sandbox = [sandbox]
      for i in sandbox:
        if disableCopies:
          break
        globList = glob.glob( i )
        for isFile in globList:
          if os.path.isabs( isFile ):
            # if a relative path, it is relative to the user working directory
            isFile = os.path.basename( isFile )
          # Attempt to copy back from job working directory
          if os.path.isdir( isFile ):
            shutil.copytree( isFile, baseDir, symlinks = True )
          elif os.path.exists( isFile ):
            shutil.copy( isFile, baseDir )
          else:
            return S_ERROR( 'Can not copy OutputSandbox file %s' % isFile )

    if status:
      return S_ERROR( 'Execution completed with non-zero status %s' % ( status ) )
    return S_OK( 'Execution completed successfully' )

  #############################################################################
  @classmethod
  def __printOutput( self, fd = None, message = '' ):
    """Internal callback function to return standard output when running locally.
    """
    if fd:
      if type( fd ) == types.IntType:
        if fd == 0:
          print >> sys.stdout, message
        elif fd == 1:
          print >> sys.stderr, message
        else:
          print message
      elif type( fd ) == types.FileType:
        print >> fd, message
    else:
      print message

  #############################################################################
  # def listCatalog( self, directory, printOutput = False ):
  #   """ Under development.
  #       Obtain listing of the specified directory.
  #   """
  #   rm = ReplicaManager()
  #   listing = rm.listCatalogDirectory( directory )
  #   if re.search( '\/$', directory ):
  #     directory = directory[:-1]
  #
  #   if printOutput:
  #     for fileKey, metaDict in listing['Value']['Successful'][directory]['Files'].items():
  #       print '#' * len( fileKey )
  #       print fileKey
  #       print '#' * len( fileKey )
  #       print self.pPrint.pformat( metaDict )

  #############################################################################
  def getReplicas( self, lfns, active = True, printOutput = False ):
    """Obtain replica information from file catalogue client. Input LFN(s) can be string or list.

       Example usage:

       >>> print dirac.getReplicas('/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst')
       {'OK': True, 'Value': {'Successful': {'/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst':
       {'CERN-RDST':
       'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst'}},
       'Failed': {}}}

       :param lfns: Logical File Name(s) to query
       :type lfns: LFN string or list []
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( lfns ) == type( " " ):
      lfns = lfns.replace( 'LFN:', '' )
    elif type( lfns ) == type( [] ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    start = time.time()
    dm = DataManager()
    if active:
      repsResult = dm.getActiveReplicas( lfns )
    else:
      repsResult = dm.getReplicas( lfns )
    timing = time.time() - start
    self.log.info( 'Replica Lookup Time: %.2f seconds ' % ( timing ) )
    self.log.debug( repsResult )
    if not repsResult['OK']:
      self.log.warn( repsResult['Message'] )
      return repsResult

    if printOutput:
      print self.pPrint.pformat( repsResult['Value'] )

    return repsResult

  #############################################################################
  def getAllReplicas( self, lfns, printOutput = False ):
    """Only differs from getReplicas method in the sense that replicas on banned SEs
       will be included in the result.

       Obtain replica information from file catalogue client. Input LFN(s) can be string or list.

       Example usage:

       >>> print dirac.getAllReplicas('/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst')
       {'OK': True, 'Value': {'Successful': {'/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst':
       {'CERN-RDST':
       'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst'}},
       'Failed': {}}}

       :param lfns: Logical File Name(s) to query
       :type lfns: LFN string or list []
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( lfns ) == type( " " ):
      lfns = lfns.replace( 'LFN:', '' )
    elif type( lfns ) == type( [] ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

#     rm = ReplicaManager()
#     start = time.time()
#     repsResult = rm.getCatalogReplicas( lfns )
    # RF_NOTE : this method will return different values that api.getReplicas
    fc = FileCatalog()
    start = time.time()
    repsResult = fc.getReplicas( lfns )

    timing = time.time() - start
    self.log.info( 'Replica Lookup Time: %.2f seconds ' % ( timing ) )
    self.log.verbose( repsResult )
    if not repsResult['OK']:
      self.log.warn( repsResult['Message'] )
      return repsResult

    if printOutput:
      print self.pPrint.pformat( repsResult['Value'] )

    return repsResult

  #############################################################################
  def splitInputData( self, lfns, maxFilesPerJob = 20, printOutput = False ):
    """Split the supplied lfn list by the replicas present at the possible
       destination sites.  An S_OK object will be returned containing a list of
       lists in order to create the jobs.

       Example usage:

       >>> d.splitInputData(lfns,10)
       {'OK': True, 'Value': [['<LFN>'], ['<LFN>']]}


       :param lfns: Logical File Name(s) to split
       :type lfns: list
       :param maxFilesPerJob: Number of files per bunch
       :type maxFilesPerJob: integer
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
    sitesForSE = {}
    if type( lfns ) == type( " " ):
      lfns = lfns.replace( 'LFN:', '' )
    elif type( lfns ) == type( [] ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    if not type( maxFilesPerJob ) == types.IntType:
      try:
        maxFilesPerJob = int( maxFilesPerJob )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer for maxFilesPerJob' )

    replicaDict = self.getReplicas( lfns, active = True )
    if not replicaDict['OK']:
      return replicaDict
    if len( replicaDict['Value']['Successful'] ) == 0:
      return self._errorReport( replicaDict['Value']['Failed'].items()[0], 'Failed to get replica information' )
    siteLfns = {}
    for lfn, reps in replicaDict['Value']['Successful'].items():
      possibleSites = set( [site for se in reps for site in ( sitesForSE[se] if se in sitesForSE else  sitesForSE.setdefault( se, getSitesForSE( se ).get( 'Value', [] ) ) )] )
      siteLfns.setdefault( ','.join( sorted( possibleSites ) ), [] ).append( lfn )

    if '' in siteLfns:
      # Some files don't have active replicas
      return self._errorReport( 'No active replica found for', str( siteLfns[''] ) )
    lfnGroups = []
    for files in siteLfns.values():
      lists = breakListIntoChunks( files, maxFilesPerJob )
      lfnGroups += lists

    if printOutput:
      print self.pPrint.pformat( lfnGroups )
    return S_OK( lfnGroups )

  #############################################################################
  def getMetadata( self, lfns, printOutput = False ):
    """Obtain replica metadata from file catalogue client. Input LFN(s) can be string or list.

       Example usage:

       >>> print dirac.getMetadata('/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst')
       {'OK': True, 'Value': {'Successful': {'/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst':
       {'Status': '-', 'Size': 619475828L, 'GUID': 'E871FBA6-71EA-DC11-8F0C-000E0C4DEB4B', 'CheckSumType': 'AD',
       'CheckSumValue': ''}}, 'Failed': {}}}

       :param lfns: Logical File Name(s) to query
       :type lfns: LFN string or list []
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( lfns ) == type( " " ):
      lfns = lfns.replace( 'LFN:', '' )
    elif type( lfns ) == type( [] ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    fc = FileCatalog()
    start = time.time()
    repsResult = fc.getFileMetadata( lfns )
    timing = time.time() - start
    self.log.info( 'Metadata Lookup Time: %.2f seconds ' % ( timing ) )
    self.log.verbose( repsResult )
    if not repsResult['OK']:
      self.log.warn( 'Failed to retrieve file metadata from the catalogue' )
      self.log.warn( repsResult['Message'] )
      return repsResult

    if printOutput:
      print self.pPrint.pformat( repsResult['Value'] )

    return repsResult

  #############################################################################
  def addFile( self, lfn, fullPath, diracSE, fileGuid = None, printOutput = False ):
    """Add a single file to Grid storage. lfn is the desired logical file name
       for the file, fullPath is the local path to the file and diracSE is the
       Storage Element name for the upload.  The fileGuid is optional, if not
       specified a GUID will be generated on the fly.  If subsequent access
       depends on the file GUID the correct one should

       Example Usage:

       >>> print dirac.addFile('/lhcb/user/p/paterson/myFile.tar.gz','myFile.tar.gz','CERN-USER')
       {'OK': True, 'Value':{'Failed': {},
        'Successful': {'/lhcb/user/p/paterson/test/myFile.tar.gz': {'put': 64.246301889419556,
                                                                    'register': 1.1102778911590576}}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param diracSE: DIRAC SE name e.g. CERN-USER
       :type diracSE: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( lfn ) == type( " " ):
      lfn = lfn.replace( 'LFN:', '' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    if not os.path.exists( fullPath ):
      return self._errorReport( 'File path %s must exist' % ( fullPath ) )

    if not os.path.isfile( fullPath ):
      return self._errorReport( 'Expected path to file not %s' % ( fullPath ) )

    dm = DataManager( catalogs = self.defaultFileCatalog )
    result = dm.putAndRegister( lfn, fullPath, diracSE, guid = fileGuid )
    if not result['OK']:
      return self._errorReport( 'Problem during putAndRegister call', result['Message'] )
    if not printOutput:
      return result

    print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def getFile( self, lfn, destDir = '', printOutput = False ):
    """Retrieve a single file or list of files from Grid storage to the current directory. lfn is the
       desired logical file name for the file, fullPath is the local path to the file and diracSE is the
       Storage Element name for the upload.  The fileGuid is optional, if not specified a GUID will be
       generated on the fly.

       Example Usage:

       >>> print dirac.getFile('/lhcb/user/p/paterson/myFile.tar.gz')
       {'OK': True, 'Value':{'Failed': {},
        'Successful': {'/lhcb/user/p/paterson/test/myFile.tar.gz': '/afs/cern.ch/user/p/paterson/w1/DIRAC3/myFile.tar.gz'}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( lfn ) == type( " " ):
      lfn = lfn.replace( 'LFN:', '' )
    elif type( lfn ) == type( [] ):
      try:
        lfn = [str( lfnName.replace( 'LFN:', '' ) ) for lfnName in lfn]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected strings for LFN(s)' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    dm = DataManager()
    result = dm.getFile( lfn, destinationDir = destDir )
    if not result['OK']:
      return self._errorReport( 'Problem during getFile call', result['Message'] )

    if result['Value']['Failed']:
      self.log.error( 'Failures occurred during rm.getFile' )
      if printOutput:
        print self.pPrint.pformat( result['Value'] )
      return S_ERROR( result['Value'] )

    if not printOutput:
      return result

    print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def replicateFile( self, lfn, destinationSE, sourceSE = '', localCache = '', printOutput = False ):
    """Replicate an existing file to another Grid SE. lfn is the desired logical file name
       for the file to be replicated, destinationSE is the DIRAC Storage Element to create a
       replica of the file at.  Optionally the source storage element and local cache for storing
       the retrieved file for the new upload can be specified.

       Example Usage:

       >>> print dirac.replicateFile('/lhcb/user/p/paterson/myFile.tar.gz','CNAF-USER')
       {'OK': True, 'Value':{'Failed': {},
       'Successful': {'/lhcb/user/p/paterson/test/myFile.tar.gz': {'register': 0.44766902923583984,
                                                                  'replicate': 56.42345404624939}}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param destinationSE: Destination DIRAC SE name e.g. CERN-USER
       :type destinationSE: string
       :param sourceSE: Optional source SE
       :type sourceSE: string
       :param localCache: Optional path to local cache
       :type localCache: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( lfn ) in types.StringTypes:
      lfn = lfn.replace( 'LFN:', '' )
    elif type( lfn ) != types.ListType:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    if not sourceSE:
      sourceSE = ''
    if not localCache:
      localCache = ''
    if not type( sourceSE ) in types.StringTypes:
      return self._errorReport( 'Expected string for source SE name' )
    if not type( localCache ) == type( " " ):
      return self._errorReport( 'Expected string for path to local cache' )

    dm = DataManager()
    result = dm.replicateAndRegister( lfn, destinationSE, sourceSE, '', localCache )
    if not result['OK']:
      return self._errorReport( 'Problem during replicateFile call', result['Message'] )
    if not printOutput:
      return result

    print self.pPrint.pformat( result['Value'] )
    return result

  def replicate( self, lfn, destinationSE, sourceSE = '', printOutput = False ):
    """Replicate an existing file to another Grid SE. lfn is the desired logical file name
       for the file to be replicated, destinationSE is the DIRAC Storage Element to create a
       replica of the file at.  Optionally the source storage element and local cache for storing
       the retrieved file for the new upload can be specified.

       Example Usage:

       >>> print dirac.replicate('/lhcb/user/p/paterson/myFile.tar.gz','CNAF-USER')
       {'OK': True, 'Value':{'Failed': {},
       'Successful': {'/lhcb/user/p/paterson/test/myFile.tar.gz': {'register': 0.44766902923583984}}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param destinationSE: Destination DIRAC SE name e.g. CERN-USER
       :type destinationSE: string
       :param sourceSE: Optional source SE
       :type sourceSE: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( lfn ) == type( " " ):
      lfn = lfn.replace( 'LFN:', '' )
    else:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    if not sourceSE:
      sourceSE = ''

    if not type( sourceSE ) == type( " " ):
      return self._errorReport( 'Expected string for source SE name' )

    dm = DataManager()
    result = dm.replicate( lfn, destinationSE, sourceSE, '' )
    if not result['OK']:
      return self._errorReport( 'Problem during replicate call', result['Message'] )
    if not printOutput:
      return result

    print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def getAccessURL( self, lfn, storageElement, printOutput = False ):
    """Allows to retrieve an access URL for an LFN replica given a valid DIRAC SE
       name.  Contacts the file catalog and contacts the site SRM endpoint behind
       the scenes.

       Example Usage:

       >>> print dirac.getAccessURL('/lhcb/data/CCRC08/DST/00000151/0000/00000151_00004848_2.dst','CERN-RAW')
       {'OK': True, 'Value': {'Successful': {'srm://...': {'SRM2': 'rfio://...'}}, 'Failed': {}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: string or list
       :param storageElement: DIRAC SE name e.g. CERN-RAW
       :type storageElement: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( lfn ) == type( " " ):
      lfn = lfn.replace( 'LFN:', '' )
    else:
      return self._errorReport( 'Expected single string for LFN' )

    dm = DataManager()
    result = dm.getReplicaAccessUrl( [lfn], storageElement )
    if not result['OK']:
      return self._errorReport( 'Problem during getAccessURL call', result['Message'] )
    if not printOutput:
      return result

    print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def getPhysicalFileAccessURL( self, pfn, storageElement, printOutput = False ):
    """Allows to retrieve an access URL for an PFN  given a valid DIRAC SE
       name.  The SE is contacted directly for this information.

       Example Usage:

       >>> print dirac.getPhysicalFileAccessURL('srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/CCRC08/DST/00000151/0000/00000151_00004848_2.dst','CERN_M-DST')
       {'OK': True, 'Value':{'Failed': {},
       'Successful': {'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/CCRC08/DST/00000151/0000/00000151_00004848_2.dst': {'RFIO': 'castor://...'}}}}

       :param pfn: Physical File Name (PFN)
       :type pfn: string or list
       :param storageElement: DIRAC SE name e.g. CERN-RAW
       :type storageElement: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( pfn ) == type( " " ):
      if re.search( 'LFN:', pfn ):
        return self._errorReport( 'Expected PFN not LFN' )
      pfn = pfn.replace( 'PFN:', '' )
    elif type( pfn ) == type( [] ):
      try:
        pfn = [str( pfnName.replace( 'PFN:', '' ) ) for pfnName in pfn]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected strings for PFN(s)' )
    else:
      return self._errorReport( 'Expected single string for PFN' )

    result = StorageElement( storageElement ).getAccessUrl( [pfn] )
    if not result['OK']:
      return self._errorReport( 'Problem during getAccessURL call', result['Message'] )
    if not printOutput:
      return result

    print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def getPhysicalFileMetadata( self, pfn, storageElement, printOutput = False ):
    """Allows to retrieve metadata for physical file(s) on a supplied storage
       element.  Contacts the site SRM endpoint and performs a gfal_ls behind
       the scenes.

       Example Usage:

       >>> print dirac.getPhysicalFileMetadata('srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data
       /lhcb/data/CCRC08/RAW/LHCb/CCRC/23341/023341_0000039571.raw','NIKHEF-RAW')
       {'OK': True, 'Value': {'Successful': {'srm://...': {'SRM2': 'rfio://...'}}, 'Failed': {}}}

       :param pfn: Physical File Name (PFN)
       :type pfn: string or list
       :param storageElement: DIRAC SE name e.g. CERN-RAW
       :type storageElement: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( pfn ) == type( " " ):
      if re.search( 'LFN:', pfn ):
        return self._errorReport( 'Expected PFN not LFN' )
      pfn = pfn.replace( 'PFN:', '' )
      pfn = [pfn]
    elif type( pfn ) == type( [] ):
      try:
        pfn = [str( pfile.replace( 'PFN:', '' ) ) for pfile in pfn]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected list of strings for PFNs' )
    else:
      return self._errorReport( 'Expected single string or list of strings for PFN(s)' )

    result = StorageElement( storageElement ).getFileMetadata( pfn )
    if not result['OK']:
      return self._errorReport( 'Problem during getStorageFileMetadata call', result['Message'] )
    if not printOutput:
      return result

    print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def removeFile( self, lfn, printOutput = False ):
    """Remove LFN and *all* associated replicas from Grid Storage Elements and
       file catalogues.

       Example Usage:

       >>> print dirac.removeFile('LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/22808/022808_0000018443.raw')
       {'OK': True, 'Value':...}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR

    """
    if type( lfn ) in types.StringTypes:
      lfn = lfn.replace( 'LFN:', '' )
    elif type( lfn ) != types.ListType:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    dm = DataManager()
    result = dm.removeFile( lfn )
    if printOutput and result['OK']:
      print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def removeReplica( self, lfn, storageElement, printOutput = False ):
    """Remove replica of LFN from specified Grid Storage Element and
       file catalogues.

       Example Usage:

       >>> print dirac.removeReplica('LFN:/lhcb/user/p/paterson/myDST.dst','CERN-USER')
       {'OK': True, 'Value':...}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param storageElement: DIRAC SE Name
       :type storageElement: string
       :returns: S_OK,S_ERROR
    """
    if type( lfn ) in types.StringTypes:
      lfn = lfn.replace( 'LFN:', '' )
    elif type( lfn ) != types.ListType:
      return self._errorReport( 'Expected single string or list of strings for LFN(s)' )

    dm = DataManager()
    result = dm.removeReplica( storageElement, lfn )
    if printOutput and result['OK']:
      print self.pPrint.pformat( result['Value'] )
    return result

  #############################################################################
  def dataLoggingInfo( self, lfn, printOutput = False ):
    """Retrieve logging information for a given dataset.

       Example Usage:

       >>> print dirac.dataLoggingInfo('/lhcb/data/CCRC08/RAW/LHCb/CCRC/22808/022808_0000018443.raw')
       {'OK': True, 'Value': [('AddedToTransformation', 'Transformation 3', datetime.datetime(2008, 5, 18, 13, 54, 15)]}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    if type( lfn ) == type( " " ):
      lfn = lfn.replace( 'LFN:', '' )
    else:
      return self._errorReport( 'Expected single string for LFN' )

    dataLogging = RPCClient( 'DataManagement/DataLogging' )
    result = dataLogging.getFileLoggingInfo( lfn )
    if not result['OK']:
      return self._errorReport( 'Problem during getFileLoggingInfo call', result['Message'] )
    if not printOutput:
      return result

    loggingTupleList = result['Value']
    headers = ( 'Status', 'MinorStatus', 'DateTime', 'Source' )
    line = ''

    statAdj = 0
    mStatAdj = 0
    dtAdj = 25
    sourceAdj = 0

    for i in loggingTupleList:
      if len( str( i[0] ) ) > statAdj:
        statAdj = len( str( i[0] ) ) + 4
      if len( str( i[1] ) ) > mStatAdj:
        mStatAdj = len( str( i[1] ) ) + 4
      if len( str( i[3] ) ) > sourceAdj:
        sourceAdj = len( str( i[3] ) ) + 4

    print '\n' + headers[0].ljust( statAdj ) + headers[1].ljust( mStatAdj ) + \
                 headers[2].ljust( dtAdj ) + headers[3].ljust( sourceAdj ) + '\n'
    for i in loggingTupleList:
      line = i[0].ljust( statAdj ) + i[1].ljust( mStatAdj ) + \
             toString( i[2] ).ljust( dtAdj ) + i[3].ljust( sourceAdj )
      print line

    return result

  #############################################################################
  def _sendJob( self, jdl ):
    """Internal function.

       This is an internal wrapper for submit() in order to
       catch whether a user is authorized to submit to DIRAC or
       does not have a valid proxy. This is not intended for
       direct use.

    """
    jobID = None
    if gConfig.getValue( '/LocalSite/DisableSubmission', '' ):
      return S_ERROR( 'Submission disabled by /LocalSite/DisableSubmission flag for debugging purposes' )

    try:
      jobID = WMSClient().submitJob( jdl )
      # raise 'problem'
    except Exception, x:
      return S_ERROR( "Cannot submit job: %s" % str( x ) )

    return jobID

  #############################################################################
  def getInputSandbox( self, jobID, outputDir = None ):
    """Retrieve input sandbox for existing JobID.

       This method allows the retrieval of an existing job input sandbox for
       debugging purposes.  By default the sandbox is downloaded to the current
       directory but this can be overidden via the outputDir parameter. All files
       are extracted into a InputSandbox<JOBID> directory that is automatically created.

       Example Usage:

       >>> print dirac.getInputSandbox(12345)
       {'OK': True, 'Value': ['Job__Sandbox__.tar.bz2']}

       :param jobID: JobID
       :type jobID: integer or string
       :param outputDir: Optional directory for files
       :type outputDir: string
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    # TODO: Do not check if dir already exists
    dirPath = ''
    if outputDir:
      dirPath = '%s/InputSandbox%s' % ( outputDir, jobID )
      if os.path.exists( dirPath ):
        return self._errorReport( 'Job input sandbox directory %s already exists' % ( dirPath ) )
    else:
      dirPath = '%s/InputSandbox%s' % ( os.getcwd(), jobID )
      if os.path.exists( dirPath ):
        return self._errorReport( 'Job input sandbox directory %s already exists' % ( dirPath ) )

    try:
      os.mkdir( dirPath )
    except Exception, x:
      return self._errorReport( str( x ), 'Could not create directory in %s' % ( dirPath ) )

    result = SandboxStoreClient( useCertificates = self.useCertificates ).downloadSandboxForJob( jobID, 'Input', dirPath )
    if not result[ 'OK' ]:
      self.log.warn( result[ 'Message' ] )
    else:
      self.log.info( 'Files retrieved and extracted in %s' % ( dirPath ) )
    return result

  #############################################################################
  def getOutputSandbox( self, jobID, outputDir = None, oversized = True, noJobDir = False ):
    """Retrieve output sandbox for existing JobID.

       This method allows the retrieval of an existing job output sandbox.
       By default the sandbox is downloaded to the current directory but
       this can be overidden via the outputDir parameter. All files are
       extracted into a <JOBID> directory that is automatically created.

       Example Usage:

       >>> print dirac.getOutputSandbox(12345)
       {'OK': True, 'Value': ['Job__Sandbox__.tar.bz2']}

       :param jobID: JobID
       :type jobID: integer or string
       :param outputDir: Optional directory path
       :type outputDir: string
       :param oversized: Optionally disable oversized sandbox download
       :type oversized: boolean
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    # TODO: Do not check if dir already exists
    dirPath = ''
    if outputDir:
      dirPath = outputDir
      if not noJobDir:
        dirPath = '%s/%s' % ( outputDir, jobID )
      # if os.path.exists( dirPath ):
      #  return self._errorReport( 'Job output directory %s already exists' % ( dirPath ) )
    else:
      dirPath = '%s/%s' % ( os.getcwd(), jobID )
      if os.path.exists( dirPath ):
        return self._errorReport( 'Job output directory %s already exists' % ( dirPath ) )

    try:
      if not os.path.exists( dirPath ):
        os.makedirs( dirPath )
    except Exception, x:
      return self._errorReport( str( x ), 'Could not create directory in %s' % ( dirPath ) )

    # New download
    result = SandboxStoreClient( useCertificates = self.useCertificates ).downloadSandboxForJob( jobID, 'Output', dirPath )
    if result['OK']:
      self.log.info( 'Files retrieved and extracted in %s' % ( dirPath ) )
      if self.jobRepo:
        self.jobRepo.updateJob( jobID, {'Retrieved':1, 'Sandbox':os.path.realpath( dirPath )} )
      return result
    self.log.warn( result[ 'Message' ] )

    if not oversized:
      if self.jobRepo:
        self.jobRepo.updateJob( jobID, {'Retrieved':1, 'Sandbox':os.path.realpath( dirPath )} )
      return result

    params = self.parameters( int( jobID ) )
    if not params['OK']:
      self.log.verbose( 'Could not retrieve job parameters to check for oversized sandbox' )
      return params

    if not params['Value'].has_key( 'OutputSandboxLFN' ):
      self.log.verbose( 'No oversized output sandbox for job %s:\n%s' % ( jobID, params ) )
      return result

    oversizedSandbox = params['Value']['OutputSandboxLFN']
    if not oversizedSandbox:
      self.log.verbose( 'Null OutputSandboxLFN for job %s' % jobID )
      return result

    self.log.info( 'Attempting to retrieve %s' % oversizedSandbox )
    start = os.getcwd()
    os.chdir( dirPath )
    getFile = self.getFile( oversizedSandbox )
    if not getFile['OK']:
      self.log.warn( 'Failed to download %s with error:%s' % ( oversizedSandbox, getFile['Message'] ) )
      os.chdir( start )
      return getFile

    fileName = os.path.basename( oversizedSandbox )
    try:
      result = S_OK()
      if tarfile.is_tarfile( fileName ):
        tarFile = tarfile.open( fileName, 'r' )
        for member in tarFile.getmembers():
          tarFile.extract( member, dirPath )
    except Exception, x :
      os.chdir( start )
      result = S_ERROR( str( x ) )

    if os.path.exists( fileName ):
      os.unlink( fileName )

    os.chdir( start )
    if result['OK']:
      if self.jobRepo:
        self.jobRepo.updateJob( jobID, {'Retrieved':1, 'Sandbox':os.path.realpath( dirPath )} )
    return result

  #############################################################################
  def delete( self, jobID ):
    """Delete job or list of jobs from the WMS, if running these jobs will
       also be killed.

       Example Usage:

       >>> print dirac.delete(12345)
       {'OK': True, 'Value': [12345]}

       :param jobID: JobID
       :type jobID: int, string or list
       :returns: S_OK,S_ERROR

    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      try:
        jobID = [int( job ) for job in jobID]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    result = WMSClient().deleteJob( jobID )
    if result['OK']:
      if self.jobRepo:
        for jobID in result['Value']:
          self.jobRepo.removeJob( jobID )
    return result

  #############################################################################
  def reschedule( self, jobID ):
    """Reschedule a job or list of jobs in the WMS.  This operation is the same
       as resubmitting the same job as new.  The rescheduling operation may be
       performed to a configurable maximum number of times but the owner of a job
       can also reset this counter and reschedule jobs again by hand.

       Example Usage:

       >>> print dirac.reschedule(12345)
       {'OK': True, 'Value': [12345]}

       :param jobID: JobID
       :type jobID: int, string or list
       :returns: S_OK,S_ERROR

    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      try:
        jobID = [int( job ) for job in jobID]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    result = WMSClient().rescheduleJob( jobID )
    if result['OK']:
      if self.jobRepo:
        repoDict = {}
        for jobID in result['Value']:
          repoDict[jobID] = {'State':'Submitted'}
        self.jobRepo.updateJobs( repoDict )
    return result

  def kill( self, jobID ):
    """Issue a kill signal to a running job.  If a job has already completed this
       action is harmless but otherwise the process will be killed on the compute
       resource by the Watchdog.

       Example Usage:

        >>> print dirac.kill(12345)
        {'OK': True, 'Value': [12345]}

       :param jobID: JobID
       :type jobID: int, string or list
       :returns: S_OK,S_ERROR

    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      try:
        jobID = [int( job ) for job in jobID]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    result = WMSClient().killJob( jobID )
    if result['OK']:
      if self.jobRepo:
        for jobID in result['Value']:
          self.jobRepo.removeJob( jobID )
    return result

  #############################################################################
  def status( self, jobID ):
    """Monitor the status of DIRAC Jobs.

       Example Usage:

       >>> print dirac.status(79241)
       {79241: {'status': 'Done', 'site': 'LCG.CERN.ch'}}

       :param jobID: JobID
       :type jobID: int, string or list
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = [int( jobID )]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      try:
        jobID = [int( job ) for job in jobID]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == types.IntType:
      jobID = [jobID]

    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    statusDict = monitoring.getJobsStatus( jobID )
    minorStatusDict = monitoring.getJobsMinorStatus( jobID )
    siteDict = monitoring.getJobsSites( jobID )

    if not statusDict['OK']:
      self.log.warn( 'Could not obtain job status information' )
      return statusDict
    if not siteDict['OK']:
      self.log.warn( 'Could not obtain job site information' )
      return siteDict
    if not minorStatusDict['OK']:
      self.log.warn( 'Could not obtain job minor status information' )
      return minorStatusDict

    result = {}
    repoDict = {}
    for job, vals in statusDict['Value'].items():
      result[job] = vals
      if self.jobRepo:
        repoDict[job] = {'State':vals['Status']}
    if self.jobRepo:
      self.jobRepo.updateJobs( repoDict )
    for job, vals in siteDict['Value'].items():
      result[job].update( vals )
    for job, vals in minorStatusDict['Value'].items():
      result[job].update( vals )
    for job, vals in result.items():
      if result[job].has_key( 'JobID' ):
        del result[job]['JobID']

    return S_OK( result )

  #############################################################################
  def getJobInputData( self, jobID ):
    """Retrieve the input data requirement of any job existing in the workload management
       system.

       Example Usage:

       >>> dirac.getJobInputData(1405)
       {'OK': True, 'Value': {1405:
        ['LFN:/lhcb/production/DC06/phys-v2-lumi5/00001680/DST/0000/00001680_00000490_5.dst']}}

       :param jobID: JobID
       :type jobID: int, string or list
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = [int( jobID )]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      try:
        jobID = [int( job ) for job in jobID]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( 1 ):
      jobID = [jobID]

    summary = {}
    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    for job in jobID:
      result = monitoring.getInputData( job )
      if result['OK']:
        summary[job] = result['Value']
      else:
        self.log.warn( 'Getting input data for job %s failed with message:\n%s' % ( job, result['Message'] ) )
        summary[job] = []

    return S_OK( summary )

  #############################################################################
  def getJobOutputLFNs( self, jobID ):
    """ Retrieve the output data LFNs of a given job locally.

       This does not download the output files but simply returns the LFN list
       that a given job has produced.

       Example Usage:

       >>> dirac.getJobOutputLFNs(1405)
       {'OK':True,'Value':[<LFN>]}

       :param jobID: JobID
       :type jobID: int or string
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    result = self.parameters( int( jobID ) )
    if not result['OK']:
      return result
    if not result['Value'].has_key( 'UploadedOutputData' ):
      self.log.info( 'Parameters for job %s do not contain uploaded output data:\n%s' % ( jobID, result ) )
      return S_ERROR( 'No output data found for job %s' % jobID )

    outputData = result['Value']['UploadedOutputData']
    outputData = outputData.replace( ' ', '' ).split( ',' )
    if not outputData:
      return S_ERROR( 'No output data files found' )

    self.log.verbose( 'Found the following output data LFNs:\n', '\n'.join( outputData ) )
    return S_OK( outputData )

  #############################################################################
  def getJobOutputData( self, jobID, outputFiles = '', destinationDir = '' ):
    """ Retrieve the output data files of a given job locally.

       Optionally restrict the download of output data to a given file name or
       list of files using the outputFiles option, by default all job outputs
       will be downloaded.

       Example Usage:

       >>> dirac.getJobOutputData(1405)
       {'OK':True,'Value':[<LFN>]}

       :param jobID: JobID
       :type jobID: int or string
       :param outputFiles: Optional files to download
       :type outputFiles: string or list
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    result = self.parameters( int( jobID ) )
    if not result['OK']:
      return result
    if not result['Value'].has_key( 'UploadedOutputData' ):
      self.log.info( 'Parameters for job %s do not contain uploaded output data:\n%s' % ( jobID, result ) )
      return S_ERROR( 'No output data found for job %s' % jobID )

    outputData = result['Value']['UploadedOutputData']
    outputData = outputData.replace( ' ', '' ).split( ',' )
    if not outputData:
      return S_ERROR( 'No output data files found to download' )

    if outputFiles:
      if type( outputFiles ) == type( " " ):
        outputFiles = [os.path.basename( outputFiles )]
      elif type( outputFiles ) == type( [] ):
        try:
          outputFiles = [os.path.basename( fname ) for fname in outputFiles]
        except Exception, x:
          return self._errorReport( str( x ), 'Expected strings for output file names' )
      else:
        return self._errorReport( 'Expected strings for output file names' )
      self.log.info( 'Found specific outputFiles to download:', ', '.join( outputFiles ) )
      newOutputData = []
      for outputFile in outputData:
        if os.path.basename( outputFile ) in outputFiles:
          newOutputData.append( outputFile )
          self.log.verbose( '%s will be downloaded' % outputFile )
        else:
          self.log.verbose( '%s will be ignored' % outputFile )
      outputData = newOutputData

    # These two lines will break backwards compatibility.
    # if not destinationDir:
    #  destinationDir = jobID
    obtainedFiles = []
    for outputFile in outputData:
      self.log.info( 'Attempting to retrieve %s' % outputFile )
      result = self.getFile( outputFile, destDir = destinationDir )
      if not result['OK']:
        self.log.error( 'Failed to download %s' % outputFile )
        return result
      else:
        localPath = "%s/%s" % ( destinationDir, os.path.basename( outputFile ) )
        obtainedFiles.append( os.path.realpath( localPath ) )

    if self.jobRepo:
      self.jobRepo.updateJob( jobID, {'OutputData':1, 'OutputFiles':obtainedFiles} )
    return S_OK( outputData )

  #############################################################################
  def selectJobs( self, status = None, minorStatus = None, applicationStatus = None,
                  site = None, owner = None, ownerGroup = None, jobGroup = None, date = None ):
    """Options correspond to the web-page table columns. Returns the list of JobIDs for
       the specified conditions.  A few notes on the formatting:
        - date must be specified as yyyy-mm-dd.  By default, the date is today.
        - jobGroup corresponds to the name associated to a group of jobs, e.g. productionID / job names.
        - site is the DIRAC site name, e.g. LCG.CERN.ch
        - owner is the immutable nickname, e.g. paterson

       Example Usage:

       >>> dirac.selectJobs( status='Failed', owner='paterson', site='LCG.CERN.ch')
       {'OK': True, 'Value': ['25020', '25023', '25026', '25027', '25040']}

       :param status: Job status
       :type status: string
       :param minorStatus: Job minor status
       :type minorStatus: string
       :param applicationStatus: Job application status
       :type applicationStatus: string
       :param site: Job execution site
       :type site: string
       :param owner: Job owner
       :type owner: string
       :param jobGroup: Job group
       :type jobGroup: string
       :param date: Selection date
       :type date: string
       :returns: S_OK,S_ERROR
    """
    options = {'Status':status, 'MinorStatus':minorStatus, 'ApplicationStatus':applicationStatus, 'Owner':owner,
               'Site':site, 'JobGroup':jobGroup, 'OwnerGroup':ownerGroup }
    conditions = {}
    for key, value in options.items():
      if value:
        try:
          conditions[key] = str( value )
        except Exception, x:
          return self._errorReport( str( x ), 'Expected string for %s field' % key )

    if not type( date ) == type( " " ):
      try:
        if date:
          date = str( date )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected yyyy-mm-dd string for date' )

    if not date:
      date = '%s' % Time.date()
      self.log.verbose( 'Setting date to %s' % ( date ) )

    self.log.verbose( 'Will select jobs with last update %s and following conditions' % date )
    self.log.verbose( self.pPrint.pformat( conditions ) )
    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = monitoring.getJobs( conditions, date )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result

    jobIDs = result['Value']
    self.log.verbose( '%s job(s) selected' % ( len( jobIDs ) ) )
    if not jobIDs:
      return S_ERROR( 'No jobs selected for conditions: %s' % conditions )
    else:
      return result

  #############################################################################
  def getJobsInHerd( self, jobID ):
    """Get all jobs in the same herd as the given one.

       Example Usage:

       >>> dirac.getJobsInHerd( 2342 )
       {'OK': True, 'Value': [ 2342, 2533, 2534, 2643, 2650 ] }

       :param jobID: JobID
       :type JobID: int
       :returns: S_OK,S_ERROR
       """

    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = monitoring.getJobsInHerd( jobID )
    try:
      result.pop( 'rpcStub' )
    except:
      pass
    return result

  #############################################################################
  def getJobSummary( self, jobID, outputFile = None, printOutput = False ):
    """Output similar to the web page can be printed to the screen
       or stored as a file or just returned as a dictionary for further usage.

       Jobs can be specified individually or as a list.

       Example Usage:

       >>> dirac.getJobSummary(959209)
       {'OK': True, 'Value': {959209: {'Status': 'Staging', 'LastUpdateTime': '2008-12-08 16:43:18',
       'MinorStatus': '28 / 30', 'Site': 'Unknown', 'HeartBeatTime': 'None', 'ApplicationStatus': 'unknown',
       'JobGroup': '00003403', 'Owner': 'joel', 'SubmissionTime': '2008-12-08 16:41:38'}}}

       :param jobID: JobID
       :type jobID: int or string
       :param outputFile: Optional output file
       :type outputFile: string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = [int( jobID )]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      try:
        jobID = [int( job ) for job in jobID]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    headers = ['Status', 'MinorStatus', 'ApplicationStatus', 'Site', 'JobGroup', 'LastUpdateTime',
               'HeartBeatTime', 'SubmissionTime', 'Owner']

    if type( jobID ) == type( 1 ):
      jobID = [jobID]

    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = monitoring.getJobsSummary( jobID )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    try:
      jobSummary = eval( result['Value'] )
      # self.log.info(self.pPrint.pformat(jobSummary))
    except Exception, x:
      self.log.warn( 'Problem interpreting result from job monitoring service' )
      return S_ERROR( 'Problem while converting result from job monitoring' )

    summary = {}
    for job in jobID:
      summary[job] = {}
      for key in headers:
        if not jobSummary.has_key( job ):
          self.log.warn( 'No records for JobID %s' % job )
          value = 'None'
        elif jobSummary[job].has_key( key ):
          value = jobSummary[job][key]
        else:
          value = 'None'
        summary[job][key] = value

    if outputFile:
      if os.path.exists( outputFile ):
        return self._errorReport( 'Output file %s already exists' % ( outputFile ) )
      dirPath = os.path.basename( outputFile )
      if re.search( '/', dirPath ) and not os.path.exists( dirPath ):
        try:
          os.mkdir( dirPath )
        except Exception, x:
          return self._errorReport( str( x ), 'Could not create directory %s' % ( dirPath ) )

      fopen = open( outputFile, 'w' )
      line = 'JobID'.ljust( 12 )
      for i in headers:
        line += i.ljust( 35 )
      fopen.write( line + '\n' )
      for jobID, params in summary.items():
        line = str( jobID ).ljust( 12 )
        for header in headers:
          for key, value in params.items():
            if header == key:
              line += value.ljust( 35 )
        fopen.write( line + '\n' )
      fopen.close()
      self.log.verbose( 'Output written to %s' % outputFile )

    if printOutput:
      print self.pPrint.pformat( summary )

    return S_OK( summary )

  #############################################################################
  def getJobDebugOutput( self, jobID ):
    """Developer function. Try to retrieve all possible outputs including
       logging information, job parameters, sandbox outputs, pilot outputs,
       last heartbeat standard output, JDL and CPU profile.

       Example Usage:

       >>> dirac.getJobDebugOutput(959209)
       {'OK': True, 'Value': '/afs/cern.ch/user/p/paterson/DEBUG_959209'}

       :param jobID: JobID
       :type jobID: int or string
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    result = self.status( jobID )
    if not result['OK']:
      self.log.info( 'Could not obtain status information for jobID %s, please check this is valid.' % jobID )
      return S_ERROR( 'JobID %s not found in WMS' % jobID )
    else:
      self.log.info( 'Job %s' % result['Value'] )

    debugDir = '%s/DEBUG_%s' % ( os.getcwd(), jobID )
    try:
      os.mkdir( debugDir )
    except Exception, x:
      return self._errorReport( str( x ), 'Could not create directory in %s' % ( debugDir ) )

    try:
      result = self.getOutputSandbox( jobID, '%s' % ( debugDir ) )
      msg = []
      if not result['OK']:
        msg.append( 'Output Sandbox: Retrieval Failed' )
      else:
        msg.append( 'Output Sandbox: Retrieved' )
    except Exception, x:
      msg.append( 'Output Sandbox: Not Available' )

    try:
      result = self.getInputSandbox( jobID, '%s' % ( debugDir ) )
      if not result['OK']:
        msg.append( 'Input Sandbox: Retrieval Failed' )
      else:
        msg.append( 'Input Sandbox: Retrieved' )
    except Exception, x:
      msg.append( 'Input Sandbox: Not Available' )

    try:
      result = self.parameters( jobID )
      if not result['OK']:
        msg.append( 'Job Parameters: Retrieval Failed' )
      else:
        self.__writeFile( result['Value'], '%s/JobParameters' % ( debugDir ) )
        msg.append( 'Job Parameters: Retrieved' )
    except Exception, x:
      msg.append( 'Job Parameters: Not Available' )

    try:
      result = self.peek( jobID )
      if not result['OK']:
        msg.append( 'Last Heartbeat StdOut: Retrieval Failed' )
      else:
        self.__writeFile( result['Value'], '%s/LastHeartBeat' % ( debugDir ) )
        msg.append( 'Last Heartbeat StdOut: Retrieved' )
    except Exception, x:
      msg.append( 'Last Heartbeat StdOut: Not Available' )

    try:
      result = self.loggingInfo( jobID )
      if not result['OK']:
        msg.append( 'Logging Info: Retrieval Failed' )
      else:
        self.__writeFile( result['Value'], '%s/LoggingInfo' % ( debugDir ) )
        msg.append( 'Logging Info: Retrieved' )
    except Exception, x:
      msg.append( 'Logging Info: Not Available' )

    try:
      result = self.getJobJDL( jobID )
      if not result['OK']:
        msg.append( 'Job JDL: Retrieval Failed' )
      else:
        self.__writeFile( result['Value'], '%s/Job%s.jdl' % ( debugDir, jobID ) )
        msg.append( 'Job JDL: Retrieved' )
    except Exception, x:
      msg.append( 'Job JDL: Not Available' )

    try:
      result = self.getJobCPUTime( jobID )
      if not result['OK']:
        msg.append( 'CPU Profile: Retrieval Failed' )
      else:
        self.__writeFile( result['Value'], '%s/JobCPUProfile' % ( debugDir ) )
        msg.append( 'CPU Profile: Retrieved' )
    except Exception, x:
      msg.append( 'CPU Profile: Not Available' )

    self.log.info( 'Summary of debugging outputs for job %s retrieved in directory:\n%s\n' % ( jobID, debugDir ),
                  '\n'.join( msg ) )
    return S_OK( debugDir )

  #############################################################################
  def __writeFile( self, pObject, fileName ):
    """Internal function.  Writes a python object to a specified file path.
    """
    fopen = open( fileName, 'w' )
    if not type( pObject ) == type( " " ):
      fopen.write( '%s\n' % self.pPrint.pformat( pObject ) )
    else:
      fopen.write( pObject )
    fopen.close()

  #############################################################################
  def getJobCPUTime( self, jobID, printOutput = False ):
    """Retrieve job CPU consumed heartbeat data from job monitoring
       service.  Jobs can be specified individually or as a list.

       The time stamps and raw CPU consumed (s) are returned (if available).

       Example Usage:

       >>> d.getJobCPUTime(959209)
       {'OK': True, 'Value': {959209: {}}}

       :param jobID: JobID
       :type jobID: int or string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = [int( jobID )]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      try:
        jobID = [int( job ) for job in jobID]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    if type( jobID ) == type( 1 ):
      jobID = [jobID]

    summary = {}
    for job in jobID:
      monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
      result = monitoring.getJobHeartBeatData( job )
      summary[job] = {}
      if not result['OK']:
        return self._errorReport( result['Message'], 'Could not get heartbeat data for job %s' % job )
      if result['Value']:
        tupleList = result['Value']
        for tup in tupleList:
          if tup[0] == 'CPUConsumed':
            summary[job][tup[2]] = tup[1]
      else:
        self.log.warn( 'No heartbeat data for job %s' % job )

    if printOutput:
      print self.pPrint.pformat( summary )

    return S_OK( summary )

  #############################################################################
  def attributes( self, jobID, printOutput = False ):
    """Return DIRAC attributes associated with the given job.

       Each job will have certain attributes that affect the journey through the
       workload management system, see example below. Attributes are optionally
       printed to the screen.

       Example Usage:

       >>> print dirac.attributes(79241)
       {'AccountedFlag': 'False','ApplicationNumStatus': '0',
       'ApplicationStatus': 'Job Finished Successfully',
       'CPUTime': '0.0','DIRACSetup': 'LHCb-Production'}

       :param jobID: JobID
       :type jobID: int, string or list
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = [int( jobID )]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      try:
        jobID = [int( job ) for job in jobID]
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = monitoring.getJobAttributes( jobID )
    if not result['OK']:
      return result

    if printOutput:
      print self.pPrint.pformat( result['Value'] )

    return result

  #############################################################################
  def parameters( self, jobID, printOutput = False ):
    """Return DIRAC parameters associated with the given job.

       DIRAC keeps track of several job parameters which are kept in the job monitoring
       service, see example below. Selected parameters also printed to screen.

       Example Usage:

       >>> print dirac.parameters(79241)
       {'OK': True, 'Value': {'JobPath': 'JobPath,JobSanity,JobPolicy,InputData,JobScheduling,TaskQueue',
       'JobSanityCheck': 'Job: 768 JDL: OK, InputData: 2 LFNs OK, ','LocalBatchID': 'dc768'}

       :param jobID: JobID
       :type jobID: int or string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      return self._errorReport( 'Expected integer or string for jobID' )

    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = monitoring.getJobParameters( jobID )
    if not result['OK']:
      return result

    if result['Value'].has_key( 'StandardOutput' ):
      del result['Value']['StandardOutput']

    if printOutput:
      print self.pPrint.pformat( result['Value'] )

    return result

  #############################################################################
  def loggingInfo( self, jobID, printOutput = False ):
    """DIRAC keeps track of job transitions which are kept in the job monitoring
       service, see example below.  Logging summary also printed to screen at the
       INFO level.

       Example Usage:

       >>> print dirac.loggingInfo(79241)
       {'OK': True, 'Value': [('Received', 'JobPath', 'Unknown', '2008-01-29 15:37:09', 'JobPathAgent'),
       ('Checking', 'JobSanity', 'Unknown', '2008-01-29 15:37:14', 'JobSanityAgent')]}

       :param jobID: JobID
       :type jobID: int or string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
     """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      return self._errorReport( 'Expected int or string, not list' )

    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = monitoring.getJobLoggingInfo( jobID )
    if not result['OK']:
      self.log.warn( 'Could not retrieve logging information for job %s' % jobID )
      self.log.warn( result )
      return result

    if printOutput:
      loggingTupleList = result['Value']
      # source is removed for printing to control width
      headers = ( 'Status', 'MinorStatus', 'ApplicationStatus', 'DateTime' )
      line = ''
      for i in headers:
        line += i.ljust( 30 )
      print line

      for i in loggingTupleList:
        line = ''
        for j in xrange( len( i ) - 1 ):
          line += i[j].ljust( 30 )
        print line

    return result

  #############################################################################
  def peek( self, jobID, printout = False ):
    """The peek function will attempt to return standard output from the WMS for
       a given job if this is available.  The standard output is periodically
       updated from the compute resource via the application Watchdog. Available
       standard output is  printed to screen at the INFO level.

       Example Usage:

       >>> print dirac.peek(1484)
       {'OK': True, 'Value': 'Job peek result'}

       :param jobID: JobID
       :type jobID: int or string
       :returns: S_OK,S_ERROR
    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )
    elif type( jobID ) == type( [] ):
      return self._errorReport( 'Expected int or string, not list' )

    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = monitoring.getJobParameter( jobID, 'StandardOutput' )
    if not result['OK']:
      return self._errorReport( result, 'Could not retrieve job attributes' )

    stdout = 'Not available yet.'
    if result['Value'].has_key( 'StandardOutput' ):
      self.log.verbose( result['Value']['StandardOutput'] )
      stdout = result['Value']['StandardOutput']
      if printout:
        print stdout
    else:
      self.log.info( 'No standard output available to print.' )

    return S_OK( stdout )

  #############################################################################
  def ping( self, system, service, printOutput = False ):
    """The ping function will attempt to return standard information from a system
       service if this is available.  If the ping() command is unsuccessful it could
       indicate a period of service unavailability.

       Example Usage:

       >>> print dirac.ping('WorkloadManagement','JobManager')
       {'OK': True, 'Value': 'Job ping result'}

       :param system: system
       :type system: string
       :param service: service name
       :type service: string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
    """
    if not type( system ) == type( " " ) and type( service ) == type( " " ):
      return self._errorReport( 'Expected string for system and service to ping()' )
    result = S_ERROR()
    try:
      systemSection = getSystemSection( system + '/' )
      self.log.verbose( 'System section is: %s' % ( systemSection ) )
      section = '%s/%s' % ( systemSection, service )
      self.log.verbose( 'Requested service should have CS path: %s' % ( section ) )
      serviceURL = getServiceURL( '%s/%s' % ( system, service ) )
      self.log.verbose( 'Service URL is: %s' % ( serviceURL ) )
      client = RPCClient( '%s/%s' % ( system, service ) )
      result = client.ping()
      if result['OK']:
        result['Value']['service url'] = serviceURL
    except Exception, x:
      self.log.warn( 'ping for %s/%s failed with exception:\n%s' % ( system, service, str( x ) ) )
      result['Message'] = str( x )

    if printOutput:
      print self.pPrint.pformat( result )
    return result

  #############################################################################
  def getJobJDL( self, jobID, printOutput = False ):
    """Simple function to retrieve the current JDL of an existing job in the
       workload management system.  The job JDL is converted to a dictionary
       and returned in the result structure.

       Example Usage:

       >>> print dirac.getJobJDL(12345)
       {'Arguments': 'jobDescription.xml',...}

       :param jobID: JobID
       :type jobID: int or string
       :returns: S_OK,S_ERROR

    """
    if type( jobID ) == type( " " ):
      try:
        jobID = int( jobID )
      except Exception, x:
        return self._errorReport( str( x ), 'Expected integer or string for existing jobID' )

    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    result = monitoring.getJobJDL( jobID )
    if not result['OK']:
      return result

    result = self.__getJDLParameters( result['Value'] )
    if printOutput:
      print self.pPrint.pformat( result['Value'] )

    return result

  #############################################################################
  def __getJDLParameters( self, jdl ):
    """Internal function. Returns a dictionary of JDL parameters.
    """
    if os.path.exists( jdl ):
      jdlFile = open( jdl, 'r' )
      jdl = jdlFile.read()
      jdlFile.close()

    try:
      parameters = {}
      if not re.search( '\[', jdl ):
        jdl = '[' + jdl + ']'
      classAdJob = ClassAd( jdl )
      paramsDict = classAdJob.contents
      for param, value in paramsDict.items():
        if re.search( '{', value ):
          self.log.debug( 'Found list type parameter %s' % ( param ) )
          rawValues = value.replace( '{', '' ).replace( '}', '' ).replace( '"', '' ).replace( 'LFN:', '' ).split()
          valueList = []
          for val in rawValues:
            if re.search( ',$', val ):
              valueList.append( val[:-1] )
            else:
              valueList.append( val )
          parameters[param] = valueList
        else:
          self.log.debug( 'Found standard parameter %s' % ( param ) )
          parameters[param] = value.replace( '"', '' )
      return S_OK( parameters )
    except Exception, x:
      self.log.exception( lException = x )
      return S_ERROR( 'Exception while extracting JDL parameters for job' )

  #############################################################################
  def __printInfo( self ):
    """Internal function to print the DIRAC API version and related information.
    """
    self.log.info( '<=====%s=====>' % ( self.diracInfo ) )
    self.log.verbose( 'DIRAC is running at %s in setup %s' % ( DIRAC.siteName(), self.setup ) )

  def getConfigurationValue( self, option, default ):
    """ Export the configuration client getValue() function
    """

    return gConfig.getValue( option, default )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
