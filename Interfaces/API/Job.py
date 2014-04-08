"""
   Job Base Class

   This class provides generic job definition functionality suitable for any VO.

   Helper functions are documented with example usage for the DIRAC API.  An example
   script (for a simple executable) would be::

     from DIRAC.Interfaces.API.Dirac import Dirac
     from DIRAC.Interfaces.API.Job import Job

     j = Job()
     j.setCPUTime(500)
     j.setExecutable('/bin/echo hello')
     j.setExecutable('yourPythonScript.py')
     j.setExecutable('/bin/echo hello again')
     j.setName('MyJobName')

     dirac = Dirac()
     jobID = dirac.submit(j)
     print 'Submission Result: ',jobID

   Note that several executables can be provided and wil be executed sequentially.
"""
__RCSID__ = "$Id$"
import re, os, types, urllib

from DIRAC                                                    import S_OK, S_ERROR, gLogger
from DIRAC.Core.Workflow.Parameter                            import Parameter
from DIRAC.Core.Workflow.Workflow                             import Workflow
from DIRAC.Core.Base.API                                      import API
from DIRAC.Core.Utilities.ClassAd.ClassAdLight                import ClassAd
from DIRAC.ConfigurationSystem.Client.Config                  import gConfig
from DIRAC.Core.Security.ProxyInfo                            import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry        import getVOForGroup
from DIRAC.Core.Utilities.Subprocess                          import shellCall
from DIRAC.Core.Utilities.List                                import uniqueElements
from DIRAC.Core.Utilities.SiteCEMapping                       import getSiteForCE, getSiteCEMapping
from DIRAC.ConfigurationSystem.Client.Helpers.Operations      import Operations
from DIRAC.ConfigurationSystem.Client.Helpers                 import Resources
from DIRAC.Interfaces.API.Dirac                               import Dirac
from DIRAC.Workflow.Utilities.Utils                           import getStepDefinition, addStepToWorkflow

COMPONENT_NAME = '/Interfaces/API/Job'

class Job( API ):
  """ DIRAC jobs
  """

  #############################################################################

  def __init__( self, script = None, stdout = 'std.out', stderr = 'std.err' ):
    """Instantiates the Workflow object and some default parameters.
    """

    super( Job, self ).__init__()

    self.dbg = False
    if gConfig.getValue( self.section + '/LogLevel', 'DEBUG' ) == 'DEBUG':
      self.dbg = True

    #gConfig.getValue('Tier0SE-tape','SEName')
    self.stepCount = 0
    self.owner = 'NotSpecified'
    self.name = 'Name'
    self.type = 'User'
    self.priority = 1
    vo = ''
    ret = getProxyInfo( disableVOMS = True )
    if ret['OK'] and 'group' in ret['Value']:
      vo = getVOForGroup( ret['Value']['group'] )
    self.group = vo
    self.site = 'ANY' #ANY
    #self.setup = 'Development'
    self.origin = 'DIRAC'
    self.stdout = stdout
    self.stderr = stderr
    self.logLevel = 'info'
    self.executable = '$DIRACROOT/scripts/dirac-jobexec' # to be clarified
    self.addToInputSandbox = []
    self.addToOutputSandbox = []
    self.addToInputData = []
    self.systemConfig = 'ANY'
    ##Add member to handle Parametric jobs
    self.parametric = {}
    self.script = script
    if not script:
      self.workflow = Workflow()
      self.__setJobDefaults()
    else:
      self.workflow = Workflow( script )

  #############################################################################

  def setExecutable( self, executable, arguments = '', logFile = '',
                       modulesList = ['Script'],
                       parameters = [( 'executable', 'string', '', "Executable Script" ),
                                     ( 'arguments', 'string', '', 'Arguments for executable Script' ),
                                     ( 'applicationLog', 'string', '', "Log file name" )],
                       paramValues = [] ):
    """Helper function.

       Specify executable script to run with optional arguments and log file
       for standard output.

       These can be either:

        - Submission of a python or shell script to DIRAC
           - Can be inline scripts e.g. C{'/bin/ls'}
           - Scripts as executables e.g. python or shell script file

       Example usage:

       >>> job = Job()
       >>> job.setExecutable('myScript.py')

       @param executable: Executable
       @type executable: string
       @param arguments: Optional arguments to executable
       @type arguments: string
       @param logFile: Optional log file name
       @type logFile: string
       @param modulesList: Optional list of modules (to be used mostly when extending this method)
       @type modulesList: list
       @param parameters: Optional list of parameters (to be used mostly when extending this method)
       @type parameters: list of tuples
       @param paramValues: Optional list of parameters values (to be used mostly when extending this method)
       @type parameters: list of tuples
    """
    kwargs = {'executable':executable, 'arguments':arguments, 'logFile':logFile}
    if not type( executable ) == type( ' ' ) or not type( arguments ) == type( ' ' ) or \
       not type( logFile ) == type( ' ' ):
      return self._reportError( 'Expected strings for executable and arguments', **kwargs )

    if os.path.exists( executable ):
      self.log.verbose( 'Found script executable file %s' % ( executable ) )
      self.addToInputSandbox.append( executable )
      logName = '%s.log' % ( os.path.basename( executable ) )
    else:
      self.log.warn( 'The executable code could not be found locally' )
      logName = 'CodeOutput.log'

    if logFile:
      if type( logFile ) == type( ' ' ):
        logName = str(logFile)

    self.stepCount += 1
    stepName = 'RunScriptStep%s' % ( self.stepCount )

    step = getStepDefinition( 'ScriptStep%s' % ( self.stepCount ), modulesList, parametersList = parameters )
    self.addToOutputSandbox.append( logName )

    stepInstance = addStepToWorkflow( self.workflow, step, stepName )

    stepInstance.setValue( 'applicationLog', logName )
    stepInstance.setValue( 'executable', executable )
    if arguments:
      stepInstance.setValue( 'arguments', arguments )
    if paramValues:
      for param, value in paramValues:
        stepInstance.setValue( param, value )

    return S_OK( stepInstance )

  #############################################################################
  def setName( self, jobName ):
    """Helper function.

       A name for the job can be specified if desired. This will appear
       in the JobName field of the monitoring webpage. If nothing is
       specified a default value will appear.

       Example usage:

       >>> job=Job()
       >>> job.setName("myJobName")

       :param jobName: Name of job
       :type jobName: string
    """
    kwargs = {'jobname':jobName}
    if not type( jobName ) == type( ' ' ):
      return self._reportError( 'Expected strings for job name', **kwargs )
    else:
      self.workflow.setName( jobName )
      self._addParameter( self.workflow, 'JobName', 'JDL', jobName, 'User specified name' )

    return S_OK()

  #############################################################################
  def setInputSandbox( self, files ):
    """Helper function.

       Specify input sandbox files less than 10MB in size.  If over 10MB, files
       or a directory may be uploaded to Grid storage, see C{dirac.uploadSandbox()}.

       Paths to the options file and (if required) 'lib/' directory of the DLLs
       are specified here. Default is local directory.  CMT requirements files or
       executables may be placed in the lib/ directory if desired. The lib/ directory
       is transferred to the Grid Worker Node before the job executes.

       Files / directories can be specified using the '*' character e.g. *.txt  these
       are resolved correctly before job execution on the WN.

       Example usage:

       >>> job = Job()
       >>> job.setInputSandbox(['DaVinci.opts'])

       :param files: Input sandbox files, can specify full path
       :type files: Single string or list of strings ['','']
    """
    if type( files ) == list and len( files ):
      resolvedFiles = self._resolveInputSandbox( files )
      fileList = ';'.join( resolvedFiles )
      description = 'Input sandbox file list'
      self._addParameter( self.workflow, 'InputSandbox', 'JDL', fileList, description )
      #self.sandboxFiles=resolvedFiles
    elif type( files ) == type( " " ):
      resolvedFiles = self._resolveInputSandbox( [files] )
      fileList = ';'.join( resolvedFiles )
      description = 'Input sandbox file'
      #self.sandboxFiles = [files]
      self._addParameter( self.workflow, 'InputSandbox', 'JDL', fileList, description )
    else:
      kwargs = {'files':files}
      return self._reportError( 'Expected file string or list of files for input sandbox contents', **kwargs )

    return S_OK()

  #############################################################################
  def setParametricInputSandbox( self, files ):
    """Helper function.

       Specify input sandbox files to used as parameters in the Parametric jobs. The possibilities are identical to the setInputSandbox.


       Example usage:

       >>> job = Job()
       >>> job.setParametricInputSandbox(['LFN:/Some_file','LFN:/Some_other_file'])

       :param files: Logical File Names
       :type files: Single LFN string or list of LFNs
    """
    kwargs = {'files':files}
    if type( files ) == list and len( files ):
      for fileName in files:
        if not fileName.lower().count( "lfn:" ):
          return self._reportError( 'All files should be LFNs', **kwargs )
      resolvedFiles = self._resolveInputSandbox( files )
      self.parametric['InputSandbox'] = resolvedFiles
      #self.sandboxFiles=resolvedFiles
    elif type( files ) == type( " " ):
      if not files.lower().count( "lfn:" ):
        return self._reportError( 'All files should be LFNs', **kwargs )
      resolvedFiles = self._resolveInputSandbox( [files] )
      self.parametric['InputSandbox'] = resolvedFiles
      #self.sandboxFiles = [files]
    else:
      return self._reportError( 'Expected file string or list of files for input sandbox contents', **kwargs )

    return S_OK()

  #############################################################################
  def setOutputSandbox( self, files ):
    """Helper function.

       Specify output sandbox files.  If specified files are over 10MB, these
       may be uploaded to Grid storage with a notification returned in the
       output sandbox.

       Example usage:

       >>> job = Job()
       >>> job.setOutputSandbox(['DaVinci_v19r12.log','DVNTuples.root'])

       :param files: Output sandbox files
       :type files: Single string or list of strings ['','']

    """
    if type( files ) == list and len( files ):
      fileList = ';'.join( files )
      description = 'Output sandbox file list'
      self._addParameter( self.workflow, 'OutputSandbox', 'JDL', fileList, description )
    elif type( files ) == type( " " ):
      description = 'Output sandbox file'
      self._addParameter( self.workflow, 'OutputSandbox', 'JDL', files, description )
    else:
      kwargs = {'files':files}
      return self._reportError( 'Expected file string or list of files for output sandbox contents', **kwargs )

    return S_OK()

  #############################################################################
  def setInputData( self, lfns ):
    """Helper function.

       Specify input data by Logical File Name (LFN).

       Example usage:

       >>> job = Job()
       >>> job.setInputData(['/lhcb/production/DC04/v2/DST/00000742_00003493_10.dst'])

       :param lfns: Logical File Names
       :type lfns: Single LFN string or list of LFNs
    """
    if type( lfns ) == list and len( lfns ):
      for i in xrange( len( lfns ) ):
        lfns[i] = lfns[i].replace( 'LFN:', '' )
      inputData = ['LFN:' + x for x in lfns ]
      inputDataStr = ';'.join( inputData )
      description = 'List of input data specified by LFNs'
      self._addParameter( self.workflow, 'InputData', 'JDL', inputDataStr, description )
    elif type( lfns ) == type( ' ' ):  #single LFN
      description = 'Input data specified by LFN'
      self._addParameter( self.workflow, 'InputData', 'JDL', lfns, description )
    else:
      kwargs = {'lfns':lfns}
      return self._reportError( 'Expected lfn string or list of lfns for input data', **kwargs )

    return S_OK()

  #############################################################################
  def setParametricInputData( self, lfns ):
    """Helper function.

       Specify input data by Logical File Name (LFN) to be used as a parameter in a parametric job

       Example usage:

       >>> job = Job()
       >>> job.setParametricInputData(['/lhcb/production/DC04/v2/DST/00000742_00003493_10.dst'])

       :param lfns: Logical File Names
       :type lfns: Single LFN string or list of LFNs
    """
    if type( lfns ) == list and len( lfns ):
      for i in xrange( len( lfns ) ):
        if type( lfns[i] ) == list and len( lfns[i] ):
          for k in xrange( len( lfns[i] ) ):
            lfns[i][k] = 'LFN:' + lfns[i][k].replace( 'LFN:', '' )
        else:
          lfns[i] = 'LFN:' + lfns[i].replace( 'LFN:', '' )
      self.parametric['InputData'] = lfns
    elif type( lfns ) == type( ' ' ):  #single LFN
      self.parametric['InputData'] = lfns
    else:
      kwargs = {'lfns':lfns}
      return self._reportError( 'Expected lfn string or list of lfns for parametric input data', **kwargs )

    return S_OK()

  #############################################################################  
  def setGenericParametricInput( self, inputlist ):
    """ Helper function

       Define a generic parametric job with this function. Should not be used when
       the ParametricInputData of ParametricInputSandbox are used.

       :param inputlist: Input list of parameters to build the parametric job
       :type inputlist: list

    """
    kwargs = {'inputlist':inputlist}
    if not type( inputlist ) == type( [] ):
      return self._reportError( 'Expected list for parameters', **kwargs )
    self.parametric['GenericParameters'] = inputlist
    return S_OK()

  #############################################################################
  def setInputDataPolicy( self, policy, dataScheduling = True ):
    """Helper function.

       Specify a job input data policy, this takes precedence over any site specific or
       global settings.

       Possible values for policy are 'Download' or 'Protocol' (case-insensitive). This
       requires that the module locations are defined for the VO in the CS.

       Example usage:

       >>> job = Job()
       >>> job.setInputDataPolicy('download')

    """
    kwargs = {'policy':policy, 'dataScheduling':dataScheduling}
    csSection = 'InputDataPolicy'
    possible = ['Download', 'Protocol']
    finalPolicy = ''
    for value in possible:
      if policy.lower() == value.lower():
        finalPolicy = value

    if not finalPolicy:
      return self._reportError( 'Expected one of %s for input data policy' % ( ', '.join( possible ) ),
                                __name__, **kwargs )

    jobPolicy = Operations().getValue( '%s/%s' % ( csSection, finalPolicy ), '' )
    if not jobPolicy:
      return self._reportError( 'Could not get value for Operations option %s/%s' % ( csSection, finalPolicy ),
                                __name__, **kwargs )

    description = 'User specified input data policy'
    self._addParameter( self.workflow, 'InputDataPolicy', 'JDL', jobPolicy, description )

    if not dataScheduling and policy.lower() == 'download':
      self.log.verbose( 'Scheduling by input data is disabled, jobs will run anywhere and download input data' )
      self._addParameter( self.workflow, 'DisableDataScheduling', 'JDL', 'True', 'Disable scheduling by input data' )

    if not dataScheduling and policy.lower() != 'download':
      self.log.error( 'Expected policy to be "download" for bypassing data scheduling' )
      return self._reportError( 'Expected policy to be "download" for bypassing data scheduling',
                                __name__, **kwargs )

    return S_OK()

  #############################################################################
  def setOutputData( self, lfns, outputSE = None, outputPath = '' ):
    """Helper function.

       For specifying output data to be registered in Grid storage.  If a list
       of OutputSEs are specified the job wrapper will try each in turn until
       successful.  If the OutputPath is specified this will appear only after
       / <VO> / user / <initial> / <username>
       directory.

       Example usage:

       >>> job = Job()
       >>> job.setOutputData(['DVNtuple.root'])

       :param lfns: Output data file or files
       :type lfns: Single string or list of strings ['','']
       :param outputSE: Optional parameter to specify the Storage Element
       :param outputPath: Optional parameter to specify part of the path in the storage (see above)
       Element to store data or files, e.g. CERN-tape
       :type outputSE: string or list
       :type outputPath: string
    """
    if outputSE == None:
      outputSE = []
    kwargs = {'lfns':lfns, 'OutputSE':outputSE, 'OutputPath':outputPath}
    if type( lfns ) == list and len( lfns ):
      outputDataStr = ';'.join( lfns )
      description = 'List of output data files'
      self._addParameter( self.workflow, 'OutputData', 'JDL', outputDataStr, description )
    elif type( lfns ) == type( " " ):
      description = 'Output data file'
      self._addParameter( self.workflow, 'OutputData', 'JDL', lfns, description )
    else:
      return self._reportError( 'Expected file name string or list of file names for output data', **kwargs )

    if outputSE:
      description = 'User specified Output SE'
      if type( outputSE ) in types.StringTypes:
        outputSE = [outputSE]
      elif type( outputSE ) != types.ListType:
        return self._reportError( 'Expected string or list for OutputSE', **kwargs )
      outputSE = ';'.join( outputSE )
      self._addParameter( self.workflow, 'OutputSE', 'JDL', outputSE, description )

    if outputPath:
      description = 'User specified Output Path'
      if not type( outputPath ) in types.StringTypes:
        return self._reportError( 'Expected string for OutputPath', **kwargs )
      # Remove leading "/" that might cause problems with os.path.join
      # FIXME: this will prevent to set OutputPath outside the Home of the User
      while outputPath[0] == '/':
        outputPath = outputPath[1:]
      self._addParameter( self.workflow, 'OutputPath', 'JDL', outputPath, description )

    return S_OK()

  #############################################################################
  def setPlatform( self, platform ):
    """Developer function: sets the target platform, e.g. Linux_x86_64_glibc-2.5.
       This platform is in the form of what it is returned by the dirac-platform script
       (or dirac-architecture if your extension provides it)
    """
    kwargs = {'platform':platform}

    if not type( platform ) == type( " " ):
      return self._reportError( "Expected string for platform", **kwargs )

    if not platform.lower() == 'any':
      availablePlatforms = Resources.getDIRACPlatforms()
      if not availablePlatforms['OK']:
        return self._reportError( "Can't check for platform", **kwargs )
      if platform in availablePlatforms['Value']:
        self._addParameter( self.workflow, 'Platform', 'JDL', platform, 'Platform ( Operating System )' )
      else:
        return self._reportError( "Invalid platform", **kwargs )

    return S_OK()

  #############################################################################
  def setSubmitPool( self, backend ):
    """Developer function.

       Choose submission pool on which job is executed.
       Default in place for users.
    """
    #should add protection here for list of supported platforms
    kwargs = {'backend':backend}
    if not type( backend ) in types.StringTypes:
      return self._reportError( 'Expected string for SubmitPool', **kwargs )

    if not backend.lower() == 'any':
      self._addParameter( self.workflow, 'SubmitPools', 'JDL', backend, 'Submit Pool' )

    return S_OK()

  #############################################################################
  def setCPUTime( self, timeInSecs ):
    """Helper function.

       Under Development. Specify CPU time requirement in DIRAC units.

       Example usage:

       >>> job = Job()
       >>> job.setCPUTime(5000)

       :param timeInSecs: CPU time
       :type timeInSecs: Int
    """
    kwargs = {'timeInSecs':timeInSecs}
    if not type( timeInSecs ) == int:
      try:
        timeInSecs = int( timeInSecs )
      except Exception:
        if not re.search( '{{', timeInSecs ):
          return self._reportError( 'Expected numerical string or int for CPU time in seconds', **kwargs )

    description = 'CPU time in secs'
    self._addParameter( self.workflow, 'CPUTime', 'JDL', timeInSecs, description )
    return S_OK()

  #############################################################################
  def setDestination( self, destination ):
    """Helper function.

       Can specify a desired destination site or sites for job.  This can be useful
       for debugging purposes but often limits the possible candidate sites
       and overall system response time.

       Example usage:

       >>> job = Job()
       >>> job.setDestination('LCG.CERN.ch')

       :param destination: site string
       :type destination: string or list
    """
    kwargs = {'destination':destination}
    if type( destination ) == type( "  " ):
      if not re.search( '^DIRAC.', destination ) and not destination.lower() == 'any':
        result = self.__checkSiteIsValid( destination )
        if not result['OK']:
          return self._reportError( '%s is not a valid destination site' % ( destination ), **kwargs )
      description = 'User specified destination site'
      self._addParameter( self.workflow, 'Site', 'JDL', destination, description )
    elif type( destination ) == list:
      for site in destination:
        if not re.search( '^DIRAC.', site ) and not site.lower() == 'any':
          result = self.__checkSiteIsValid( site )
          if not result['OK']:
            return self._reportError( '%s is not a valid destination site' % ( destination ), **kwargs )
      destSites = ';'.join( destination )
      description = 'List of sites selected by user'
      self._addParameter( self.workflow, 'Site', 'JDL', destSites, description )
    else:
      return self._reportError( '%s is not a valid destination site, expected string' % ( destination ), **kwargs )
    return S_OK()

  #############################################################################
  def __checkSiteIsValid( self, site ):
    """Internal function to check that a site name is valid.
    """
    sites = getSiteCEMapping()
    if not sites['OK']:
      return S_ERROR( 'Could not get site CE mapping' )
    siteList = sites['Value'].keys()
    if not site in siteList:
      return S_ERROR( 'Specified site %s is not in list of defined sites' % site )

    return S_OK( '%s is valid' % site )

  #############################################################################
  def setDestinationCE( self, ceName ):
    """ Developer function.

        Allows to direct a job to a particular Grid CE.
    """
    kwargs = {'ceName':ceName}
    diracSite = getSiteForCE( ceName )
    if not diracSite['OK']:
      return self._reportError( diracSite['Message'], **kwargs )
    if not diracSite['Value']:
      return self._reportError( 'No DIRAC site name found for CE %s' % ( ceName ), **kwargs )

    diracSite = diracSite['Value']
    self.setDestination( diracSite )
    # Keep GridRequiredCEs for backward compatibility
    self._addJDLParameter( 'GridRequiredCEs', ceName )
    self._addJDLParameter( 'GridCE', ceName )
    return S_OK()

  #############################################################################
  def setBannedSites( self, sites ):
    """Helper function.

       Can specify a desired destination site for job.  This can be useful
       for debugging purposes but often limits the possible candidate sites
       and overall system response time.

       Example usage:

       >>> job = Job()
       >>> job.setBannedSites(['LCG.GRIDKA.de','LCG.CNAF.it'])

       :param sites: single site string or list
       :type sites: string or list
    """
    if type( sites ) == list and len( sites ):
      bannedSites = ';'.join( sites )
      description = 'List of sites excluded by user'
      self._addParameter( self.workflow, 'BannedSites', 'JDL', bannedSites, description )
    elif type( sites ) == type( " " ):
      description = 'Site excluded by user'
      self._addParameter( self.workflow, 'BannedSites', 'JDL', sites, description )
    else:
      kwargs = {'sites':sites}
      return self._reportError( 'Expected site string or list of sites', **kwargs )
    return S_OK()

  #############################################################################
  def setOwner( self, ownerProvided ):
    """Developer function.

       Normally users should always specify their immutable DIRAC nickname.
    """
    if not type( ownerProvided ) == type( " " ):
      return self._reportError( 'Expected string for owner', **{'ownerProvided':ownerProvided} )

    self._addParameter( self.workflow, 'Owner', 'JDL', ownerProvided, 'User specified ID' )
    return S_OK()

  #############################################################################
  def setOwnerGroup( self, ownerGroup ):
    """Developer function.

       Allows to force expected owner group of proxy.
    """
    if not type( ownerGroup ) == type( " " ):
      return self._reportError( 'Expected string for job owner group', **{'ownerGroup':ownerGroup} )

    self._addParameter( self.workflow, 'OwnerGroup', 'JDL', ownerGroup, 'User specified owner group.' )
    return S_OK()

  #############################################################################
  def setOwnerDN( self, ownerDN ):
    """Developer function.

       Allows to force expected owner DN of proxy.
    """
    if not type( ownerDN ) == type( " " ):
      return self._reportError( 'Expected string for job owner DN', **{'ownerGroup':ownerDN} )

    self._addParameter( self.workflow, 'OwnerDN', 'JDL', ownerDN, 'User specified owner DN.' )
    return S_OK()

  #############################################################################
  def setType( self, jobType ):
    """Developer function.

       Specify job type for testing purposes.
    """
    if not type( jobType ) == type( " " ):
      return self._reportError( 'Expected string for job type', **{'jobType':jobType} )

    self._addParameter( self.workflow, 'JobType', 'JDL', jobType, 'User specified type' )
    self.type = jobType
    return S_OK()
  
  #############################################################################
  def setTag( self, tags ):
    """ Set the Tags job requirements
    
        Example usage:

        >>> job = Job()
        >>> job.setTag( ['WholeNode','8GBMemory'] ) 
    
        :param tags: single tag string or a list of tags
        :type tags: string or list
    """
    
    if type( tags ) in types.StringTypes:
      tagValue = tags
    elif type( tags ) == types.ListType:
      tagValue = ";".join( tags )
    else:  
      return self._reportError( 'Expected string or list for job tags', tags = tags )

    self._addParameter( self.workflow, 'Tags', 'JDL', tagValue, 'User specified job tags' )
    self.tags = tags
    return S_OK()

  #############################################################################
  def _setSoftwareTags( self, tags ):
    """Developer function.

       Choose any software tags if desired.  These are not compulsory but will ensure jobs only
       arrive at an LCG site where the software is preinstalled.  Without the tags, missing software is
       installed automatically by the Job Agent.

       Example usage:

       >>> job=Job()
       >>> job.setSoftwareTags(['VO-lhcb-Brunel-v30r17','VO-lhcb-Boole-v12r10','VO-lhcb-Gauss-v25r12'])

       :param tags: software tags
       :type tags: string or list
    """
    if type( tags ) == type( " " ):
      self._addParameter( self.workflow, 'SoftwareTag', 'JDL', tags, 'VO software tag' )
    elif type( tags ) == list:
      swTags = ';'.join( tags )
      self._addParameter( self.workflow, 'SoftwareTag', 'JDL', swTags, 'List of VO software tags' )
    else:
      kwargs = {'tags':tags}
      return self._reportError( 'Expected String or List of software tags', **kwargs )

    return S_OK()

  #############################################################################
  def setJobGroup( self, jobGroup ):
    """Helper function.

       Allows to group certain jobs according to an ID.

       Example usage:

       >>> job = Job()
       >>> job.setJobGroup('Bs2JPsiPhi')

       :param jobGroup: JobGroup name
       :type jobGroup: string
    """
    if not type( jobGroup ) == type( " " ):
      return self._reportError( 'Expected string for job group name', **{'jobGroup':jobGroup} )

    description = 'User specified job group'
    self._addParameter( self.workflow, 'JobGroup', 'JDL', jobGroup, description )
    return S_OK()

  #############################################################################
  def setLogLevel( self, logLevel ):
    """Helper function.

       Optionally specify a DIRAC logging level for the job, e.g.
       ALWAYS, INFO, VERBOSE, WARN, DEBUG
       by default this is set to the info level.

       Example usage:

       >>> job = Job()
       >>> job.setLogLevel('debug')

       :param logLevel: Logging level
       :type logLevel: string
    """
    kwargs = {'logLevel':logLevel}
    if type( logLevel ) in types.StringTypes:
      if logLevel.upper() in gLogger._logLevels.getLevels():
        description = 'User specified logging level'
        self.logLevel = logLevel
        self._addParameter( self.workflow, 'LogLevel', 'JDL', logLevel, description )
      else:
        return self._reportError( 'Error Level "%s" not valid' % logLevel, **kwargs )
    else:
      return self._reportError( 'Expected string for logging level', **kwargs )
    return S_OK()

  #############################################################################
  def setConfigArgs( self, cfgString ):
    """Developer function. Allow to pass arbitrary settings to the payload
       configuration service environment.
    """
    if not type( cfgString ) == type( " " ):
      return self._reportError( 'Expected string for DIRAC Job Config Args', **{'cfgString':cfgString} )

    description = 'User specified cfg settings'
    self._addParameter( self.workflow, 'JobConfigArgs', 'JDL', cfgString, description )
    return S_OK()

  #############################################################################
  def setExecutionEnv( self, environmentDict ):
    """Helper function.

       Optionally specify a dictionary of key, value pairs to be set before
       the job executes e.g. {'MYVAR':3}

       The standard application environment variables are always set so this
       is intended for user variables only.

       Example usage:

       >>> job = Job()
       >>> job.setExecutionEnv({'<MYVARIABLE>':'<VALUE>'})

       :param environmentDict: Environment variables
       :type environmentDict: dictionary
    """
    kwargs = {'environmentDict':environmentDict}
    if not type( environmentDict ) == type( {} ):
      return self._reportError( 'Expected dictionary of environment variables', **kwargs )

    if environmentDict:
      environment = []
      for var, val in environmentDict.items():
        try:
          environment.append( '='.join( [str( var ), urllib.quote( str( val ) )] ) )
        except Exception:
          return self._reportError( 'Expected string for environment variable key value pairs', **kwargs )

      envStr = ';'.join( environment )
      description = 'Env vars specified by user'
      self._addParameter( self.workflow, 'ExecutionEnvironment', 'JDL', envStr, description )
    return S_OK()

  #############################################################################
  def execute( self ):
    """Developer function. Executes the job locally.
    """
    self.workflow.createCode()
    self.workflow.execute()

  #############################################################################
  def _getParameters( self ):
    """Developer function.
       Method to return the workflow parameters.
    """
    wfParams = {}
    params = self.workflow.parameters
    for par in params:
      wfParams[par.getName()] = par.getValue()
    return wfParams

  #############################################################################
  def _dumpParameters( self, showType = None ):
    """Developer function.
       Method to print the workflow parameters.
    """
    paramsDict = {}
    paramList = self.workflow.parameters
    for param in paramList:
      paramsDict[param.getName()] = {'type':param.getType(), 'value':param.getValue()}
    self.log.info( '--------------------------------------' )
    self.log.info( 'Workflow parameter summary:           ' )
    self.log.info( '--------------------------------------' )
    #print self.workflow.parameters
    #print params.getParametersNames()
    for name, _props in paramsDict.items():
      ptype = paramsDict[name]['type']
      value = paramsDict[name]['value']
      if showType:
        if ptype == showType:
          self.log.info( 'NAME: %s\nTYPE: %s\nVALUE: %s ' % ( name, ptype, value ) )
          self.log.info( '--------------------------------------' )
      else:
        self.log.info( 'NAME: %s\nTYPE: %s\nVALUE: %s ' % ( name, ptype, value ) )
        self.log.info( '--------------------------------------' )

  #############################################################################

  def __setJobDefaults( self ):
    """Set job default values. Note that the system configuration is set to "ANY".
    """
    self._addParameter( self.workflow, 'JobType', 'JDL', self.type, 'Job Type' )
    self._addParameter( self.workflow, 'Priority', 'JDL', self.priority, 'User Job Priority' )
    self._addParameter( self.workflow, 'JobGroup', 'JDL', self.group, 'Name of the JobGroup' )
    self._addParameter( self.workflow, 'JobName', 'JDL', self.name, 'Name of Job' )
    #self._addParameter(self.workflow,'DIRACSetup','JDL',self.setup,'DIRAC Setup')
    self._addParameter( self.workflow, 'Site', 'JDL', self.site, 'Site Requirement' )
    self._addParameter( self.workflow, 'Origin', 'JDL', self.origin, 'Origin of client' )
    self._addParameter( self.workflow, 'StdOutput', 'JDL', self.stdout, 'Standard output file' )
    self._addParameter( self.workflow, 'StdError', 'JDL', self.stderr, 'Standard error file' )
    self._addParameter( self.workflow, 'InputData', 'JDL', '', 'Default null input data value' )
    self._addParameter( self.workflow, 'LogLevel', 'JDL', self.logLevel, 'Job Logging Level' )
    #Those 2 below are need for on-site resolution
    self._addParameter( self.workflow, 'ParametricInputData', 'string', '',
                        'Default null parametric input data value' )
    self._addParameter( self.workflow, 'ParametricInputSandbox', 'string', '',
                        'Default null parametric input sandbox value' )
    self._addParameter( self.workflow, 'ParametricParameters', 'string', '',
                        'Default null parametric input parameters value' )

  #############################################################################
  def _addParameter( self, wObject, name, ptype, value, description, io = 'input' ):
    """ Internal Function

        Adds a parameter to the object.
    """
    if io == 'input':
      inBool = True
      outBool = False
    elif io == 'output':
      inBool = False
      outBool = True
    else:
      raise TypeError, 'I/O flag is either input or output'

    par = Parameter( name, value, ptype, "", "", inBool, outBool, description )
    wObject.addParameter( Parameter( parameter = par ) )
    return par

  ############################################################################
  def _resolveInputSandbox( self, inputSandbox ):
    """ Internal function.

        Resolves wildcards for input sandbox files.  This is currently linux
        specific and should be modified.
    """
    resolvedIS = []
    for i in inputSandbox:
      if not re.search( '\*', i ):
        if not os.path.isdir( i ):
          resolvedIS.append( i )

    for name in inputSandbox:
      if re.search( '\*', name ): #escape the star character...
        cmd = 'ls -d ' + name
        output = shellCall( 10, cmd )
        if not output['OK']:
          self.log.error( 'Could not perform: ', cmd )
        elif output['Value'][0]:
          self.log.error(" Failed getting the files ", output['Value'][2])
        else:
          files = output['Value'][1].split()
          for check in files:
            if os.path.isfile( check ):
              self.log.verbose( 'Found file ' + check + ' appending to Input Sandbox' )
              resolvedIS.append( check )
            if os.path.isdir( check ):
              if re.search( '/$', check ): #users can specify e.g. /my/dir/lib/
                check = check[:-1]
              tarname = os.path.basename( check )
              directory = os.path.dirname( check ) #if just the directory this is null
              if directory:
                cmd = 'tar cfz ' + tarname + '.tar.gz ' + ' -C ' + directory + ' ' + tarname
              else:
                cmd = 'tar cfz ' + tarname + '.tar.gz ' + tarname

              output = shellCall( 60, cmd )
              if not output['OK']:
                self.log.error( 'Could not perform: %s' % ( cmd ) )
              resolvedIS.append( tarname + '.tar.gz' )
              self.log.verbose( 'Found directory ' + check + ', appending ' + check + '.tar.gz to Input Sandbox' )

      if os.path.isdir( name ):
        self.log.verbose( 'Found specified directory ' + name + ', appending ' + name + '.tar.gz to Input Sandbox' )
        if re.search( '/$', name ): #users can specify e.g. /my/dir/lib/
          name = name[:-1]
        tarname = os.path.basename( name )
        directory = os.path.dirname( name ) #if just the directory this is null
        if directory:
          cmd = 'tar cfz ' + tarname + '.tar.gz ' + ' -C ' + directory + ' ' + tarname
        else:
          cmd = 'tar cfz ' + tarname + '.tar.gz ' + tarname

        output = shellCall( 60, cmd )
        if not output['OK']:
          self.log.error( 'Could not perform: %s' % ( cmd ) )
        else:
          resolvedIS.append( tarname + '.tar.gz' )

    return resolvedIS

  #############################################################################

  def _toXML( self ):
    """Creates an XML representation of itself as a Job.
    """
    return self.workflow.toXML()

  #############################################################################
  def _toJDL( self, xmlFile = '' ): #messy but need to account for xml file being in /tmp/guid dir
    """Creates a JDL representation of itself as a Job.
    """
    #Check if we have to do old bootstrap...
    classadJob = ClassAd( '[]' )

    paramsDict = {}
    params = self.workflow.parameters # ParameterCollection object

    paramList = params
    for param in paramList:
      paramsDict[param.getName()] = {'type':param.getType(), 'value':param.getValue()}

    scriptname = 'jobDescription.xml'
    arguments = []
    if self.script:
      if os.path.exists( self.script ):
        scriptname = os.path.abspath( self.script )
        self.log.verbose( 'Found script name %s' % scriptname )
    else:
      if xmlFile:
        self.log.verbose( 'Found XML File %s' % xmlFile )
        scriptname = xmlFile

    arguments.append( os.path.basename( scriptname ) )
    self.addToInputSandbox.append( scriptname )
    if paramsDict.has_key( 'LogLevel' ):
      if paramsDict['LogLevel']['value']:
        arguments.append( '-o LogLevel=%s' % ( paramsDict['LogLevel']['value'] ) )
      else:
        self.log.warn( 'Job LogLevel defined with null value' )
    if paramsDict.has_key( 'DIRACSetup' ):
      if paramsDict['DIRACSetup']['value']:
        arguments.append( '-o DIRAC/Setup=%s' % ( paramsDict['DIRACSetup']['value'] ) )
      else:
        self.log.warn( 'Job DIRACSetup defined with null value' )
    if paramsDict.has_key( 'JobMode' ):
      if paramsDict['JobMode']['value']:
        arguments.append( '-o JobMode=%s' % ( paramsDict['JobMode']['value'] ) )
      else:
        self.log.warn( 'Job Mode defined with null value' )
    if paramsDict.has_key( 'JobConfigArgs' ):
      if paramsDict['JobConfigArgs']['value']:
        arguments.append( '%s' % ( paramsDict['JobConfigArgs']['value'] ) )
      else:
        self.log.warn( 'JobConfigArgs defined with null value' )

    classadJob.insertAttributeString( 'Executable', self.executable )
    self.addToOutputSandbox.append( self.stderr )
    self.addToOutputSandbox.append( self.stdout )

    #Extract i/o sandbox parameters from steps and any input data parameters
    #to do when introducing step-level api...

    #To add any additional files to input and output sandboxes
    if self.addToInputSandbox:
      extraFiles = ';'.join( self.addToInputSandbox )
      if paramsDict.has_key( 'InputSandbox' ):
        currentFiles = paramsDict['InputSandbox']['value']
        finalInputSandbox = currentFiles + ';' + extraFiles
        uniqueInputSandbox = uniqueElements( finalInputSandbox.split( ';' ) )
        paramsDict['InputSandbox']['value'] = ';'.join( uniqueInputSandbox )
        self.log.verbose( 'Final unique Input Sandbox %s' % ( ';'.join( uniqueInputSandbox ) ) )
      else:
        paramsDict['InputSandbox'] = {}
        paramsDict['InputSandbox']['value'] = extraFiles
        paramsDict['InputSandbox']['type'] = 'JDL'

    if self.addToOutputSandbox:
      extraFiles = ';'.join( self.addToOutputSandbox )
      if paramsDict.has_key( 'OutputSandbox' ):
        currentFiles = paramsDict['OutputSandbox']['value']
        finalOutputSandbox = currentFiles + ';' + extraFiles
        uniqueOutputSandbox = uniqueElements( finalOutputSandbox.split( ';' ) )
        paramsDict['OutputSandbox']['value'] = ';'.join( uniqueOutputSandbox )
        self.log.verbose( 'Final unique Output Sandbox %s' % ( ';'.join( uniqueOutputSandbox ) ) )
      else:
        paramsDict['OutputSandbox'] = {}
        paramsDict['OutputSandbox']['value'] = extraFiles
        paramsDict['OutputSandbox']['type'] = 'JDL'

    if self.addToInputData:
      extraFiles = ';'.join( self.addToInputData )
      if paramsDict.has_key( 'InputData' ):
        currentFiles = paramsDict['InputData']['value']
        finalInputData = extraFiles
        if currentFiles:
          finalInputData = currentFiles + ';' + extraFiles
        uniqueInputData = uniqueElements( finalInputData.split( ';' ) )
        paramsDict['InputData']['value'] = ';'.join( uniqueInputData )
        self.log.verbose( 'Final unique Input Data %s' % ( ';'.join( uniqueInputData ) ) )
      else:
        paramsDict['InputData'] = {}
        paramsDict['InputData']['value'] = extraFiles
        paramsDict['InputData']['type'] = 'JDL'

    # Handle here the Parametric values
    if self.parametric:
      for pType in ['InputData', 'InputSandbox']:
        if self.parametric.has_key( pType ):
          if paramsDict.has_key( pType ) and paramsDict[pType]['value']:
            pData = self.parametric[pType]
            # List of lists case
            currentFiles = paramsDict[pType]['value'].split( ';' )
            tmpList = []
            if type( pData[0] ) == list:
              for pElement in pData:
                tmpList.append( currentFiles + pElement )
            else:
              for pElement in pData:
                tmpList.append( currentFiles + [pElement] )
            self.parametric[pType] = tmpList

          paramsDict[pType] = {}
          paramsDict[pType]['value'] = "%s"
          paramsDict[pType]['type'] = 'JDL'
          self.parametric['files'] = self.parametric[pType]
          arguments.append( ' -p Parametric' + pType + '=%s' )
          break

      if self.parametric.has_key( 'files' ):
        paramsDict['Parameters'] = {}
        paramsDict['Parameters']['value'] = self.parametric['files']
        paramsDict['Parameters']['type'] = 'JDL'
      if self.parametric.has_key( 'GenericParameters' ):
        paramsDict['Parameters'] = {}
        paramsDict['Parameters']['value'] = self.parametric['GenericParameters']
        paramsDict['Parameters']['type'] = 'JDL'
        arguments.append( ' -p ParametricParameters=%s' )
    ##This needs to be put here so that the InputData and/or InputSandbox parameters for parametric jobs are processed
    classadJob.insertAttributeString( 'Arguments', ' '.join( arguments ) )

    #Add any JDL parameters to classad obeying lists with ';' rule
    for name, props in paramsDict.items():
      ptype = props['type']
      value = props['value']
      if name.lower() == 'requirements' and ptype == 'JDL':
        self.log.verbose( 'Found existing requirements: %s' % ( value ) )

      if re.search( '^JDL', ptype ):
        if type( value ) == list:
          if type( value[0] ) == list:
            classadJob.insertAttributeVectorStringList( name, value )
          else:
            classadJob.insertAttributeVectorString( name, value )
        elif value == "%s":
          classadJob.insertAttributeInt( name, value )
        elif not re.search( ';', value ) or name == 'GridRequirements': #not a nice fix...
          classadJob.insertAttributeString( name, value )
        else:
          classadJob.insertAttributeVectorString( name, value.split( ';' ) )

    self.addToInputSandbox.remove( scriptname )
    self.addToOutputSandbox.remove( self.stdout )
    self.addToOutputSandbox.remove( self.stderr )
    jdl = classadJob.asJDL()
    start = jdl.find( '[' )
    end = jdl.rfind( ']' )
    return jdl[( start + 1 ):( end - 1 )]

  #############################################################################
  def _setParamValue( self, name, value ):
    """Internal Function. Sets a parameter value, used for production.
    """
    return self.workflow.setValue( name, value )

  #############################################################################
  def _addJDLParameter( self, name, value ):
    """Developer function, add an arbitrary JDL parameter.
    """
    self._addParameter( self.workflow, name, 'JDL', value, 'Optional JDL parameter added' )
    return self.workflow.setValue( name, value )

  #############################################################################

  def runLocal( self, dirac = None ):
    """ The dirac (API) object is for local submission.
    """

    if dirac is None:
      dirac = Dirac()

    return dirac.submit( self, mode = 'local' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
