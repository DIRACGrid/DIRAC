########################################################################
# $HeadURL$
# File :    Job.py
# Author :  Stuart Paterson
########################################################################
"""
   Job Base Class

   This class provides generic job definition functionality suitable for any VO.

   Helper functions are documented with example usage for the DIRAC API.  An example
   script (for a simple executable) would be::

     from DIRAC.Interfaces.API.Dirac import Dirac
     from DIRAC.Interfaces.API.Job import Job

     j = Job()
     j.setCPUTime(500)
     j.setSystemConfig('slc4_ia32_gcc34')
     j.setExecutable('/bin/echo hello')
     j.setExecutable('yourPythonScript.py')
     j.setExecutable('/bin/echo hello again')
     j.setName('MyJobName')

     dirac = Dirac()
     jobID = dirac.submit(j)
     print 'Submission Result: ',jobID

   Note that several executables can be provided and wil be executed sequentially.
"""

from DIRAC.Core.Base import Script
Script.initialize()

__RCSID__ = "$Id$"

import string, re, os, time, shutil, types, copy

from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import ClassAd
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers       import getVO
from DIRAC.Core.Utilities.Subprocess                import shellCall
from DIRAC.Core.Utilities.List                      import uniqueElements
from DIRAC.Core.Utilities.SiteCEMapping             import getSiteForCE, getSiteCEMapping
from DIRAC                                          import gLogger

COMPONENT_NAME = '/Interfaces/API/Job'

class Job:

  #############################################################################

  def __init__( self, script = None, stdout = 'std.out', stderr = 'std.err' ):
    """Instantiates the Workflow object and some default parameters.
    """
    self.log = gLogger
    self.section = COMPONENT_NAME
    self.dbg = False
    if gConfig.getValue( self.section + '/LogLevel', 'DEBUG' ) == 'DEBUG':
      self.dbg = True

    #gConfig.getValue('Tier0SE-tape','SEName')
    self.stepCount = 0
    self.owner = 'NotSpecified'
    self.name = 'Name'
    self.type = 'User'
    self.priority = 1
    self.group = getVO( 'lhcb' )
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
    self.reqParams = {'MaxCPUTime':   'other.NAME>=VALUE',
                      'MinCPUTime':   'other.NAME<=VALUE',
                      'Site':         'other.NAME=="VALUE"',
                      'Platform':     'other.NAME=="VALUE"',
                      #'BannedSites':  '!Member(other.Site,BannedSites)', #doesn't work unfortunately
                      'BannedSites':  'other.Site!="VALUE"',
                      'SystemConfig': 'Member("VALUE",other.CompatiblePlatforms)'}
    ##Add member to handle Parametric jobs
    self.parametric = {}
    self.script = script
    if not script:
      self.workflow = Workflow()
      self.__setJobDefaults()
    else:
      self.workflow = Workflow( script )

    #Global error dictionary
    self.errorDict = {}

  #############################################################################
  def setExecutable( self, executable, arguments = '', logFile = '' ):
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
    """
    kwargs = {'executable':executable, 'arguments':arguments, 'logFile':logFile}
    if not type( executable ) == type( ' ' ) or not type( arguments ) == type( ' ' ) or not type( logFile ) == type( ' ' ):
      return self._reportError( 'Expected strings for executable and arguments', **kwargs )

    if os.path.exists( executable ):
      self.log.verbose( 'Found script executable file %s' % ( executable ) )
      self.addToInputSandbox.append( executable )
      logName = '%s.log' % ( os.path.basename( executable ) )
      moduleName = os.path.basename( executable )
    else:
      self.log.verbose( 'Found executable code' )
      logName = 'CodeOutput.log'
      moduleName = 'CodeSegment'

    if logFile:
      if type( logFile ) == type( ' ' ):
        logName = logFile

    self.stepCount += 1

    moduleName = moduleName.replace( '.', '' )
    stepNumber = self.stepCount
    stepDefn = 'ScriptStep%s' % ( stepNumber )
    step = self.__getScriptStep( stepDefn )
    stepName = 'RunScriptStep%s' % ( stepNumber )
    logPrefix = 'Script%s_' % ( stepNumber )
    logName = '%s%s' % ( logPrefix, logName )
    self.addToOutputSandbox.append( logName )
    self.workflow.addStep( step )

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance( stepDefn, stepName )
    stepInstance.setValue( "name", moduleName )
    stepInstance.setValue( "logFile", logName )
    stepInstance.setValue( "executable", executable )
    if arguments:
      stepInstance.setValue( "arguments", arguments )

    return S_OK()

  #############################################################################
  def setName( self, jobName ):
    """Helper function.

       A name for the job can be specified if desired. This will appear
       in the JobName field of the monitoring webpage. If nothing is
       specified a default value will appear.

       Example usage:

       >>> job=Job()
       >>> job.setName("myJobName")

       @param jobName: Name of job
       @type jobName: string
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

       @param files: Input sandbox files, can specify full path
       @type files: Single string or list of strings ['','']
    """
    if type( files ) == list and len( files ):
      resolvedFiles = self._resolveInputSandbox( files )
      fileList = string.join( resolvedFiles, ";" )
      description = 'Input sandbox file list'
      self._addParameter( self.workflow, 'InputSandbox', 'JDL', fileList, description )
      #self.sandboxFiles=resolvedFiles
    elif type( files ) == type( " " ):
      resolvedFiles = self._resolveInputSandbox( [files] )
      fileList = string.join( resolvedFiles, ";" )
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

       @param files: Logical File Names
       @type files: Single LFN string or list of LFNs
    """
    kwargs = {'files':files}
    if type( files ) == list and len( files ):
      for file in files:
        if not file.lower().count("lfn:"):
          return self._reportError('All files should be LFNs', **kwargs )
      resolvedFiles = self._resolveInputSandbox( files )
      fileList = string.join( resolvedFiles, ";" )
      self.parametric['InputSandbox']=fileList
      #self.sandboxFiles=resolvedFiles
    elif type( files ) == type( " " ):
      if not files.lower().count("lfn:"):
        return self._reportError('All files should be LFNs', **kwargs )
      resolvedFiles = self._resolveInputSandbox( [files] )
      fileList = string.join( resolvedFiles, ";" )
      self.parametric['InputSandbox']=fileList
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

       @param files: Output sandbox files
       @type files: Single string or list of strings ['','']

    """
    if type( files ) == list and len( files ):
      fileList = string.join( files, ";" )
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

       @param lfns: Logical File Names
       @type lfns: Single LFN string or list of LFNs
    """
    if type( lfns ) == list and len( lfns ):
      for i in xrange( len( lfns ) ):
        lfns[i] = lfns[i].replace( 'LFN:', '' )
      inputData = map( lambda x: 'LFN:' + x, lfns )
      inputDataStr = string.join( inputData, ';' )
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

       @param lfns: Logical File Names
       @type lfns: Single LFN string or list of LFNs
    """
    if type( lfns ) == list and len( lfns ):
      for i in xrange( len( lfns ) ):
        lfns[i] = lfns[i].replace( 'LFN:', '' )
      inputData = map( lambda x: 'LFN:' + x, lfns )
      inputDataStr = string.join( inputData, ';' )
      self.parametric['InputData']=inputDataStr
    elif type( lfns ) == type( ' ' ):  #single LFN
      self.parametric['InputData']=lfns
    else:
      kwargs = {'lfns':lfns}
      return self._reportError( 'Expected lfn string or list of lfns for parametric input data', **kwargs )
    
    return S_OK()

  #############################################################################  
  def setGenericParametricInput(self, inputlist):
    """ Helper function
    
       Define a generic parametric job with this function. Should not be used when 
       the ParametricInputData of ParametricInputSandbox are used.
       
       @param inputlist: Input list of parameters to build the parametric job
       @type inputlist: list
    
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
    csSection = '/Operations/InputDataPolicy'
    possible = ['Download', 'Protocol']
    finalPolicy = ''
    for p in possible:
      if string.lower( policy ) == string.lower( p ):
        finalPolicy = p

    if not finalPolicy:
      return self._reportError( 'Expected one of %s for input data policy' % ( string.join( possible, ', ' ) ), __name__, **kwargs )

    jobPolicy = gConfig.getValue( '%s/%s' % ( csSection, finalPolicy ), '' )
    if not jobPolicy:
      return self._reportError( 'Could not get value for CS option %s/%s' % ( csSection, finalPolicy ), __name__, **kwargs )

    description = 'User specified input data policy'
    self._addParameter( self.workflow, 'InputDataPolicy', 'JDL', jobPolicy, description )

    if not dataScheduling and policy.lower() == 'download':
      self.log.verbose( 'Scheduling by input data is disabled, jobs will run anywhere and download input data' )
      self._addParameter( self.workflow, 'DisableDataScheduling', 'JDL', 'True', 'Disable scheduling by input data' )

    if not dataScheduling and policy.lower() != 'download':
      self.log.error( 'Expected policy to be "download" for bypassing data scheduling' )
      return self._reportError( 'Expected policy to be "download" for bypassing data scheduling', __name__, **kwargs )

    return S_OK()

  #############################################################################
  def setOutputData( self, lfns, OutputSE = [], OutputPath = '' ):
    """Helper function.

       For specifying output data to be registered in Grid storage.  If a list
       of OutputSEs are specified the job wrapper will try each in turn until
       successful.  If the OutputPath is specified this will appear only after
       / <VO> / user / <initial> / <username>
       directory.

       Example usage:

       >>> job = Job()
       >>> job.setOutputData(['DVNtuple.root'])

       @param lfns: Output data file or files
       @type lfns: Single string or list of strings ['','']
       @param OutputSE: Optional parameter to specify the Storage Element
       @param OutputPath: Optional parameter to specify part of the path in the storage (see above)
       Element to store data or files, e.g. CERN-tape
       @type OutputSE: string or list
       @type OutputPath: string
    """
    kwargs = {'lfns':lfns, 'OutputSE':OutputSE, 'OutputPath':OutputPath}
    if type( lfns ) == list and len( lfns ):
      outputDataStr = string.join( lfns, ';' )
      description = 'List of output data files'
      self._addParameter( self.workflow, 'OutputData', 'JDL', outputDataStr, description )
    elif type( lfns ) == type( " " ):
      description = 'Output data file'
      self._addParameter( self.workflow, 'OutputData', 'JDL', lfns, description )
    else:
      return self._reportError( 'Expected file name string or list of file names for output data', **kwargs )

    if OutputSE:
      description = 'User specified Output SE'
      if type( OutputSE ) in types.StringTypes:
        OutputSE = [OutputSE]
      elif type( OutputSE ) != types.ListType:
        return self._reportError( 'Expected string or list for OutputSE', **kwargs )
      OutputSE = ';'.join( OutputSE )
      self._addParameter( self.workflow, 'OutputSE', 'JDL', OutputSE, description )

    if OutputPath:
      description = 'User specified Output Path'
      if not type( OutputPath ) in types.StringTypes:
        return self._reportError( 'Expected string for OutputPath', **kwargs )
      # Remove leading "/" that might cause problems with os.path.join
      while OutputPath[0] == '/': OutputPath = OutputPath[1:]
      self._addParameter( self.workflow, 'OutputPath', 'JDL', OutputPath, description )

    return S_OK()

  #############################################################################
  def setPlatform( self, backend ):
    """Developer function.

       Choose submission pool on which job is executed e.g. DIRAC, LCG.
       Default in place for users.
    """
    #should add protection here for list of supported platforms
    kwargs = {'backend':backend}
    if not type( backend ) == type( " " ):
      return self._reportError( 'Expected string for platform', **kwargs )

    if not backend.lower() == 'any':
      self._addParameter( self.workflow, 'SubmitPools', 'JDL', backend, 'Platform type' )

    return S_OK()

  #############################################################################
  def setSystemConfig( self, config ):
    """Helper function.

       Choose system configuration (e.g. where user DLLs have been compiled). Default ANY in place
       for user jobs.  Available system configurations can be browsed
       via dirac.checkSupportedPlatforms() method.

       Example usage:

       >>> job=Job()
       >>> job.setSystemConfig("slc4_ia32_gcc34")

       @param config: architecture, CMTCONFIG value
       @type config: string
    """
    kwargs = {'config':config}
    if not type( config ) == type( " " ):
      return self._reportError( 'Expected string for system configuration', **kwargs )

    description = 'User specified system configuration for job'
    self._addParameter( self.workflow, 'SystemConfig', 'JDLReqt', config, description )
    self.systemConfig = config
    return S_OK()

  #############################################################################
  def setCPUTime( self, timeInSecs ):
    """Helper function.

       Under Development. Specify CPU time requirement in DIRAC units.

       Example usage:

       >>> job = Job()
       >>> job.setCPUTime(5000)

       @param timeInSecs: CPU time
       @type timeInSecs: Int
    """
    kwargs = {'timeInSecs':timeInSecs}
    if not type( timeInSecs ) == int:
      try:
        timeInSecs = int( timeInSecs )
      except Exception, x:
        if not re.search( '{{', timeInSecs ):
          return self._reportError( 'Expected numerical string or int for CPU time in seconds', **kwargs )

    description = 'CPU time in secs'
    self._addParameter( self.workflow, 'MaxCPUTime', 'JDLReqt', timeInSecs, description )
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

       @param destination: site string
       @type destination: string or list
    """
    kwargs = {'destination':destination}
    if type( destination ) == type( "  " ):
      if not re.search( '^DIRAC.', destination ) and not destination.lower() == 'any':
        result = self.__checkSiteIsValid( destination )
        if not result['OK']:
          return self._reportError( '%s is not a valid destination site' % ( destination ), **kwargs )
      description = 'User specified destination site'
      self._addParameter( self.workflow, 'Site', 'JDLReqt', destination, description )
    elif type( destination ) == list:
      for site in destination:
        if not re.search( '^DIRAC.', site ) and not site.lower() == 'any':
          result = self.__checkSiteIsValid( site )
          if not result['OK']:
            return self._reportError( '%s is not a valid destination site' % ( destination ), **kwargs )
      destSites = string.join( destination, ';' )
      description = 'List of sites selected by user'
      self._addParameter( self.workflow, 'Site', 'JDLReqt', destSites, description )
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
      return self.__reportError( diracSite['Message'], **kwargs )
    if not diracSite['Value']:
      return self.__reportErrror( 'No DIRAC site name found for CE %s' % ( ceName ), **kwargs )

    diracSite = diracSite['Value']
    self.setDestination( diracSite )
    self._addJDLParameter( 'GridRequiredCEs', ceName )
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

       @param sites: single site string or list
       @type sites: string or list
    """
    if type( sites ) == list and len( sites ):
      bannedSites = string.join( sites, ';' )
      description = 'List of sites excluded by user'
      self._addParameter( self.workflow, 'BannedSites', 'JDLReqt', bannedSites, description )
    elif type( sites ) == type( " " ):
      description = 'Site excluded by user'
      self._addParameter( self.workflow, 'BannedSites', 'JDLReqt', sites, description )
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
  def _setSoftwareTags( self, tags ):
    """Developer function. 

       Choose any software tags if desired.  These are not compulsory but will ensure jobs only
       arrive at an LCG site where the software is preinstalled.  Without the tags, missing software is
       installed automatically by the Job Agent.

       Example usage:

       >>> job=Job()
       >>> job.setSoftwareTags(['VO-lhcb-Brunel-v30r17','VO-lhcb-Boole-v12r10','VO-lhcb-Gauss-v25r12'])

       @param tags: software tags
       @type tags: string or list
    """
    if type( tags ) == type( " " ):
      self._addParameter( self.workflow, 'SoftwareTag', 'JDL', tags, 'VO software tag' )
    elif type( tags ) == list:
      swTags = string.join( tags, ';' )
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

       @param jobGroup: JobGroup name
       @type jobGroup: string
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

       @param logLevel: Logging level
       @type logLevel: string
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
  def setMode( self, mode ):
    """Developer function. Under development.
    """
    if not type( mode ) == type( " " ):
      return self._reportError( 'Expected string for job mode', **{'mode':mode} )

    description = 'Choose a different DIRAC job mode'
    self._addParameter( self.workflow, 'JobMode', 'JDL', mode, description )
    return S_OK()

  #############################################################################
  def selectSetup( self, setup ):
    """Developer function. Under development.
    """
    if not type( setup ) == type( " " ):
      return self._reportError( 'Expected string for DIRAC setup', **{'setup':setup} )

    description = 'Choose a different DIRAC setup in which to execute the job'
    self._addParameter( self.workflow, 'DIRACSetup', 'JDL', setup, description )
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
       >>> job.setExecutionEnviroment({'<MYVARIABLE>':'<VALUE>'})

       @param environmentDict: Environment variables
       @type environmentDict: dictionary
    """
    kwargs = {'environmentDict':environmentDict}
    if not type( environmentDict ) == type( {} ):
      return self._reportError( 'Expected dictionary of environment variables', **kwargs )

    environment = []
    for var, val in environmentDict.items():
      try:
        environment.append( string.join( [str( var ), str( val )], '=' ) )
      except Exception, x:
        return self._reportError( 'Expected string for environment variable key value pairs', **kwargs )

    envStr = string.join( environment, ';' )
    description = 'Env vars specified by user'
    self._addParameter( self.workflow, 'ExecutionEnvironment', 'JDL', envStr, description )
    return S_OK()

  #############################################################################
  def sendMail( self ):
    """Under development.
    """
    description = 'Optional flag to send email when jobs complete'
    self._addParameter( self.workflow, 'SendMail', 'JDL', 'True', description )

  #############################################################################
  def createCode( self ):
    """Developer function. Wrapper method to create the code.
    """
    return self.workflow.createCode()

  #############################################################################
  def execute( self ):
    """Developer function. Executes the job locally.
    """
    code = self.createCode()
    #eval(compile(code,'<string>','exec'))
    self.workflow.execute()

  #############################################################################
  def _getParameters( self ):
    """Developer function.
       Method to return the workflow parameters.
    """
    wfParams = {}
    params = self.workflow.parameters
    for p in params:
      wfParams[p.getName()] = p.getValue()
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
    for name, props in paramsDict.items():
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
    self._addParameter( self.workflow, 'JobGroup', 'JDL', self.group, 'Corresponding VOMS role' )
    self._addParameter( self.workflow, 'JobName', 'JDL', self.name, 'Name of Job' )
    #self._addParameter(self.workflow,'DIRACSetup','JDL',self.setup,'DIRAC Setup')
    self._addParameter( self.workflow, 'SystemConfig', 'JDLReqt', self.systemConfig, 'System configuration for job' )
    self._addParameter( self.workflow, 'Site', 'JDL', self.site, 'Site Requirement' )
    self._addParameter( self.workflow, 'Origin', 'JDL', self.origin, 'Origin of client' )
    self._addParameter( self.workflow, 'StdOutput', 'JDL', self.stdout, 'Standard output file' )
    self._addParameter( self.workflow, 'StdError', 'JDL', self.stderr, 'Standard error file' )
    self._addParameter( self.workflow, 'InputData', 'JDL', '', 'Default null input data value' )
    self._addParameter( self.workflow, 'LogLevel', 'JDL', self.logLevel, 'Job Logging Level' )
    #Those 2 below are need for on-site resolution
    self._addParameter( self.workflow, 'ParametricInputData', 'JDL', '', 'Default null parametric input data value' )
    self._addParameter( self.workflow, 'ParametricInputSandbox', 'JDL', '', 'Default null parametric input sandbox value' )

  #############################################################################
  def _addParameter( self, object, name, ptype, value, description, io = 'input' ):
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

    p = Parameter( name, value, ptype, "", "", inBool, outBool, description )
    object.addParameter( Parameter( parameter = p ) )
    return p

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

    for f in inputSandbox:
      if re.search( '\*', f ): #escape the star character...
        cmd = 'ls -d ' + f
        output = shellCall( 10, cmd )
        if not output['OK']:
          self.log.error( 'Could not perform: %s' % ( cmd ) )
        else:
          files = string.split( output['Value'] )
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

      if os.path.isdir( f ):
        self.log.verbose( 'Found specified directory ' + f + ', appending ' + f + '.tar.gz to Input Sandbox' )
        if re.search( '/$', f ): #users can specify e.g. /my/dir/lib/
          f = f[:-1]
        tarname = os.path.basename( f )
        directory = os.path.dirname( f ) #if just the directory this is null
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
  def __getScriptStep( self, name = 'Script' ):
    """Internal function. This method controls the definition for a script module.
    """
    # Create the script module first
    moduleName = 'Script'
    module = ModuleDefinition( moduleName )
    module.setDescription( 'A  script module that can execute any provided script.' )
    body = 'from DIRAC.Core.Workflow.Modules.Script import Script\n'
    module.setBody( body )
    # Create Step definition
    step = StepDefinition( name )
    step.addModule( module )
    moduleInstance = step.createModuleInstance( 'Script', name )
    # Define step parameters
    step.addParameter( Parameter( "name", "", "string", "", "", False, False, 'Name of executable' ) )
    step.addParameter( Parameter( "executable", "", "string", "", "", False, False, 'Executable Script' ) )
    step.addParameter( Parameter( "arguments", "", "string", "", "", False, False, 'Arguments for executable Script' ) )
    step.addParameter( Parameter( "logFile", "", "string", "", "", False, False, 'Log file name' ) )
    return step

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
      extraFiles = string.join( self.addToInputSandbox, ';' )
      if paramsDict.has_key( 'InputSandbox' ):
        currentFiles = paramsDict['InputSandbox']['value']
        finalInputSandbox = currentFiles + ';' + extraFiles
        uniqueInputSandbox = uniqueElements( finalInputSandbox.split( ';' ) )
        paramsDict['InputSandbox']['value'] = string.join( uniqueInputSandbox, ';' )
        self.log.verbose( 'Final unique Input Sandbox %s' % ( string.join( uniqueInputSandbox, ';' ) ) )
      else:
        paramsDict['InputSandbox'] = {}
        paramsDict['InputSandbox']['value'] = extraFiles
        paramsDict['InputSandbox']['type'] = 'JDL'

    if self.addToOutputSandbox:
      extraFiles = string.join( self.addToOutputSandbox, ';' )
      if paramsDict.has_key( 'OutputSandbox' ):
        currentFiles = paramsDict['OutputSandbox']['value']
        finalOutputSandbox = currentFiles + ';' + extraFiles
        uniqueOutputSandbox = uniqueElements( finalOutputSandbox.split( ';' ) )
        paramsDict['OutputSandbox']['value'] = string.join( uniqueOutputSandbox, ';' )
        self.log.verbose( 'Final unique Output Sandbox %s' % ( string.join( uniqueOutputSandbox, ';' ) ) )
      else:
        paramsDict['OutputSandbox'] = {}
        paramsDict['OutputSandbox']['value'] = extraFiles
        paramsDict['OutputSandbox']['type'] = 'JDL'

    if self.addToInputData:
      extraFiles = string.join( self.addToInputData, ';' )
      if paramsDict.has_key( 'InputData' ):
        currentFiles = paramsDict['InputData']['value']
        finalInputData = extraFiles
        if currentFiles:
          finalInputData = currentFiles + ';' + extraFiles
        uniqueInputData = uniqueElements( finalInputData.split( ';' ) )
        paramsDict['InputData']['value'] = string.join( uniqueInputData, ';' )
        self.log.verbose( 'Final unique Input Data %s' % ( string.join( uniqueInputData, ';' ) ) )
      else:
        paramsDict['InputData'] = {}
        paramsDict['InputData']['value'] = extraFiles
        paramsDict['InputData']['type'] = 'JDL'

    ###Here handle the Parametric values
    if self.parametric:
      if self.parametric.has_key('InputData'):
        if paramsDict.has_key('InputData'):
          if paramsDict['InputData']['value']:
            currentFiles = paramsDict['InputData']['value']+";%s"
            paramsDict['InputData']['value'] =currentFiles
          else:
            paramsDict['InputData'] = {}
            paramsDict['InputData']['value'] = "%s"
            paramsDict['InputData']['type'] = 'JDL'
        self.parametric['files']=  self.parametric['InputData']
        arguments.append(' -p ParametricInputData=%s')
      elif self.parametric.has_key('InputSandbox'):
        if paramsDict.has_key( 'InputSandbox' ):
          currentFiles = paramsDict['InputSandbox']['value']+";%s"
          paramsDict['InputSandbox']['value'] = currentFiles
        else:
          paramsDict['InputSandbox'] = {}
          paramsDict['InputSandbox']['value'] = '%s'
          paramsDict['InputSandbox']['type'] = 'JDL'
        self.parametric['files']=  self.parametric['InputSandbox']
        arguments.append(' -p ParametricInputSandbox=%s')
      if self.parametric.has_key('files'):   
        paramsDict['Parameters']={}
        paramsDict['Parameters']['value']=self.parametric['files']
        paramsDict['Parameters']['type'] = 'JDL'
      if self.parametric.has_key('GenericParameters'):
        paramsDict['Parameters']={}
        paramsDict['Parameters']['value']=self.parametric['GenericParameters']
        paramsDict['Parameters']['type'] = 'JDL'
    ##This needs to be put here so that the InputData and/or InputSandbox parameters for parametric jobs are processed
    classadJob.insertAttributeString( 'Arguments', string.join( arguments, ' ' ) )

    #Add any JDL parameters to classad obeying lists with ';' rule
    requirements = False
    for name, props in paramsDict.items():
      ptype = paramsDict[name]['type']
      value = paramsDict[name]['value']
      if name.lower() == 'requirements' and ptype == 'JDL':
        self.log.verbose( 'Found existing requirements: %s' % ( value ) )
        requirements = True

      if re.search( '^JDL', ptype ):
        if not re.search( ';', value ) or name == 'GridRequirements': #not a nice fix...
          classadJob.insertAttributeString( name, value )
        else:
          classadJob.insertAttributeVectorString( name, string.split( value, ';' ) )

    if not requirements:
      reqtsDict = self.reqParams
      exprn = ''
      plus = ''
      for name, props in paramsDict.items():
        ptype = paramsDict[name]['type']
        value = paramsDict[name]['value']
        if not ptype == 'dict':
          if ptype == 'JDLReqt':
            if value and not value.lower() == 'any':
              plus = ' && '
              if re.search( ';', value ):
                for val in value.split( ';' ):
                  exprn += reqtsDict[name].replace( 'NAME', name ).replace( 'VALUE', str( val ) ) + plus
              else:
                exprn += reqtsDict[name].replace( 'NAME', name ).replace( 'VALUE', str( value ) ) + plus

      if len( plus ):
        exprn = exprn[:-len( plus )]
      if not exprn:
        exprn = 'true'
      self.log.verbose( 'Requirements: %s' % ( exprn ) )
      #classadJob.set_expression('Requirements', exprn)

    self.addToInputSandbox.remove( scriptname )
    self.addToOutputSandbox.remove( self.stdout )
    self.addToOutputSandbox.remove( self.stderr )
    jdl = classadJob.asJDL()
    start = string.find( jdl, '[' )
    end = string.rfind( jdl, ']' )
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
  def _getErrors( self ):
    """Returns the dictionary of stored errors that will prevent submission or
       execution. 
    """
    return self.errorDict

  #############################################################################
  def _reportError( self, message, name = '', **kwargs ):
    """Internal Function. Gets caller method name and arguments, formats the 
       information and adds an error to the global error dictionary to be 
       returned to the user. 
    """
    className = name
    if not name:
      className = __name__
    methodName = sys._getframe( 1 ).f_code.co_name
    arguments = []
    for key in kwargs:
      if kwargs[key]:
        arguments.append( '%s = %s ( %s )' % ( key, kwargs[key], type( kwargs[key] ) ) )
    finalReport = 'Problem with %s.%s() call:\nArguments: %s\nMessage: %s\n' % ( className, methodName, string.join( arguments, ', ' ), message )
    if self.errorDict.has_key( methodName ):
      tmp = self.errorDict[methodName]
      tmp.append( finalReport )
      self.errorDict[methodName] = tmp
    else:
      self.errorDict[methodName] = [finalReport]
    self.log.verbose( finalReport )
    return S_ERROR( finalReport )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
