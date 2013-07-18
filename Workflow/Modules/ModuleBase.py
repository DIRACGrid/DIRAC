""" ModuleBase - contains the base class for workflow modules. Defines several common utility methods.

    The modules defined within this package are developed in a way to be executed by a DIRAC.Core.Worfklow.Worfklow.
    In particular, a DIRAC.Core.Workflow.Worfklow object will only call the "execute" function, that is defined here.

    These modules, inspired by the LHCb experience, give the possibility to define simple user and production jobs.
    Many VOs might want to extend this package. And actually, for some cases, it will be necessary. For example,
    defining the LFN output at runtime (within the "UploadOutputs" module is a VO specific operation.

    The DIRAC APIs are used to create Jobs that make use of these modules.
"""

import os, copy

from DIRAC                                                  import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.WorkloadManagementSystem.Client.JobReport        import JobReport
from DIRAC.TransformationSystem.Client.FileReport           import FileReport
from DIRAC.RequestManagementSystem.Client.RequestContainer  import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager       import ReplicaManager


class ModuleBase( object ):
  """ Base class for Modules - works only within DIRAC workflows

      This module, inheriting by "object", can use cooperative methods, very useful here.
  """

  #############################################################################

  def __init__( self, loggerIn = None ):
    """ Initialization of module base.

        loggerIn is a logger object that can be passed so that the logging will be more clear.
    """

    if not loggerIn:
      self.log = gLogger.getSubLogger( 'ModuleBase' )
    else:
      self.log = loggerIn

    # These 2 are used in many places, so it's good to have them available here.
    self.opsH = Operations()
    self.rm = ReplicaManager()

    # Some job parameters
    self.production_id = ''
    self.prod_job_id = ''
    self.jobID = 0
    self.step_number = ''
    self.step_id = ''
    self.jobType = ''
    self.executable = ''
    self.command = None

    self.workflowStatus = None
    self.stepStatus = None
    self.workflow_commons = None
    self.step_commons = None

    # These are useful objects (see the getFileReporter(), getJobReporter() and getRequestContainer() functions)
    self.fileReport = None
    self.jobReport = None
    self.request = None

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Function called by all super classes. This is the only function that Workflow will call automatically.

        The design adopted here is that all the modules are inheriting from this class,
        and will NOT override this function. Instead, the inherited modules will override the following functions:
        _resolveInputVariables()
        _initialize()
        _setCommand()
        _executeCommand()
        _execute()
        that are called here exactly in this order.
        Each implementation of these functions, in the subclasses, should never return S_OK, S_ERROR.
        This choice has been made for convenience of coding, and for the high level of inheritance implemented here.
        Instead, they should return:
        - None when no issues arise
        - a RuntimeError exception when there are issues
        - a GracefulTermination exception (defined also here) when the module should be terminated gracefully

        The various parameters in input to this method are used almost only for testing purposes.
    """

    if production_id:
      self.production_id = int( production_id )
    else:
      # self.PRODUCTION_ID is always set by the workflow
      self.production_id = int( self.PRODUCTION_ID )

    if prod_job_id:
      self.prod_job_id = int( prod_job_id )
    else:
      # self.JOB_ID is set by the workflow, but this is not the WMS job id, but the transformation (production) task id
      self.prod_job_id = int( self.JOB_ID )

    if os.environ.has_key( 'JOBID' ):
      # this is the real wms job ID
      self.jobID = int( os.environ['JOBID'] )
    if wms_job_id:
      self.jobID = int( wms_job_id )

    if workflowStatus:
      self.workflowStatus = workflowStatus

    if stepStatus:
      self.stepStatus = stepStatus

    if wf_commons:
      self.workflow_commons = wf_commons

    if step_commons:
      self.step_commons = step_commons

    if step_number:
      self.step_number = int( step_number )
    else:
      # self.STEP_NUMBER is always set by the workflow
      self.step_number = int( self.STEP_NUMBER )

    if step_id:
      self.step_id = step_id
    else:
      self.step_id = '%s_%s_%s' % ( str( self.production_id ), str( self.prod_job_id ), str( self.step_number ) )

    try:
      # This is what has to be extended in the modules
      self._resolveInputVariables()
      self._initialize()
      self._setCommand()
      self._executeCommand()
      self._execute()
      self._finalize()

    # If everything is OK
    except GracefulTermination, status:
      self.setApplicationStatus( status )
      self.log.info( status )
      return S_OK( status )

    # This catches everything that is voluntarily thrown within the modules, so an error
    except RuntimeError, e:
      self.log.error( e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    # This catches everything that is not voluntarily thrown (here, really writing an exception)
    except Exception, e:
      self.log.exception( e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    finally:
      self.finalize()

  def _resolveInputVariables( self ):
    """ By convention the module input parameters are resolved here.
        fileReport, jobReport, and request objects are instantiated/recorded here.

        This will also call the resolution of the input workflow.
        The resolution of the input step should instead be done on a step basis.

        NB: Never forget to call this base method when extending it.
    """

    self.log.verbose( "workflow_commons = ", self.workflow_commons )
    self.log.verbose( "step_commons = ", self.step_commons )

    if not self.fileReport:
      self.fileReport = self._getFileReporter()
    if not self.jobReport:
      self.jobReport = self._getJobReporter()
    if not self.request:
      self.request = self._getRequestContainer()

    self._resolveInputWorkflow()

  def _initialize( self ):
    """ TBE

        For initializing the module, whatever operation this can be
    """
    pass

  def _setCommand( self ):
    """ TBE

        For "executors" modules, set the command to be used in the self.command variable.
    """
    pass

  def _executeCommand( self ):
    """ TBE

        For "executors" modules, executes self.command as set in the _setCommand() method
    """
    pass

  def _execute( self ):
    """ TBE

        Executes, whatever this means for the module implementing it
    """
    pass

  def _finalize( self, status = '' ):
    """ TBE

        By default, the module finalizes correctly
    """
    
    if not status:
      status = '%s correctly finalized' % str( self.__class__ )

    raise GracefulTermination, status

  #############################################################################

  def finalize( self ):
    """ Just finalizing the module execution by flushing the logs. This will be done always.
    """

    self.log.flushAllMessages( 0 )
    self.log.info( '===== Terminating ' + str( self.__class__ ) + ' ===== ' )

  #############################################################################

  def _getJobReporter( self ):
    """ just return the job reporter (object, always defined by dirac-jobexec)
    """

    if self.workflow_commons.has_key( 'JobReport' ):
      return self.workflow_commons['JobReport']
    else:
      jobReport = JobReport( self.jobID )
      self.workflow_commons['JobReport'] = jobReport
      return jobReport

  #############################################################################

  def _getFileReporter( self ):
    """ just return the file reporter (object)
    """

    if self.workflow_commons.has_key( 'FileReport' ):
      return self.workflow_commons['FileReport']
    else:
      fileReport = FileReport()
      self.workflow_commons['FileReport'] = fileReport
      return fileReport

  #############################################################################

  def _getRequestContainer( self ):
    """ just return the RequestContainer reporter (object)
    """

    if self.workflow_commons.has_key( 'Request' ):
      return self.workflow_commons['Request']
    else:
      request = RequestContainer()
      self.workflow_commons['Request'] = request
      return request

  #############################################################################

  def _resolveInputWorkflow( self ):
    """ Resolve the input variables that are in the workflow_commons
    """

    if self.workflow_commons.has_key( 'JobType' ):
      self.jobType = self.workflow_commons['JobType']

    self.InputData = ''
    if self.workflow_commons.has_key( 'InputData' ):
      if self.workflow_commons['InputData']:
        self.InputData = self.workflow_commons['InputData']

    if self.workflow_commons.has_key( 'ParametricInputData' ):
      pID = copy.deepcopy( self.workflow_commons['ParametricInputData'] )
      if pID:
        if type( pID ) == type( [] ):
          pID = ';'.join( pID )
  #      self.InputData += ';' + pID
        self.InputData = pID
        self.InputData = self.InputData.rstrip( ';' )

    if self.InputData == ';':
      self.InputData = ''

    if self.workflow_commons.has_key( 'outputDataFileMask' ):
      self.outputDataFileMask = self.workflow_commons['outputDataFileMask']
      if not type( self.outputDataFileMask ) == type( [] ):
        self.outputDataFileMask = [i.lower().strip() for i in self.outputDataFileMask.split( ';' )]

  #############################################################################

  def _resolveInputStep( self ):
    """ Resolve the input variables for an application step
    """

    self.stepName = self.step_commons['STEP_INSTANCE_NAME']

    if self.step_commons.has_key( 'executable' ) and self.step_commons['executable']:
      self.executable = self.step_commons['executable']
    else:
      self.executable = 'Unknown'

    if self.step_commons.has_key( 'applicationName' ) and self.step_commons['applicationName']:
      self.applicationName = self.step_commons['applicationName']
    else:
      self.applicationName = 'Unknown'

    if self.step_commons.has_key( 'applicationVersion' ) and self.step_commons['applicationVersion']:
      self.applicationVersion = self.step_commons['applicationVersion']
    else:
      self.applicationVersion = 'Unknown'

    if self.step_commons.has_key( 'applicationLog' ):
      self.applicationLog = self.step_commons['applicationLog']

    stepInputData = []
    if self.step_commons.has_key( 'inputData' ):
      if self.step_commons['inputData']:
        stepInputData = self.step_commons['inputData']
    elif self.InputData:
      stepInputData = copy.deepcopy( self.InputData )
    if stepInputData:
      stepInputData = self._determineStepInputData( stepInputData, )
      self.stepInputData = [sid.strip( 'LFN:' ) for sid in stepInputData]

  #############################################################################

  def _determineStepInputData( self, inputData ):
    """ determine the input data for the step
    """
    if inputData == 'previousStep':
      stepIndex = self.gaudiSteps.index( self.stepName )
      previousStep = self.gaudiSteps[stepIndex - 1]

      stepInputData = []
      for outputF in self.workflow_commons['outputList']:
        try:
          if outputF['stepName'] == previousStep and outputF['outputDataType'].lower() == self.inputDataType.lower():
            stepInputData.append( outputF['outputDataName'] )
        except KeyError:
          raise RuntimeError, 'Can\'t find output of step %s' % previousStep

      return stepInputData

    else:
      return [x.strip( 'LFN:' ) for x in inputData.split( ';' )]

  #############################################################################

  def setApplicationStatus( self, status, sendFlag = True ):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self._WMSJob():
      return 0  # e.g. running locally prior to submission

    if self._checkWFAndStepStatus( noPrint = True ):
      # The application status won't be updated in case the workflow or the step is failed already
      if not type( status ) == type( '' ):
        status = str( status )
      self.log.verbose( 'setJobApplicationStatus(%d, %s)' % ( self.jobID, status ) )
      jobStatus = self.jobReport.setApplicationStatus( status, sendFlag )
      if not jobStatus['OK']:
        self.log.warn( jobStatus['Message'] )

  #############################################################################

  def _WMSJob( self ):
    """ Check if this job is running via WMS
    """
    return True if self.jobID else False

  #############################################################################

  def _enableModule( self ):
    """ Enable module if it's running via WMS
    """
    if not self._WMSJob():
      self.log.info( 'No WMS JobID found, disabling module via control flag' )
      return False
    else:
      self.log.verbose( 'Found WMS JobID = %d' % self.jobID )
      return True

  #############################################################################

  def _checkWFAndStepStatus( self, noPrint = False ):
    """ Check the WF and Step status
    """
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      if not noPrint:
        self.log.info( 'Skip this module, failure detected in a previous step :' )
        self.log.info( 'Workflow status : %s' % ( self.workflowStatus ) )
        self.log.info( 'Step Status : %s' % ( self.stepStatus ) )
      return False
    else:
      return True

  #############################################################################

  def setJobParameter( self, name, value, sendFlag = True ):
    """Wraps around setJobParameter of state update client
    """
    if not self._WMSJob():
      return 0  # e.g. running locally prior to submission

    self.log.verbose( 'setJobParameter(%d,%s,%s)' % ( self.jobID, name, value ) )

    jobParam = self.jobReport.setJobParameter( str( name ), str( value ), sendFlag )
    if not jobParam['OK']:
      self.log.warn( jobParam['Message'] )

  #############################################################################

  def setFileStatus( self, production, lfn, status, fileReport = None ):
    """ set the file status for the given production in the Transformation Database
    """
    self.log.verbose( 'setFileStatus(%s,%s,%s)' % ( production, lfn, status ) )

    if not fileReport:
      fileReport = self._getFileReporter()

    fileReport.setFileStatus( production, lfn, status )

  #############################################################################


  def getCandidateFiles( self, outputList, outputLFNs, fileMask, stepMask = '' ):
    """ Returns list of candidate files to upload, check if some outputs are missing.

        outputList has the following structure:
          [ {'outputDataType':'','outputDataSE':'','outputDataName':''} , {...} ]

        outputLFNs is the list of output LFNs for the job

        fileMask is the output file extensions to restrict the outputs to

        returns dictionary containing type, SE and LFN for files restricted by mask
    """
    fileInfo = {}

    for outputFile in outputList:
      if outputFile.has_key( 'outputDataType' ) \
      and outputFile.has_key( 'outputDataSE' ) \
      and outputFile.has_key( 'outputDataName' ):
        fname = outputFile['outputDataName']
        fileSE = outputFile['outputDataSE']
        fileType = outputFile['outputDataType']
        fileInfo[fname] = {'type':fileType, 'workflowSE':fileSE}
      else:
        self.log.error( 'Ignoring malformed output data specification', str( outputFile ) )

    for lfn in outputLFNs:
      if os.path.basename( lfn ) in fileInfo.keys():
        fileInfo[os.path.basename( lfn )]['lfn'] = lfn
        self.log.verbose( 'Found LFN %s for file %s' % ( lfn, os.path.basename( lfn ) ) )

    # check local existance
    self._checkLocalExistance( fileInfo.keys() )

    # Select which files have to be uploaded: in principle all
    candidateFiles = self._applyMask( fileInfo, fileMask, stepMask )

    # Sanity check all final candidate metadata keys are present (return S_ERROR if not)
    self._checkSanity( candidateFiles )

    return candidateFiles

  #############################################################################

  def _applyMask( self, candidateFilesIn, fileMask, stepMask ):
    """ Select which files have to be uploaded: in principle all
    """
    candidateFiles = copy.deepcopy( candidateFilesIn )

    if fileMask and type( fileMask ) != type( [] ):
      fileMask = [fileMask]
    if type( stepMask ) == type( 1 ):
      stepMask = str( stepMask )
    if stepMask and type( stepMask ) != type( [] ):
      stepMask = [stepMask]

    if fileMask and fileMask != ['']:
      for fileName, metadata in candidateFiles.items():
        if ( ( metadata['type'].lower() not in fileMask ) ):  # and ( fileName.split( '.' )[-1] not in fileMask ) ):
          del( candidateFiles[fileName] )
          self.log.info( 'Output file %s was produced but will not be treated (fileMask is %s)' % ( fileName,
                                                                                              ', '.join( fileMask ) ) )
    else:
      self.log.info( 'No outputDataFileMask provided, the files with all the extensions will be considered' )

    if stepMask and stepMask != ['']:
      # FIXME: This supposes that the LFN contains the step ID
      for fileName, metadata in candidateFiles.items():
        if fileName.split( '_' )[-1].split( '.' )[0] not in stepMask:
          del( candidateFiles[fileName] )
          self.log.info( 'Output file %s was produced but will not be treated (stepMask is %s)' % ( fileName,
                                                                                               ', '.join( stepMask ) ) )
    else:
      self.log.info( 'No outputDataStep provided, the files output of all the steps will be considered' )

    return candidateFiles

  #############################################################################

  def _checkSanity( self, candidateFiles ):
    """ Sanity check all final candidate metadata keys are present
    """

    notPresentKeys = []

    mandatoryKeys = ['type', 'workflowSE', 'lfn']  # filedict is used for requests
    for fileName, metadata in candidateFiles.items():
      for key in mandatoryKeys:
        if not metadata.has_key( key ):
          notPresentKeys.append( ( fileName, key ) )

    if notPresentKeys:
      for fileName_keys in notPresentKeys:
        self.log.error( 'File %s has missing %s' % ( fileName_keys[0], fileName_keys[1] ) )
      raise ValueError

  #############################################################################

  def _checkLocalExistance( self, fileList ):
    """ Check that the list of output files are present locally
    """

    notPresentFiles = []

    for fileName in fileList:
      if not os.path.exists( fileName ):
        notPresentFiles.append( fileName )

    if notPresentFiles:
      self.log.error( 'Output data file list %s does not exist locally' % notPresentFiles )
      raise os.error

  #############################################################################

#############################################################################

class GracefulTermination( Exception ):
  pass

#############################################################################
