########################################################################
# $HeadURL$
# File :   Optimizer.py
# Author : Stuart Paterson
########################################################################
"""
  The Optimizer base class is an agent that polls for jobs with a specific
  status and minor status pair.  The checkJob method is overridden for all
  optimizer instances and associated actions are performed there.
"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.DB.JobDB         import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB  import JobLoggingDB
from DIRAC.AccountingSystem.Client.Types.Job         import Job as AccountingJob
from DIRAC.Core.Utilities.ClassAd.ClassAdLight       import ClassAd
from DIRAC.Core.Base.AgentModule                     import AgentModule
from DIRAC                                           import S_OK, S_ERROR

class OptimizerModule( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  #############################################################################
  def initialize( self, jobDB = False, logDB = False ):
    """ Initialization of the Optimizer Agent.
    """
    if not jobDB:
      self.jobDB = JobDB()
    else:
      self.jobDB = jobDB
    if not logDB:
      self.logDB = JobLoggingDB()
    else:
      self.logDB = logDB

    trailing = "Agent"
    optimizerName = self.am_getModuleParam( 'agentName' )
    if optimizerName[ -len( trailing ):].find( trailing ) == 0:
      optimizerName = optimizerName[ :-len( trailing ) ]
    self.am_setModuleParam( 'optimizerName', optimizerName )

    self.startingMinorStatus = self.am_getModuleParam( 'optimizerName' )
    self.startingMajorStatus = "Checking"
    self.failedStatus = self.am_getOption( "FailedJobStatus" , 'Failed' )
    self.requiredJobInfo = 'jdl'
    self.am_setOption( "PollingTime", 30 )

    return self.initializeOptimizer()

  def initializeOptimizer( self ):
    """ To be overwritten by inheriting class
    """
    return S_OK()

  #############################################################################
  def execute( self ):
    """ The main agent execution method
    """

    result = self.initializeOptimizer()
    if not result[ 'OK' ]:
      return result
    self._initResult = result[ 'Value' ]

    condition = { 'Status' : self.startingMajorStatus }
    if self.startingMinorStatus:
      condition[ 'MinorStatus' ] = self.startingMinorStatus

    result = self.jobDB.selectJobs( condition )
    if not result['OK']:
      self.log.warn( 'Failed to get a job list from the JobDB' )
      return S_ERROR( 'Failed to get a job list from the JobDB' )

    if not len( result['Value'] ):
      self.log.verbose( 'No pending jobs to process' )
      return S_OK( 'No work to do' )

    for job in result['Value']:
      result = self.getJobDefinition( job )
      if not result['OK']:
        self.setFailedJob( job, result[ 'Message' ], '' )
        continue
      jobDef = result[ 'Value' ]
      result = self.optimizeJob( job, jobDef[ 'classad' ] )

    return S_OK()

  #############################################################################
  def optimizeJob( self, job, classAdJob ):
    """ Call the corresponding Optimizer checkJob method
    """
    self.log.info( 'Job %s will be processed by %sAgent' % ( job, self.am_getModuleParam( 'optimizerName' ) ) )
    result = self.checkJob( job, classAdJob )
    if not result['OK']:
      self.setFailedJob( job, result['Message'], classAdJob )
    return result

  #############################################################################
  def getJobDefinition( self, job, jobDef = False ):
    """ Retrieve JDL of the Job and return jobDef dictionary
    """
    if jobDef == False:
      jobDef = {}
    #If not jdl in jobinfo load it
    if 'jdl' not in jobDef:
      if 'jdlOriginal' == self.requiredJobInfo:
        result = self.jobDB.getJobJDL( job, original = True )
        if not result[ 'OK' ]:
          self.log.error( "No JDL for job", "%s" % job )
          return S_ERROR( "No JDL for job" )
        jobDef[ 'jdl' ] = result[ 'Value' ]
      if 'jdl' == self.requiredJobInfo:
        result = self.jobDB.getJobJDL( job )
        if not result[ 'OK' ]:
          self.log.error( "No JDL for job", "%s" % job )
          return S_ERROR( "No JDL for job" )
        jobDef[ 'jdl' ] = result[ 'Value' ]
    #Load the classad if needed
    if 'jdl' in jobDef and not 'classad' in jobDef:
      try:
        classad = ClassAd( jobDef[ 'jdl' ] )
      except:
        self.log.debug( "Cannot load JDL" )
        return S_ERROR( 'Illegal Job JDL' )
      if not classad.isOK():
        self.log.debug( "Warning: illegal JDL for job %s, will be marked problematic" % ( job ) )
        return S_ERROR( 'Illegal Job JDL' )
      jobDef[ 'classad' ] = classad
    return S_OK( jobDef )

  #############################################################################
  def getOptimizerJobInfo( self, job, reportName ):
    """This method gets job optimizer information that will
       be used for
    """
    self.log.verbose( "self.jobDB.getJobOptParameter(%s,'%s')" % ( job, reportName ) )
    result = self.jobDB.getJobOptParameter( job, reportName )
    if result['OK']:
      value = result['Value']
      if not value:
        self.log.warn( 'JobDB returned null value for %s %s' % ( job, reportName ) )
        return S_ERROR( 'No optimizer info returned' )
      else:
        try:
          return S_OK( eval( value ) )
        except Exception, x:
          return S_ERROR( 'Could not evaluate optimizer parameters' )

    return result

  #############################################################################
  def setOptimizerJobInfo( self, job, reportName, value ):
    """This method sets the job optimizer information that will subsequently
       be used for job scheduling and TURL queries on the WN.
    """
    self.log.verbose( "self.jobDB.setJobOptParameter(%s,'%s','%s')" % ( job, reportName, value ) )
    if not self.am_Enabled():
      return S_OK()
    return self.jobDB.setJobOptParameter( job, reportName, str( value ) )

  #############################################################################
  def setOptimizerChain( self, job, value ):
    """This method sets the job optimizer chain, in principle only needed by
       one of the optimizers.
    """
    self.log.verbose( "self.jobDB.setOptimizerChain(%s,%s)" % ( job, value ) )
    if not self.am_Enabled():
      return S_OK()
    return self.jobDB.setOptimizerChain( job, value )

  #############################################################################
  def setNextOptimizer( self, job ):
    """This method is executed when the optimizer instance has successfully
       processed the job.  The next optimizer in the chain will subsequently
       start to work on the job.
    """

    result = self.logDB.addLoggingRecord( job, status = self.startingMajorStatus,
                                               minor = self.startingMinorStatus,
                                               source = self.am_getModuleParam( "optimizerName" ) )
    if not result['OK']:
      self.log.warn( result['Message'] )

    self.log.verbose( "self.jobDB.setNextOptimizer(%s,'%s')" % ( job, self.am_getModuleParam( "optimizerName" ) ) )
    return self.jobDB.setNextOptimizer( job, self.am_getModuleParam( "optimizerName" ) )


  #############################################################################
  def updateJobStatus( self, job, status, minorStatus = None, appStatus = None ):
    """This method updates the job status in the JobDB, this should only be
       used to fail jobs due to the optimizer chain.
    """
    self.log.verbose( "self.jobDB.setJobStatus(%s,'Status/Minor/Application','%s'/'%s'/'%s',update=True)" %
                      ( job, status, str( minorStatus ), str( appStatus ) ) )
    if not self.am_Enabled():
      return S_OK()

    result = self.jobDB.setJobStatus( job, status, minorStatus, appStatus )
    if not result['OK']:
      return result

    result = self.logDB.addLoggingRecord( job, status = status, minor = minorStatus, application = appStatus,
                                          source = self.am_getModuleParam( 'optimizerName' ) )
    if not result['OK']:
      self.log.warn ( result['Message'] )

    return S_OK()

  #############################################################################
  def setJobParam( self, job, reportName, value ):
    """This method updates a job parameter in the JobDB.
    """
    self.log.verbose( "self.jobDB.setJobParameter(%s,'%s','%s')" % ( job, reportName, value ) )
    if not self.am_Enabled():
      return S_OK()
    return self.jobDB.setJobParameter( job, reportName, value )

  #############################################################################
  def setFailedJob( self, job, msg, classAdJob = None ):
    """This method moves the job to the failed status
    """
    self.log.verbose( "self.updateJobStatus(%s,'%s','%s')" % ( job, self.failedStatus, msg ) )
    if not self.am_Enabled():
      return S_OK()
    self.updateJobStatus( job, self.failedStatus, msg )
    if classAdJob:
      self.sendAccountingRecord( job, msg, classAdJob )

  #############################################################################
  def checkJob( self, job, classad ):
    """This method controls the checking of the job, should be overridden in a subclass
    """
    self.log.warn( 'Optimizer: checkJob method should be implemented in a subclass' )
    return S_ERROR( 'Optimizer: checkJob method should be implemented in a subclass' )

  #############################################################################
  def sendAccountingRecord( self, job, msg, classAdJob ):
    """
      Send and accounting record for the failed job
    """
    accountingReport = AccountingJob()
    accountingReport.setStartTime()
    accountingReport.setEndTime()

    owner = classAdJob.getAttributeString( 'Owner' )
    userGroup = classAdJob.getAttributeString( 'OwnerGroup' )
    jobGroup = classAdJob.getAttributeString( 'JobGroup' )
    jobType = classAdJob.getAttributeString( 'JobType' )
    jobClass = 'unknown'
    if classAdJob.lookupAttribute( 'HerdState' ):
      jobClass = classAdJob.getAttributeString( 'HerdState' )
    inputData = []
    processingType = 'unknown'
    if classAdJob.lookupAttribute( 'ProcessingType' ):
      processingType = classAdJob.getAttributeString( 'ProcessingType' )
    if classAdJob.lookupAttribute( 'InputData' ):
      inputData = classAdJob.getListFromExpression( 'InputData' )
    inputDataFiles = len( inputData )
    outputData = []
    if classAdJob.lookupAttribute( 'OutputData' ):
      outputData = classAdJob.getListFromExpression( 'OutputData' )
    outputDataFiles = len( outputData )

    acData = {
               'User' : owner,
               'UserGroup' : userGroup,
               'JobGroup' : jobGroup,
               'JobType' : jobType,
               'JobClass' : jobClass,
               'ProcessingType' : processingType,
               'FinalMajorStatus' : self.failedStatus,
               'FinalMinorStatus' : msg,
               'CPUTime' : 0.0,
               'NormCPUTime' : 0.0,
               'ExecTime' : 0.0,
               'InputDataSize' : 0.0,
               'OutputDataSize' : 0.0,
               'InputDataFiles' : inputDataFiles,
               'OutputDataFiles' : outputDataFiles,
               'DiskSpace' : 0.0,
               'InputSandBoxSize' : 0.0,
               'OutputSandBoxSize' : 0.0,
               'ProcessedEvents' : 0.0
             }

    self.log.verbose( 'Accounting Report is:' )
    self.log.verbose( acData )
    accountingReport.setValuesFromDict( acData )
    return accountingReport.commit()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
