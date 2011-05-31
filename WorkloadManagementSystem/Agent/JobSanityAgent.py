########################################################################
# $HeadURL$
# File :   JobSanityAgent.py
# Author : Stuart Paterson
########################################################################
"""
  The Job Sanity Agent accepts all jobs from the Job
  receiver and screens them for the following problems:
   - Output data already exists
   - Problematic JDL
   - Jobs with too much input data e.g. > 100 files
   - Jobs with input data incorrectly specified e.g. castor:/
   - Input sandbox not correctly uploaded.
"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC                                                 import S_OK, S_ERROR
from DIRAC.Core.Security                                   import CS
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient   import SandboxStoreClient
import re

class JobSanityAgent( OptimizerModule ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle      
  """

  #############################################################################
  def initializeOptimizer( self ):
    """Initialize specific parameters for JobSanityAgent.
    """
    #Test control flags N.B. JDL check is mandatory
    self.inputDataCheck = self.am_getOption( 'InputDataCheck', 1 )
    self.outputDataCheck = self.am_getOption( 'OutputDataCheck', 0 )
    self.inputSandboxCheck = self.am_getOption( 'InputSandboxCheck', 1 )
    self.platformCheck = self.am_getOption( 'PlatformCheck', 0 )
    #Other parameters
    self.successStatus = self.am_getOption( 'SuccessfulJobStatus', 'OutputReady' )
    self.maxDataPerJob = self.am_getOption( 'MaxInputDataPerJob', 100 )
    #Sandbox
    self.sandboxClient = SandboxStoreClient( useCertificates = True )

    self.log.debug( 'JDL Check          ==>  Enabled' )
    if self.inputDataCheck:
      self.log.debug( 'Input Data Check   ==>  Enabled' )
    else:
      self.log.debug( 'Input Data Check   ==>  Disabled' )
    if self.outputDataCheck:
      self.log.debug( 'Output Data Check  ==>  Enabled' )
    else:
      self.log.debug( 'Output Data Check  ==>  Disabled' )
    if self.inputSandboxCheck:
      self.log.debug( 'Input Sbox Check   ==>  Enabled' )
    else:
      self.log.debug( 'Input Sbox Check   ==>  Disabled' )
    if self.platformCheck:
      self.log.debug( 'Platform Check     ==>  Enabled' )
    else:
      self.log.debug( 'Platform Check     ==>  Disabled' )

    return S_OK()

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """ This method controls the order and presence of
        each sanity check for submitted jobs. This should
        be easily extended in the future to accommodate
        any other potential checks.
    """
    #Job JDL check
    message = "Job: %s JDL: OK," % job
    self.log.debug( "Checking Loop Starts for job %s" % job )

    jobType = self.jobDB.getJobAttribute( job, 'JobType' )
    if not jobType['OK']:
      return S_ERROR( 'Could not determine job type' )
    jobType = jobType['Value']

    #Input data check
    if self.inputDataCheck:
      voName = classAdJob.getAttributeString( "VirtualOrganization" )
      inputData = self.checkInputData( job, jobType, voName )
      if inputData['OK']:
        number = inputData['Value']
        message += 'InputData: ' + number + ', '
      else:
        minorStatus = inputData['Value']
        self.log.info( message )
        self.log.info( 'Job: ' + str( job ) + ' Failed input data check.' )
        return S_ERROR( minorStatus )

    #Platform check # disabled
    if self.platformCheck:
      platform = self.checkPlatformSupported( job, classAdJob )
      if platform['OK']:
        arch = platform['Value']
        message += 'Platform: ' + arch + ' OK, '
      else:
        res = 'No supported platform for job ' + str( job ) + '.'
        minorStatus = platform['Value']
        self.log.info( message )
        self.log.info( res )
        return S_ERROR( message )

    #Output data exists check
    if self.outputDataCheck: # disabled
      if jobType != 'user':
        outputData = self.checkOutputDataExists( job, classAdJob )
        if outputData['OK']:
          if outputData.has_key( 'SUCCESS' ):
            success = self.successStatus
            minorStatus = outputData['SUCCESS']
            report = outputData['Value']
            message += report
            self.log.info( message )
            self.setJobParam( job, 'JobSanityCheck', message )
            self.updateJobStatus( job, success, minorStatus )
            # FIXME: this can not be a S_OK(), Job has to be aborted if OutPut data is present
            return S_OK( 'Found successful job' )
          else:
            flag = outputData['Value']
            message += 'Output Data: ' + flag + ', '
        else:
          res = 'Job: ' + str( job ) + ' Failed since output data exists.'
          minorStatus = outputData['Value']
          self.log.info( message )
          self.log.info( res )
          return S_ERROR( message )

    #Input Sandbox uploaded check
    if self.inputSandboxCheck: # disabled
      inputSandbox = self.checkInputSandbox( job, classAdJob )
      if inputSandbox['OK']:
        sbChecked = inputSandbox['Value']
        message += ' Input Sandboxes: %s, OK.' % sbChecked
      else:
        res = 'Job: %s failed due some missing sandboxes' % job
        minorStatus = inputSandbox['Message']
        self.log.info( message )
        self.log.info( res )
        return S_ERROR( minorStatus )

    self.log.info( message )
    self.setJobParam( job, 'JobSanityCheck', message )
    return self.setNextOptimizer( job )

  #############################################################################
  def checkInputData( self, job, jobType, voName ):
    """This method checks both the amount of input
       datasets for the job and whether the LFN conventions
       are correct.
    """
    maxData = int( self.maxDataPerJob )
    totalData = 0
    slashFlag = 0
    incorrectDataFlag = 0

    result = self.jobDB.getInputData( job )

    if not result['OK']:
      self.log.warn( 'Failed to get input data from JobDB for %s' % ( job ) )
      self.log.warn( result['Message'] )
      result = S_ERROR()
      result['Value'] = 'Input Data Specification'
      return result

    if not result['Value']:
      return S_OK( 'No input LFNs' )

    data = result['Value'] # seems to be [''] when null, which isn't an empty list ;)
    ok = False
    for i in data:
      if i:
        ok = True
    if not ok:
      self.log.debug( 'Job %s has no input data requirement' % ( job ) )
      return S_OK( 'No input LFNs' )

    self.log.debug( 'Job %s has an input data requirement and will be checked' % ( job ) )
    data = result['Value']
    repData = '\n'
    for i in data:
      repData += i + '\n'
    self.log.debug( 'Data is: %s' % ( repData ) )

    totalData = len( data )

    if totalData:
      for i in data:
        j = i.replace( 'LFN:', '' )
        if not re.search( '^/' + voName + '/', j ):
          incorrectDataFlag += 1
        if re.search( '//', j ):
          slashFlag += 1

    if incorrectDataFlag:
      result = S_ERROR()
      result['Value'] = "Input data not correctly specified"
      return result

    if slashFlag:
      result = S_ERROR()
      result['Value'] = "Input data contains //"
      return result

    #only check limit for user jobs
    if jobType.lower() == 'user' and totalData > maxData:
      message = '%s datasets selected. Max limit is %s.' % ( totalData, maxData )
      self.setJobParam( job, 'DatasetCheck', message )
      result = S_ERROR()
      result['Value'] = "Exceeded Maximum Dataset Limit (%s)" % ( maxData )
      return result

    number = str( totalData )
    result = S_OK()
    result['Value'] = number + ' LFNs OK'
    return result

  #############################################################################
  def  checkOutputDataExists( self, job, classAdJob ):
    """If the job output data is already in the LFC, this
       method will fail the job for the attention of the
       data manager. To be tidied for DIRAC3...
    """
    # FIXME: To implement checkOutputDataExists
    return S_OK()

  #############################################################################
  def checkPlatformSupported( self, job, classAdJob ):
    """This method queries the CS for available platforms
       supported by DIRAC and will check these against what
       the job requests.
    """
    # FIXME: To implement checkPlatformSupported
    return S_OK()

  #############################################################################
  def checkInputSandbox( self, job, classAdJob ):
    """The number of input sandbox files, as specified in the job
       JDL are checked in the JobDB.
    """
    ownerName = classAdJob.getAttributeString( "Owner" )
    if not ownerName:
      ownerDN = classAdJob.getAttributeString( "OwnerDN" )
      ownerName = CS.getUsernameForDN( ownerDN )
    ownerGroup = classAdJob.getAttributeString( "OwnerGroup" )
    jobSetup = classAdJob.getAttributeString( "DIRACSetup" )
    isbList = classAdJob.getListFromExpression( 'InputSandbox' )
    sbsToAssign = []
    for isb in isbList:
      if isb.find( "SB:" ) == 0:
        self.log.info( "Found a sandbox", isb )
        sbsToAssign.append( ( isb, "Input" ) )
    numSBsToAssign = len( sbsToAssign )
    if not numSBsToAssign:
      return S_OK( 0 )
    self.log.info( "Assigning %s sandboxes on behalf of %s@%s" % ( numSBsToAssign, ownerName, ownerGroup ) )
    result = self.sandboxClient.assignSandboxesToJob( job, sbsToAssign, ownerName, ownerGroup, jobSetup )
    if not result[ 'OK' ]:
      self.log.error( "Could not assign sandboxes in the SandboxStore", "assigned to job %s" % job )
      return S_ERROR( "Cannot assign sandbox to job" )
    assigned = result[ 'Value' ]
    if assigned != numSBsToAssign:
      self.log.error( "Could not assign all sandboxes (%s). Only assigned %s" % ( numSBsToAssign, assigned ) )
    return S_OK( numSBsToAssign )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
