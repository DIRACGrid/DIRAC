''' Create and send a combined request for any pending operations at
    the end of a job:
      fileReport (for the transformation)
      jobReport (for jobs)
      accounting
      request (for failover)
'''

from DIRAC                              import gLogger
from DIRAC.Workflow.Modules.ModuleBase  import ModuleBase, GracefulTermination

class FailoverRequest( ModuleBase ):

  #############################################################################

  def __init__( self, rm = None ):
    '''Module initialization.
    '''

    self.log = gLogger.getSubLogger( "FailoverRequest" )
    super( FailoverRequest, self ).__init__( self.log, rm = rm )

    self.stepInputData = []

  #############################################################################

  def _resolveInputVariables( self ):
    ''' By convention the module input parameters are resolved here.
    '''
    super( FailoverRequest, self )._resolveInputVariables()
    super( FailoverRequest, self )._resolveInputStep()

  def _initialize( self ):
    ''' checks if is to do, then prepare few things
    '''
    if not self._enableModule():
      raise GracefulTermination

    self.request.setRequestName( 'job_%s_request.xml' % self.jobID )
    self.request.setJobID( self.jobID )
    self.request.setSourceComponent( "Job_%s" % self.jobID )

  def _execute( self ):

    # report on the status of the input data
    if self.stepInputData:
      inputFiles = self.fileReport.getFiles()
      for lfn in self.stepInputData:
        if not lfn in inputFiles:
          self.log.verbose( 'No status populated for input data %s, setting to "Unused"' % lfn )
          self.fileReport.setFileStatus( int( self.production_id ), lfn, 'Unused' )

    if not self._checkWFAndStepStatus( noPrint = True ):
      inputFiles = self.fileReport.getFiles()
      for lfn in inputFiles:
        self.log.info( 'Forcing status to "Unused" due to workflow failure for: %s' % ( lfn ) )
        self.fileReport.setFileStatus( int( self.production_id ), lfn, 'Unused' )
    else:
      inputFiles = self.fileReport.getFiles()

      if inputFiles:
        self.log.info( 'Workflow status OK, setting input file status to Processed' )
      for lfn in inputFiles:
        self.log.info( 'Setting status to "Processed" for: %s' % ( lfn ) )
        self.fileReport.setFileStatus( int( self.production_id ), lfn, 'Processed' )

    result = self.fileReport.commit()
    if not result['OK']:
      self.log.error( 'Failed to report file status to TransformationDB, trying again before populating request with file report information' )
      result = self.fileReport.generateRequest()
      if not result['OK']:
        self.log.warn( 'Could not generate request for file report with result:\n%s' % ( result['Value'] ) )
      else:
        if result['Value'] is None:
          self.log.info( 'Files correctly reported to TransformationDB' )
        else:
          result = self.request.update( result['Value'] )
    else:
      self.log.info( 'Status of files have been properly updated in the TransformationDB' )

    # Must ensure that the local job report instance is used to report the final status
    # in case of failure and a subsequent failover operation
    if self.workflowStatus['OK'] and self.stepStatus['OK']:
      self.setApplicationStatus( 'Job Finished Successfully', jr = self.jobReport )

    # Retrieve the accumulated reporting request
    reportRequest = None
    result = self.jobReport.generateRequest()
    if not result['OK']:
      self.log.warn( 'Could not generate request for job report with result:\n%s' % ( result ) )
    else:
      reportRequest = result['Value']
    if reportRequest:
      self.log.info( 'Populating request with job report information' )
      self.request.update( reportRequest )

    accountingReport = None
    if self.workflow_commons.has_key( 'AccountingReport' ):
      accountingReport = self.workflow_commons['AccountingReport']
    if accountingReport:
      result = accountingReport.commit()
      if not result['OK']:
        self.log.error( '!!! Both accounting and RequestDB are down? !!!' )
        raise RuntimeError, result['Message']

    if self.request.isEmpty()['Value']:
      self.log.info( 'Request is empty, nothing to do.' )

    else:
      request_string = self.request.toXML()['Value']
      self.log.debug( request_string )
      # Write out the request string
      fname = '%s_%s_request.xml' % ( self.production_id, self.prod_job_id )
      xmlfile = open( fname, 'w' )
      xmlfile.write( request_string )
      xmlfile.close()
      self.log.info( 'Creating failover request for deferred operations for job %s:' % self.jobID )
      result = self.request.getDigest()
      if result['OK']:
        digest = result['Value']
        self.log.info( digest )

  def _finalize( self ):
    ''' Finalize and report correct status for the workflow based on the workflow
        or step status.
    '''
    self.log.verbose( 'Workflow status = %s, step status = %s' % ( self.workflowStatus['OK'], self.stepStatus['OK'] ) )
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.warn( 'Workflow status is not ok, will not overwrite status' )
      self.log.info( 'Workflow failed, end of FailoverRequest module execution.' )
      raise RuntimeError, 'Workflow failed, FailoverRequest module completed'

    self.log.info( 'Workflow successful, end of FailoverRequest module execution.' )

    raise GracefulTermination, 'FailoverRequest module completed'

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
