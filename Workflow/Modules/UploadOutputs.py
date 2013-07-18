# ##WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING #
#                                           Under development                                                   #
# ##WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING #

""" Module to upload specified job output files according to the parameters
    defined in the production workflow.
"""

from DIRAC                              import gLogger

from DIRAC.Workflow.Modules.ModuleBase  import ModuleBase, GracefulTermination

class UploadOutputs( ModuleBase ):

  #############################################################################

  def __init__( self ):
    """ c'tor
    """
    self.log = gLogger.getSubLogger( "UploadOutputs" )
    super( UploadOutputs, self ).__init__( self.log )

    self.outputDataStep = ''
    self.outputData = []
    self.outputList = []

  #############################################################################

  def _resolveInputVariables( self ):
    """ The module parameters are resolved here.
    """
    super( UploadOutputs, self )._resolveInputVariables()

    # this comes from Job().setOutputData(). Typical for user jobs
    if self.workflow_commons.has_key( 'OutputData' ):
      self.outputData = self.workflow_commons['OutputData']
      if not isinstance( self.outputData, list ):  # type( userOutputData ) == type( [] ):
        self.outputData = [ i.strip() for i in self.outputData.split( ';' ) ]
    # if not present, we use the outputList, which is instead incrementally created based on the single step outputs
    # This is more typical for production jobs, that can have many steps linked one after the other
    elif self.workflow_commons.has_key( 'outputList' ):
      self.outputList = self.workflow_commons['outputList']
    else:
      raise GracefulTermination, 'Nothing to upload'

    # in case you want to put a mask on the steps
    # TODO: add it to the DIRAC API
    if self.workflow_commons.has_key( 'outputDataStep' ):
      self.outputDataStep = self.workflow_commons['outputDataStep']

    # this comes from Job().setOutputData(). Typical for user jobs
    if self.workflow_commons.has_key( 'OutputSE' ):
      specifiedSE = self.workflow_commons['OutputSE']
      if not type( specifiedSE ) == type( [] ):
        self.utputSE = [i.strip() for i in specifiedSE.split( ';' )]
    else:
      self.log.verbose( 'No OutputSE specified, using default value: %s' % ( ', '.join( self.defaultOutputSE ) ) )
      self.outputSE = []

    # this comes from Job().setOutputData(). Typical for user jobs
    if self.workflow_commons.has_key( 'OutputPath' ):
      self.outputPath = self.workflow_commons['OutputPath']


  def _initialize( self ):
    """ gets the files to upload, check if to upload
    """
    # lfnsList = self.__getOutputLFNs( self.outputData ) or outputList?

    if not self._checkWFAndStepStatus():
      raise GracefulTermination, 'No output data upload attempted'

  def __getOuputLFNs( self, outputList, *args ):
    """ This is really VO-specific.
        It should be replaced by each VO. Setting an LFN here just as an idea, and for testing purposes.
    """
    lfnList = []
    for outputFile in outputList:
      lfnList.append( '/'.join( [str( x ) for x in args] ) + outputFile )

    return lfnList

  def _execute( self ):
    """ uploads the files
    """
    pass
