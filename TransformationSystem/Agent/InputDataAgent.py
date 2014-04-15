'''
Update the transformation files of active transformations given an InputDataQuery fetched from the Transformation Service.

Possibility to speedup the query time by only fetching files that were added since the last iteration.
Use the CS option RefreshOnly (False by default) and set the DateKey (empty by default) to the meta data
key set in the DIRAC FileCatalog.
'''

import time, datetime

from DIRAC                                                                import S_OK, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.TransformationSystem.Client.TransformationClient               import TransformationClient
from DIRAC.Resources.Catalog.FileCatalogClient                            import FileCatalogClient
from DIRAC.Core.Utilities.List                                            import sortList
from DIRAC.ConfigurationSystem.Client.Helpers.Operations                  import Operations

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/InputDataAgent'

class InputDataAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):
    ''' c'tor
    '''
    AgentModule.__init__( self, *args, **kwargs )

    self.fileLog = {}
    self.timeLog = {}
    self.fullTimeLog = {}

    self.pollingTime = self.am_getOption( 'PollingTime', 120 )
    self.fullUpdatePeriod = self.am_getOption( 'FullUpdatePeriod', 86400 )
    self.refreshonly = self.am_getOption( 'RefreshOnly', False )
    self.dateKey = self.am_getOption( 'DateKey', None )

    self.transClient = TransformationClient()
    self.metadataClient = FileCatalogClient()
    self.transformationTypes = None

  #############################################################################
  def initialize( self ):
    ''' Make the necessary initializations
    '''
    gMonitor.registerActivity( "Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM )
    agentTSTypes = self.am_getOption( 'TransformationTypes', [] )
    if agentTSTypes:
      self.transformationTypes = sortList( agentTSTypes )
    else:
      dataProc = Operations().getValue( 'Transformations/DataProcessing', ['MCSimulation', 'Merge'] )
      dataManip = Operations().getValue( 'Transformations/DataManipulation', ['Replication', 'Removal'] )
      self.transformationTypes = sortList( dataProc + dataManip )
    extendables = Operations().getValue( 'Transformations/ExtendableTransfTypes', [])
    if extendables:
      for extendable in extendables:
        if extendable in self.transformationTypes:
          self.transformationTypes.remove(extendable)
          #This is because the Extendables do not use this Agent (have no Input data query)
          
    return S_OK()

  ##############################################################################
  def execute( self ):
    ''' Main execution method
    '''

    gMonitor.addMark( 'Iteration', 1 )
    # Get all the transformations
    result = self.transClient.getTransformations( {'Status' : 'Active', 
                                                   'Type' : self.transformationTypes } )
    if not result['OK']:
      gLogger.error( "InputDataAgent.execute: Failed to get transformations.", result['Message'] )
      return S_OK()

    # Process each transformation
    for transDict in result['Value']:
      transID = long( transDict['TransformationID'] )
      res = self.transClient.getTransformationInputDataQuery( transID )
      if not res['OK']:
        if res['Message'] == 'No InputDataQuery found for transformation':
          gLogger.info( "InputDataAgent.execute: No input data query found for transformation %d" % transID )
        else:
          gLogger.error( "InputDataAgent.execute: Failed to get input data query for %d" % transID, res['Message'] )
        continue
      inputDataQuery = res['Value']

      if self.refreshonly:
        # Determine the correct time stamp to use for this transformation
        if self.timeLog.has_key( transID ):
          if self.fullTimeLog.has_key( transID ):
            # If it is more than a day since the last reduced query, make a full query just in case
            if ( datetime.datetime.utcnow() - self.fullTimeLog[transID] ) < datetime.timedelta( seconds = self.fullUpdatePeriod ):
              timeStamp = self.timeLog[transID]
              if self.dateKey:
                inputDataQuery[self.dateKey] = ( timeStamp - datetime.timedelta( seconds = 10 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
              else:
                gLogger.error( "DateKey was not set in the CS, cannot use the RefreshOnly" )
            else:
              self.fullTimeLog[transID] = datetime.datetime.utcnow()
        self.timeLog[transID] = datetime.datetime.utcnow()
        if not self.fullTimeLog.has_key( transID ):
          self.fullTimeLog[transID] = datetime.datetime.utcnow()

      # Perform the query to the metadata catalog
      gLogger.verbose( "Using input data query for transformation %d: %s" % ( transID, str( inputDataQuery ) ) )
      start = time.time()
      result = self.metadataClient.findFilesByMetadata( inputDataQuery )
      rtime = time.time() - start
      gLogger.verbose( "Metadata catalog query time: %.2f seconds." % ( rtime ) )
      if not result['OK']:
        gLogger.error( "InputDataAgent.execute: Failed to get response from the metadata catalog", result['Message'] )
        continue
      lfnList = result['Value']

      # Check if the number of files has changed since the last cycle
      nlfns = len( lfnList )
      gLogger.info( "%d files returned for transformation %d from the metadata catalog" % ( nlfns, int( transID ) ) )
      if self.fileLog.has_key( transID ):
        if nlfns == self.fileLog[transID]:
          gLogger.verbose( 'No new files in metadata catalog since last check' )
      self.fileLog[transID] = nlfns

      # Add any new files to the transformation
      addedLfns = []
      if lfnList:
        gLogger.verbose( 'Processing %d lfns for transformation %d' % ( len( lfnList ), transID ) )
        # Add the files to the transformation
        gLogger.verbose( 'Adding %d lfns for transformation %d' % ( len( lfnList ), transID ) )
        result = self.transClient.addFilesToTransformation( transID, sortList( lfnList ) )
        if not result['OK']:
          gLogger.warn( "InputDataAgent.execute: failed to add lfns to transformation", result['Message'] )
          self.fileLog[transID] = 0
        else:
          if result['Value']['Failed']:
            for lfn, error in res['Value']['Failed'].items():
              gLogger.warn( "InputDataAgent.execute: Failed to add %s to transformation" % lfn, error )
          if result['Value']['Successful']:
            for lfn, status in result['Value']['Successful'].items():
              if status == 'Added':
                addedLfns.append( lfn )
            gLogger.info( "InputDataAgent.execute: Added %d files to transformation" % len( addedLfns ) )

    return S_OK()
