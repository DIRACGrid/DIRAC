"""  TransformationAgent processes transformations found in the transformation database. 
"""

__RCSID__ = "$Id$"

from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
from DIRAC.DataManagementSystem.Client.ReplicaManager           import ReplicaManager
import time, re, random

AGENT_NAME = 'Transformation/TransformationAgent'

class TransformationAgent( AgentModule ):

  def initialize( self ):
    """ standard init
    """
    self.pluginLocation = self.am_getOption( 'PluginLocation',
                                             'DIRAC.TransformationSystem.Agent.TransformationPlugin' )
    self.checkCatalog = self.am_getOption( 'CheckCatalog', 'yes' )
    self.transformationStatus = self.am_getOption( 'transformationStatus', ['Active', 'Completing', 'Flush'] )
    self.maxFiles = self.am_getOption( 'MaxFiles', 5000 )
    self.transformationTypes = self.am_getOption( 'TransformationTypes', [] )

    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    self.transDB = TransformationClient( 'TransformationDB' )
    self.rm = ReplicaManager()
    self.unusedFiles = {}
    return S_OK()

  def execute( self ):
    """ get and process the transformations to be processed
    """
    res = self.getTransformations()
    if not res['OK']:
      gLogger.info( "execute: Failed to obtain transformations: %s" % res['Message'] )
      return S_OK()
    # Process the transformations
    for transDict in res['Value']:
      transID = long( transDict['TransformationID'] )
      gLogger.info( "execute: Processing transformation %s." % transID )
      startTime = time.time()
      res = self.processTransformation( transDict )
      if not res['OK']:
        gLogger.info( "execute: Failed to process transformation: %s" % res['Message'] )
      else:
        gLogger.info( "execute: Processed transformation in %.1f seconds" % ( time.time() - startTime ) )
    return S_OK()

  def getTransformations( self ):
    """ Obtain the transformations to be executed 
    """
    transName = self.am_getOption( 'Transformation', 'All' )
    if transName == 'All':
      gLogger.info( "%s.getTransformations: Initializing general purpose agent." % AGENT_NAME )
      transfDict = {'Status':['Active', 'Completing', 'Flush'] }
      if self.transformationTypes:
        transfDict['Type'] = self.transformationTypes
      res = self.transDB.getTransformations( transfDict, extraParams = True )
      if not res['OK']:
        gLogger.error( "getTransformations: Failed to get transformations: %s" % res['Message'] )
        return res
      transformations = res['Value']
      gLogger.info( "getTransformations: Obtained %d transformations to process" % len( transformations ) )
    else:
      gLogger.info( "getTransformations: Initializing for transformation %s." % transName )
      res = self.transDB.getTransformation( transName, extraParams = True )
      if not res['OK']:
        gLogger.error( "getTransformations: Failed to get transformation: %s." % res['Message'] )
        return res
      transformations = [res['Value']]
    return S_OK( transformations )

  def processTransformation( self, transDict ):
    transID = transDict['TransformationID']
    # First get the LFNs associated to the transformation
    res = self.transDB.getTransformationFiles( condDict = {'TransformationID':transID, 'Status':'Unused'} )
    if not res['OK']:
      gLogger.error( "processTransformation: Failed to obtain input data: %s." % res['Message'] )
      return res
    transFiles = res['Value']
    lfns = [ f['LFN'] for f in transFiles ]

    if not lfns:
      gLogger.info( "processTransformation: No 'Unused' files found for transformation." )
      if transDict['Status'] == 'Flush':
        res = self.transDB.setTransformationParameter( transID, 'Status', 'Active' )
        if not res['OK']:
          gLogger.error( "processTransformation: Failed to update transformation status to 'Active': %s." % res['Message'] )
        else:
          gLogger.info( "processTransformation: Updated transformation status to 'Active'." )
      return S_OK()
    #Check if something new happened
    if len( lfns ) == self.unusedFiles.get( transID, 0 ) and transDict['Status'] != 'Flush':
      gLogger.info( "processTransformation: No new 'Unused' files found for transformation." )
      return S_OK()

    replicateOrRemove = transDict['Type'].lower() in ["replication", "removal"]
    # Limit the number of LFNs to be considered for replication or removal as they are treated individually
    if replicateOrRemove:
      if len( lfns ) <= self.maxFiles:
        firstFile = 0
      else:
        firstFile = int( random.uniform( 0, len( lfns ) - self.maxFiles ) )
      lfns = lfns[firstFile:firstFile + self.maxFiles - 1]
    unusedFiles = len( lfns )

    # Check the data is available with replicas
    res = self.__getDataReplicas( transID, lfns, active = not replicateOrRemove )
    if not res['OK']:
      gLogger.error( "processTransformation: Failed to get data replicas: %s" % res['Message'] )
      return res
    dataReplicas = res['Value']

    # Get the plug-in type and create the plug-in object
    plugin = 'Standard'
    if transDict.has_key( 'Plugin' ) and transDict['Plugin']:
      plugin = transDict['Plugin']
    gLogger.info( "processTransformation: Processing transformation with '%s' plug-in." % plugin )
    res = self.__generatePluginObject( plugin )
    if not res['OK']:
      return res
    oPlugin = res['Value']

    # Get the plug-in and set the required params
    oPlugin.setParameters( transDict )
    oPlugin.setInputData( dataReplicas )
    oPlugin.setTransformationFiles( transFiles )
    res = oPlugin.generateTasks()
    if not res['OK']:
      gLogger.error( "processTransformation: Failed to generate tasks for transformation: %s" % res['Message'] )
      return res
    tasks = res['Value']
    # Create the tasks
    allCreated = True
    created = 0
    for se, lfns in tasks:
      res = self.transDB.addTaskForTransformation( transID, lfns, se )
      if not res['OK']:
        gLogger.error( "processTransformation: Failed to add task generated by plug-in: %s." % res['Message'] )
        allCreated = False
      else:
        created += 1
        unusedFiles -= len( lfns )
    if created:
      gLogger.info( "processTransformation: Successfully created %d tasks for transformation." % created )
    self.unusedFiles[transID] = unusedFiles

    # If this production is to Flush
    if transDict['Status'] == 'Flush' and allCreated:
      res = self.transDB.setTransformationParameter( transID, 'Status', 'Active' )
      if not res['OK']:
        gLogger.error( "processTransformation: Failed to update transformation status to 'Active': %s." % res['Message'] )
      else:
        gLogger.info( "processTransformation: Updated transformation status to 'Active'." )
    return S_OK()

  ######################################################################
  #
  # Internal methods used by the agent
  #

  def __generatePluginObject( self, plugin ):
    """ This simply instantiates the TransformationPlugin class with the relevant plugin name
    """
    try:
      plugModule = __import__( self.pluginLocation, globals(), locals(), ['TransformationPlugin'] )
    except ImportError, e:
      gLogger.exception( "__generatePluginObject: Failed to import 'TransformationPlugin' %s: %s" % ( plugin, e ) )
      return S_ERROR()
    try:
      plugin_o = getattr( plugModule, 'TransformationPlugin' )( '%s' % plugin,
                                                                transClient = self.transDB,
                                                                replicaManager = self.rm )
      return S_OK( plugin_o )
    except AttributeError, e:
      gLogger.exception( "__generatePluginObject: Failed to create %s(): %s." % ( plugin, e ) )
      return S_ERROR()

  def __getDataReplicas( self, transID, lfns, active = True ):
    """ Get the replicas for the LFNs and check their statuses 
    """
    startTime = time.time()
    if active:
      res = self.rm.getActiveReplicas( lfns )
    else:
      res = self.rm.getReplicas( lfns )
    if not res['OK']:
      return res
    gLogger.info( "__getDataReplicas: Replica results for %d files obtained in %.2f seconds" % ( len( lfns ), time.time() - startTime ) )
    # Create a dictionary containing all the file replicas
    dataReplicas = {}
    for lfn, replicaDict in res['Value']['Successful'].items():
      ses = replicaDict.keys()
      for se in ses:
        if active and re.search( 'failover', se.lower() ):
          gLogger.warn( "__getDataReplicas: Ignoring failover replica for %s." % lfn )
        else:
          if not dataReplicas.has_key( lfn ):
            dataReplicas[lfn] = {}
          dataReplicas[lfn][se] = replicaDict[se]
    # Make sure that file missing from the catalog are marked in the transformation DB.
    missingLfns = []
    for lfn, reason in res['Value']['Failed'].items():
      if re.search( "No such file or directory", reason ):
        gLogger.warn( "__getDataReplicas: %s not found in the catalog." % lfn )
        missingLfns.append( lfn )
    if missingLfns:
      res = self.transDB.setFileStatusForTransformation( transID, 'MissingLFC', missingLfns )
      if not res['OK']:
        gLogger.warn( "__getDataReplicas: Failed to update status of missing files: %s." % res['Message'] )
    if not dataReplicas:
      return S_ERROR( "No replicas obtained" )
    return S_OK( dataReplicas )
