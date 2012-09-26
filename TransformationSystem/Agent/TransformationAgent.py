########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
"""  TransformationAgent processes transformations found in the transformation database. """

from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
from DIRAC.DataManagementSystem.Client.ReplicaManager           import ReplicaManager
import time, re, random

AGENT_NAME = 'Transformation/TransformationAgent'

class TransformationAgent( AgentModule ):

  def initialize( self ):
    self.pluginLocation = self.am_getOption( 'PluginLocation', 'DIRAC.TransformationSystem.Agent.TransformationPlugin' )
    self.checkCatalog = self.am_getOption( 'CheckCatalog', 'yes' )
    self.maxFiles = self.am_getOption( 'MaxFiles', 5000 )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/ProductionManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    self.transDB = TransformationClient( 'TransformationDB' )
    self.rm = ReplicaManager()
    self.unusedFiles = {}
    return S_OK()

  def execute( self ):
    # Get the transformations to process
    res = self.getTransformations()
    if not res['OK']:
      gLogger.info( "%s.execute: Failed to obtain transformations: %s" % ( AGENT_NAME, res['Message'] ) )
      return S_OK()
    # Process the transformations
    for transDict in res['Value']:
      transID = long( transDict['TransformationID'] )
      gLogger.info( "%s.execute: Processing transformation %s." % ( AGENT_NAME, transID ) )
      startTime = time.time()
      res = self.processTransformation( transDict )
      if not res['OK']:
        gLogger.info( "%s.execute: Failed to process transformation: %s" % ( AGENT_NAME, res['Message'] ) )
      else:
        gLogger.info( "%s.execute: Processed transformation in %.1f seconds" % ( AGENT_NAME, time.time() - startTime ) )
    return S_OK()

  def getTransformations( self ):
    # Obtain the transformations to be executed
    transName = self.am_getOption( 'Transformation', 'All' )
    if transName == 'All':
      gLogger.info( "%s.getTransformations: Initializing general purpose agent." % AGENT_NAME )
      res = self.transDB.getTransformations( {'Status':['Active', 'Completing', 'Flush']}, extraParams = True )
      if not res['OK']:
        gLogger.error( "%s.getTransformations: Failed to get transformations." % AGENT_NAME, res['Message'] )
        return res
      transformations = res['Value']
      gLogger.info( "%s.getTransformations: Obtained %d transformations to process" % ( AGENT_NAME, len( transformations ) ) )
    else:
      gLogger.info( "%s.getTransformations: Initializing for transformation %s." % ( AGENT_NAME, transName ) )
      res = self.transDB.getTransformation( transName, extraParams = True )
      if not res['OK']:
        gLogger.error( "%s.getTransformations: Failed to get transformation." % AGENT_NAME, res['Message'] )
        return res
      transformations = [res['Value']]
    return S_OK( transformations )

  def processTransformation( self, transDict ):
    transID = transDict['TransformationID']
    # First get the LFNs associated to the transformation
    res = self.transDB.getTransformationFiles( condDict = {'TransformationID':transID, 'Status':'Unused'} )
    if not res['OK']:
      gLogger.error( "%s.processTransformation: Failed to obtain input data." % AGENT_NAME, res['Message'] )
      return res
    transFiles = res['Value']
    lfns = [ f['LFN'] for f in transFiles ]

    if not lfns:
      gLogger.info( "%s.processTransformation: No 'Unused' files found for transformation." % AGENT_NAME )
      if transDict['Status'] == 'Flush':
        res = self.transDB.setTransformationParameter( transID, 'Status', 'Active' )
        if not res['OK']:
          gLogger.error( "%s.execute: Failed to update transformation status to 'Active'." % AGENT_NAME, res['Message'] )
        else:
          gLogger.info( "%s.execute: Updated transformation status to 'Active'." % AGENT_NAME )
      return S_OK()
    #Check if something new happened
    if len( lfns ) == self.unusedFiles.get( transID, 0 ) and transDict['Status'] != 'Flush':
      gLogger.info( "%s.processTransformation: No new 'Unused' files found for transformation." % AGENT_NAME )
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
      gLogger.error( "%s.processTransformation: Failed to get data replicas" % AGENT_NAME, res['Message'] )
      return res
    dataReplicas = res['Value']

    # Get the plug-in type and create the plug-in object
    plugin = 'Standard'
    if transDict.has_key( 'Plugin' ) and transDict['Plugin']:
      plugin = transDict['Plugin']
    gLogger.info( "%s.processTransformation: Processing transformation with '%s' plug-in." % ( AGENT_NAME, plugin ) )
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
      gLogger.error( "%s.processTransformation: Failed to generate tasks for transformation." % AGENT_NAME, res['Message'] )
      return res
    tasks = res['Value']
    # Create the tasks
    allCreated = True
    created = 0
    for se, lfns in tasks:
      res = self.transDB.addTaskForTransformation( transID, lfns, se )
      if not res['OK']:
        gLogger.error( "%s.processTransformation: Failed to add task generated by plug-in." % AGENT_NAME, res['Message'] )
        allCreated = False
      else:
        created += 1
        unusedFiles -= len( lfns )
    if created:
      gLogger.info( "%s.processTransformation: Successfully created %d tasks for transformation." % ( AGENT_NAME, created ) )
    self.unusedFiles[transID] = unusedFiles

    # If this production is to Flush
    if transDict['Status'] == 'Flush' and allCreated:
      res = self.transDB.setTransformationParameter( transID, 'Status', 'Active' )
      if not res['OK']:
        gLogger.error( "%s.execute: Failed to update transformation status to 'Active'." % AGENT_NAME, res['Message'] )
      else:
        gLogger.info( "%s.execute: Updated transformation status to 'Active'." % AGENT_NAME )
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
    except Exception, x:
      gLogger.exception( "%s.__generatePluginObject: Failed to import 'TransformationPlugin'" % AGENT_NAME, '', x )
      return S_ERROR()
    try:
      evalString = "plugModule.TransformationPlugin('%s')" % plugin
      return S_OK( eval( evalString ) )
    except Exception, x:
      gLogger.exception( "%s.__generatePluginObject: Failed to create %s()." % ( AGENT_NAME, plugin ), '', x )
      return S_ERROR()

  def __getDataReplicas( self, transID, lfns, active = True ):
    """ Get the replicas for the LFNs and check their statuses """
    startTime = time.time()
    if active:
      res = self.rm.getActiveReplicas( lfns )
    else:
      res = self.rm.getReplicas( lfns )
    if not res['OK']:
      return res
    gLogger.info( "%s.__getDataReplicas: Replica results for %d files obtained in %.2f seconds" % ( AGENT_NAME, len( lfns ), time.time() - startTime ) )
    # Create a dictionary containing all the file replicas
    dataReplicas = {}
    for lfn, replicaDict in res['Value']['Successful'].items():
      ses = replicaDict.keys()
      for se in ses:
        if active and re.search( 'failover', se.lower() ):
          gLogger.warn( "%s.__getDataReplicas: Ignoring failover replica for %s." % ( AGENT_NAME, lfn ) )
        else:
          if not dataReplicas.has_key( lfn ):
            dataReplicas[lfn] = {}
          dataReplicas[lfn][se] = replicaDict[se]
    # Make sure that file missing from the catalog are marked in the transformation DB.
    missingLfns = []
    for lfn, reason in res['Value']['Failed'].items():
      if re.search( "No such file or directory", reason ):
        gLogger.warn( "%s.__getDataReplicas: %s not found in the catalog." % ( AGENT_NAME, lfn ) )
        missingLfns.append( lfn )
    if missingLfns:
      res = self.transDB.setFileStatusForTransformation( transID, 'MissingLFC', missingLfns )
      if not res['OK']:
        gLogger.warn( "%s.__getDataReplicas: Failed to update status of missing files." % AGENT_NAME, res['Message'] )
    if not dataReplicas:
      return S_ERROR( "No replicas obtained" )
    return S_OK( dataReplicas )
