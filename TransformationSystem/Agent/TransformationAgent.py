"""  TransformationAgent processes transformations found in the transformation database.
"""

import time, Queue, os, datetime, pickle, gc
from DIRAC                                                          import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from DIRAC.Core.Utilities.ThreadPool                                import ThreadPool
from DIRAC.Core.Utilities.ThreadSafe                                import Synchronizer
from DIRAC.Core.Utilities.List                                      import breakListIntoChunks, randomize
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations
from DIRAC.TransformationSystem.Client.TransformationClient         import TransformationClient
from DIRAC.TransformationSystem.Agent.TransformationAgentsUtilities import TransformationAgentsUtilities
from DIRAC.DataManagementSystem.Client.DataManager                  import DataManager

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/TransformationAgent'
gSynchro = Synchronizer()

class TransformationAgent( AgentModule, TransformationAgentsUtilities ):
  """ Usually subclass of AgentModule
  """

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    AgentModule.__init__( self, *args, **kwargs )
    TransformationAgentsUtilities.__init__( self )

    # few parameters
    self.pluginLocation = ''
    self.transformationStatus = []
    self.maxFiles = 0
    self.transformationTypes = []

    # clients (out of the threads)
    self.transfClient = None

    # parameters for the threading
    self.transQueue = Queue.Queue()
    self.transInQueue = []

    # parameters for caching
    self.workDirectory = ''
    self.cacheFile = ''
    self.controlDirectory = ''
    self.dateWriteCache = datetime.datetime.utcnow()

    # Validity of the cache
    self.replicaCache = None
    self.replicaCacheValidity = None
    self.writingCache = False
    self.removedFromCache = 0

    self.noUnusedDelay = 0
    self.unusedFiles = {}
    self.unusedTimeStamp = {}

    self.debug = False
    self.transInThread = {}

  def initialize( self ):
    """ standard initialize
    """
    # few parameters
    self.pluginLocation = self.am_getOption( 'PluginLocation',
                                             'DIRAC.TransformationSystem.Agent.TransformationPlugin' )
    self.transformationStatus = self.am_getOption( 'transformationStatus', ['Active', 'Completing', 'Flush'] )
    self.maxFiles = self.am_getOption( 'MaxFiles', 5000 )

    agentTSTypes = self.am_getOption( 'TransformationTypes', [] )
    if agentTSTypes:
      self.transformationTypes = sorted( agentTSTypes )
    else:
      dataProc = Operations().getValue( 'Transformations/DataProcessing', ['MCSimulation', 'Merge'] )
      dataManip = Operations().getValue( 'Transformations/DataManipulation', ['Replication', 'Removal'] )
      self.transformationTypes = sorted( dataProc + dataManip )



    # clients
    self.transfClient = TransformationClient()

    # for caching using a pickle file
    self.workDirectory = self.am_getWorkDirectory()
    self.cacheFile = os.path.join( self.workDirectory, 'ReplicaCache.pkl' )
    self.__readCache()
    self.controlDirectory = self.am_getControlDirectory()
    self.replicaCacheValidity = self.am_getOption( 'ReplicaCacheValidity', 2 )
    self.noUnusedDelay = self.am_getOption( 'NoUnusedDelay', 6 )
    self.dateWriteCache = datetime.datetime.utcnow()

    # Get it threaded
    maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
    threadPool = ThreadPool( maxNumberOfThreads, maxNumberOfThreads )
    self.log.info( "Multithreaded with %d threads" % maxNumberOfThreads )

    for i in xrange( maxNumberOfThreads ):
      threadPool.generateJobAndQueueIt( self._execute, [i] )

    self.log.info( "Will treat the following transformation types: %s" % str( self.transformationTypes ) )

    return S_OK()

  def finalize( self ):
    """ graceful finalization
    """
    method = 'finalize'
    if self.transInQueue:
      self._logInfo( "Wait for threads to get empty before terminating the agent (%d tasks)" % len( self.transInQueue ), method = method )
      self._logInfo( 'Remaining transformations: ' + ','.join( [str( transID ) for transID in self.transInQueue] ), method = method )
      self.transInQueue = []
      while self.transInThread:
        time.sleep( 2 )
      self._logInfo( "Threads are empty, terminating the agent..." , method = method )
    self.__writeCache( force = True )
    return S_OK()

  def execute( self ):
    """ Just puts transformations in the queue
    """
    # Get the transformations to process
    res = self.getTransformations()
    if not res['OK']:
      self._logError( "Failed to obtain transformations: %s" % ( res['Message'] ) )
      return S_OK()
    # Process the transformations
    count = 0
    for transDict in res['Value']:
      transID = long( transDict['TransformationID'] )
      if transDict.get( 'InheritedFrom' ):
        # Try and move datasets from the ancestor production
        res = self.transfClient.moveFilesToDerivedTransformation( transDict )
        if not res['OK']:
          self._logError( "Error moving files from an inherited transformation", res['Message'], transID = transID )
        else:
          parentProd, movedFiles = res['Value']
          if movedFiles:
            self._logInfo( "Successfully moved files from %d to %d:" % ( parentProd, transID ), transID = transID )
            for status, val in movedFiles.items():
              self._logInfo( "\t%d files to status %s" % ( val, status ), transID = transID )
      if transID not in self.transInQueue:
        count += 1
        self.transInQueue.append( transID )
        self.transQueue.put( transDict )
    self._logInfo( "Out of %d transformations, %d put in thread queue" % ( len( res['Value'] ), count ) )
    return S_OK()

  def getTransformations( self ):
    """ Obtain the transformations to be executed - this is executed at the start of every loop (it's really the
        only real thing in the execute()
    """
    transName = self.am_getOption( 'Transformation', 'All' )
    if transName == 'All':
      self._logInfo( "Initializing general purpose agent.", method = 'getTransformations' )
      transfDict = {'Status': self.transformationStatus }
      if self.transformationTypes:
        transfDict['Type'] = self.transformationTypes
      res = self.transfClient.getTransformations( transfDict, extraParams = True )
      if not res['OK']:
        self._logError( "Failed to get transformations: %s" % res['Message'], method = 'getTransformations' )
        return res
      transformations = res['Value']
      self._logInfo( "Obtained %d transformations to process" % len( transformations ), method = 'getTransformations' )
    else:
      self._logInfo( "Initializing for transformation %s." % transName, method = "getTransformations" )
      res = self.transfClient.getTransformation( transName, extraParams = True )
      if not res['OK']:
        self._logError( "Failed to get transformation: %s." % res['Message'], method = 'getTransformations' )
        return res
      transformations = [res['Value']]
    return S_OK( transformations )

  def _getClients( self ):
    """ returns the clients used in the threads
    """
    threadTransformationClient = TransformationClient()
    threadDataManager = DataManager()

    return {'TransformationClient': threadTransformationClient,
            'DataManager': threadDataManager}

  def _execute( self, threadID ):
    """ thread - does the real job: processing the transformations to be processed
    """

    # Each thread will have its own clients
    clients = self._getClients()

    while True:
      transDict = self.transQueue.get()
      try:
        transID = long( transDict['TransformationID'] )
        if transID not in self.transInQueue:
          break
        self.transInThread[transID] = ' [Thread%d] [%s] ' % ( threadID, str( transID ) )
        self._logInfo( "Processing transformation %s." % transID, transID = transID )
        startTime = time.time()
        res = self.processTransformation( transDict, clients )
        if not res['OK']:
          self._logInfo( "Failed to process transformation: %s" % res['Message'], transID = transID )
      except Exception, x:
        self._logException( '%s' % x, transID = transID )
      finally:
        if not transID:
          transID = 'None'
        self.transInThread.pop( transID, None )
        if transID in self.transInQueue:
          self._logInfo( "Processed transformation in %.1f seconds" % ( time.time() - startTime ), transID = transID )
          self.transInQueue.remove( transID )
        self._logVerbose( "%d transformations still in queue" % len( self.transInQueue ) )
    return S_OK()

  def processTransformation( self, transDict, clients, active = True ):
    """ process a single transformation (in transDict)
    """

    transID = transDict['TransformationID']
    replicateOrRemove = transDict['Type'].lower() in ( 'replication', 'removal' )

    # First get the LFNs associated to the transformation
    transFiles = self._getTransformationFiles( transDict, clients, replicateOrRemove = replicateOrRemove )
    if not transFiles['OK']:
      return transFiles
    if not transFiles['Value']:
      return S_OK()

    transFiles = transFiles['Value']
    lfns = [ f['LFN'] for f in transFiles ]
    self.__cleanReplicas( transID, lfns )
    unusedFiles = len( lfns )

    # Limit the number of LFNs to be considered for replication or removal as they are treated individually
    if replicateOrRemove:
      totLfns = len( lfns )
      lfns = self.__applyReduction( lfns )
      if len( lfns ) != totLfns:
        self._logInfo( "Reduced number of files from %d to %d" % ( totLfns, len( lfns ) ),
                       method = "processTransformation", transID = transID )


    # Check the data is available with replicas
    res = self.__getDataReplicas( transDict, lfns, clients, active = not replicateOrRemove )
    if not res['OK']:
      self._logError( "Failed to get data replicas: %s" % res['Message'],
                       method = "processTransformation", transID = transID )
      return res
    dataReplicas = res['Value']

    # Get the plug-in type and create the plug-in object
    plugin = 'Standard'
    if transDict.get( 'Plugin' ):
      plugin = transDict['Plugin']
    self._logInfo( "Processing transformation with '%s' plug-in." % plugin,
                    method = "processTransformation", transID = transID )
    res = self.__generatePluginObject( plugin, clients )
    if not res['OK']:
      return res
    oPlugin = res['Value']

    # Get the plug-in and set the required params
    oPlugin.setParameters( transDict )
    oPlugin.setInputData( dataReplicas )
    oPlugin.setTransformationFiles( transFiles )
    res = oPlugin.generateTasks()
    if not res['OK']:
      self._logError( "Failed to generate tasks for transformation: %s" % res['Message'],
                       method = "processTransformation", transID = transID )
      return res
    tasks = res['Value']
    # Create the tasks
    allCreated = True
    created = 0
    lfnsInTasks = []
    for se, lfns in tasks:
      res = clients['TransformationClient'].addTaskForTransformation( transID, lfns, se )
      if not res['OK']:
        self._logError( "Failed to add task generated by plug-in: %s." % res['Message'],
                          method = "processTransformation", transID = transID )
        allCreated = False
      else:
        created += 1
        lfnsInTasks += lfns
    if created:
      self._logInfo( "Successfully created %d tasks for transformation." % created,
                      method = "processTransformation", transID = transID )
    else:
      self._logInfo( "No new tasks created for transformation.",
                     method = "processTransformation", transID = transID )
    self.unusedFiles[transID] = unusedFiles - len( lfnsInTasks )
    self.__removeFilesFromCache( transID, lfnsInTasks )

    # If this production is to Flush
    if transDict['Status'] == 'Flush' and allCreated:
      res = clients['TransformationClient'].setTransformationParameter( transID, 'Status', 'Active' )
      if not res['OK']:
        self._logError( "Failed to update transformation status to 'Active': %s." % res['Message'],
                         method = "processTransformation", transID = transID )
      else:
        self._logInfo( "Updated transformation status to 'Active'.",
                        method = "processTransformation", transID = transID )
    return S_OK()

  ######################################################################
  #
  # Internal methods used by the agent
  #

  def _getTransformationFiles( self, transDict, clients, statusList = None, replicateOrRemove = False ):
    """ get the data replicas for a certain transID
    """

    transID = transDict['TransformationID']
    method = '_getTransformationFiles'

    # Files that were problematic (either explicit or because SE was banned) may be recovered,
    # and always removing the missing ones
    if not statusList:
      statusList = ['Unused', 'ProbInFC']
    statusList += ['MissingInFC'] if transDict['Type'] == 'Removal' else []
    res = clients['TransformationClient'].getTransformationFiles( condDict = {'TransformationID':transID,
                                                                              'Status':statusList} )
    if not res['OK']:
      self._logError( "Failed to obtain input data: %s." % res['Message'],
                       method = method, transID = transID )
      return res
    transFiles = res['Value']

    if not transFiles:
      self._logInfo( "No '%s' files found for transformation." % ','.join( statusList ),
                      method = method, transID = transID )
      if transDict['Status'] == 'Flush':
        res = clients['TransformationClient'].setTransformationParameter( transID, 'Status', 'Active' )
        if not res['OK']:
          self._logError( "Failed to update transformation status to 'Active': %s." % res['Message'],
                           method = method, transID = transID )
        else:
          self._logInfo( "Updated transformation status to 'Active'.",
                          method = method, transID = transID )
      return S_OK()
    # Check if transformation is kicked
    kickFile = os.path.join( self.controlDirectory, 'KickTransformation_%s' % str( transID ) )
    try:
      kickTrans = os.path.exists( kickFile )
      if kickTrans:
        os.remove( kickFile )
    except:
      pass

    # Check if something new happened
    now = datetime.datetime.utcnow()
    if not kickTrans and not replicateOrRemove:
      nextStamp = self.unusedTimeStamp.setdefault( transID, now ) + datetime.timedelta( hours = self.noUnusedDelay )
      skip = now < nextStamp
      if len( transFiles ) == self.unusedFiles.get( transID, 0 ) and transDict['Status'] != 'Flush' and skip:
        self._logInfo( "No new '%s' files found for transformation." % ','.join( statusList ),
                        method = method, transID = transID )
        return S_OK()

    self.unusedTimeStamp[transID] = now
    # If files are not Unused, set them Unused
    notUnused = [trFile['LFN'] for trFile in transFiles if trFile['Status'] != 'Unused']
    otherStatuses = sorted( set( [trFile['Status'] for trFile in transFiles] ) - set( ['Unused'] ) )
    if notUnused:
      res = clients['TransformationClient'].setFileStatusForTransformation( transID, 'Unused', notUnused, force = True )
      if not res['OK']:
        self._logError( "Error setting %d files Unused" % len( notUnused ), res['Message'],
                        method = method, transID = transID )
      else:
        self._logInfo( "Set %d files from %s to Unused" % ( ','.join( otherStatuses ). len( notUnused ) ) )
        self.__removeFilesFromCache( transID, notUnused )
    return S_OK( transFiles )

  def __applyReduction( self, lfns ):
    """ eventually remove the number of files to be considered
    """
    if len( lfns ) <= self.maxFiles:
      return lfns
    return randomize( lfns )[:self.maxFiles]

  def __getDataReplicas( self, transDict, lfns, clients, active = True ):
    """ Get the replicas for the LFNs and check their statuses. It first looks within the cache.
    """
    method = '__getDataReplicas'
    transID = transDict['TransformationID']
    if 'RemoveFile' in transDict['Body']:
      # When removing files, we don't care about their replicas
      return S_OK( dict.fromkeys( lfns, ['None'] ) )
    clearCacheFile = os.path.join( self.workDirectory, 'ClearCache_%s' % str( transID ) )
    try:
      clearCache = os.path.exists( clearCacheFile )
      if clearCache:
        os.remove( clearCacheFile )
    except:
      pass
    if clearCache or transDict['Status'] == 'Flush':
      self._logInfo( "Replica cache cleared", method = method, transID = transID )
      # We may need to get new replicas
      self.__clearCacheForTrans( transID )
    else:
      # If the cache needs to be cleaned
      self.__cleanCache( transID )
    startTime = time.time()
    dataReplicas = {}
    nLfns = len( lfns )
    self._logVerbose( "Getting replicas for %d files" % nLfns, method = method, transID = transID )
    cachedReplicaSets = self.replicaCache.get( transID, {} )
    cachedReplicas = {}
    # Merge all sets of replicas
    for replicas in cachedReplicaSets.values():
      cachedReplicas.update( replicas )
    self._logInfo( "Number of cached replicas: %d" % len( cachedReplicas ), method = method, transID = transID )
    setCached = set( cachedReplicas )
    setLfns = set( lfns )
    for lfn in setLfns & setCached:
      dataReplicas[lfn] = cachedReplicas[lfn]
    newLFNs = setLfns - setCached
    self._logInfo( "ReplicaCache hit for %d out of %d LFNs" % ( len( dataReplicas ), nLfns ),
                   method = method, transID = transID )
    if newLFNs:
      startTime = time.time()
      self._logInfo( "Getting replicas for %d files from catalog" % len( newLFNs ),
                         method = method, transID = transID )
      newReplicas = {}
      for chunk in breakListIntoChunks( newLFNs, 1000 ):
        res = self._getDataReplicasDM( transID, chunk, clients, active = active )
        if res['OK']:
          for lfn, ses in res['Value'].items():
            if ses:
              newReplicas[lfn] = ses
        else:
          self._logWarn( "Failed to get replicas for %d files" % len( chunk ), res['Message'],
                          method = method, transID = transID )
      noReplicas = newLFNs - set( newReplicas )
      if noReplicas:
        self._logWarn( "Found %d files without replicas (or only in Failover)" % len( noReplicas ),
                       method = method, transID = transID )
      self.__updateCache( transID, newReplicas )
      dataReplicas.update( newReplicas )
      self._logInfo( "Obtained %d replicas from catalog in %.1f seconds" \
                      % ( len( newReplicas ), time.time() - startTime ),
                      method = method, transID = transID )
    return S_OK( dataReplicas )

  def _getDataReplicasDM( self, transID, lfns, clients, active = True, ignoreMissing = False ):
    """ Get the replicas for the LFNs and check their statuses, using the replica manager
    """
    method = '_getDataReplicasDM'

    startTime = time.time()
    self._logVerbose( "Getting replicas for %d files from catalog" % len( lfns ),
                      method = method, transID = transID )
    if active:
      res = clients['DataManager'].getActiveReplicas( lfns )
    else:
      res = clients['DataManager'].getReplicas( lfns )
    if not res['OK']:
      return res
    replicas = res['Value']
    # Prepare a dictionary for all LFNs
    dataReplicas = {}
    self._logVerbose( "Replica results for %d files obtained in %.2f seconds" % ( len( lfns ), time.time() - startTime ),
                    method = method, transID = transID )
    # If files are neither Successful nor Failed, they are set problematic in the FC
    problematicLfns = [lfn for lfn in lfns if lfn not in replicas['Successful'] and lfn not in replicas['Failed']]
    if problematicLfns:
      self._logInfo( "%d files found problematic in the catalog, set ProbInFC" % len( problematicLfns ) )
      res = clients['TransformationClient'].setFileStatusForTransformation( transID, 'ProbInFC', problematicLfns )
      if not res['OK']:
        self._logError( "Failed to update status of problematic files: %s." % res['Message'],
                        method = method, transID = transID )
    # Create a dictionary containing all the file replicas
    failoverLfns = []
    for lfn, replicaDict in replicas['Successful'].items():
      for se in replicaDict:
        #### This should definitely be included in the SE definition (i.e. not used for transformations)
        if active and 'failover' in se.lower():
          self._logVerbose( "Ignoring failover replica for %s." % lfn, method = method, transID = transID )
        else:
          dataReplicas.setdefault( lfn, [] ).append( se )
      if not dataReplicas.get( lfn ):
        failoverLfns.append( lfn )
    if failoverLfns:
      self._logVerbose( "%d files have no replica but possibly in Failover SE" % len( failoverLfns ) )
    # Make sure that file missing from the catalog are marked in the transformation DB.
    missingLfns = []
    for lfn, reason in replicas['Failed'].items():
      if "No such file or directory" in reason:
        self._logVerbose( "%s not found in the catalog." % lfn, method = method, transID = transID )
        missingLfns.append( lfn )
    if missingLfns:
      self._logInfo( "%d files not found in the catalog" % len( missingLfns ) )
      if ignoreMissing:
        dataReplicas.update( dict.fromkeys( missingLfns, [] ) )
      else:
        res = clients['TransformationClient'].setFileStatusForTransformation( transID, 'MissingInFC', missingLfns )
        if not res['OK']:
          self._logError( "Failed to update status of missing files: %s." % res['Message'],
                          method = method, transID = transID )
    return S_OK( dataReplicas )


  @gSynchro
  def __updateCache( self, transID, newReplicas ):
    """ Add replicas to the cache
    """
    self.replicaCache.setdefault( transID, {} )[datetime.datetime.utcnow()] = newReplicas

  @gSynchro
  def __clearCacheForTrans( self, transID ):
    """ Remove all replicas for a transformation
    """
    self.replicaCache.pop( transID , None )

  @gSynchro
  def __cleanReplicas( self, transID, lfns ):
    """ Remove cached replicas that are not in a list
    """
    cachedReplicas = set()
    for replicas in self.replicaCache.get( transID, {} ).values():
      cachedReplicas.update( replicas )
    toRemove = cachedReplicas - set( lfns )
    if toRemove:
      self._logInfo( "Remove %d files from cache" % len( toRemove ), method = '__cleanReplicas', transID = transID )
      self.__removeFromCache( transID, toRemove, log = False )


  @gSynchro
  def __cleanCache( self, fromTrans ):
    """ Cleans the cache
    """
    cacheChanged = False
    try:
      timeLimit = datetime.datetime.utcnow() - datetime.timedelta( days = self.replicaCacheValidity )
      for transID in set( self.replicaCache ):
        for updateTime in set( self.replicaCache[transID] ):
          nCache = len( self.replicaCache[transID][updateTime] )
          if updateTime < timeLimit or not nCache:
            self._logInfo( "Clear %s replicas for transformation %s, time %s" %
                           ( '%d cached' % nCache if nCache else 'empty cache' , str( transID ), str( updateTime ) ),
                           transID = fromTrans, method = '__cleanCache' )
            del self.replicaCache[transID][updateTime]
            cacheChanged = True
        # Remove empty transformations
        if not self.replicaCache[transID]:
          del self.replicaCache[transID]
    except Exception:
      self._logException( "Exception when cleaning replica cache:" )

    # Write the cache file
    try:
      if cacheChanged:
        gc.collect()
        self.__writeCache()
    except Exception:
      self._logException( "While writing replica cache" )

  @gSynchro
  def __removeFilesFromCache( self, transID, lfns ):
    self.__removeFromCache( transID, lfns, log = True )

  def __removeFromCache( self, transID, lfns, log = False ):
    if transID not in self.replicaCache:
      return
    removed = 0
    if self.replicaCache[transID] and lfns:
      for lfn in lfns:
        for timeKey in self.replicaCache[transID]:
          if self.replicaCache[transID][timeKey].pop( lfn, None ):
            removed += 1
    if removed:
      gc.collect()
      self.removedFromCache += removed
      if log:
        self._logInfo( "Removed %d replicas from cache" % removed, method = '__removeFromCache', transID = transID )
      if self.removedFromCache > 1000:
        self.__writeCache()

  def __readCache( self ):
    """ Reads from the cache
    """
    try:
      cacheFile = open( self.cacheFile, 'r' )
      self.replicaCache = pickle.load( cacheFile )
      cacheFile.close()
      self.removedFromCache = 0
      self._logInfo( "Successfully loaded replica cache from file %s" % self.cacheFile )
    except Exception:
      self._logException( "Failed to load replica cache from file %s" % self.cacheFile, method = '__readCache' )
      self.replicaCache = {}

  def __writeCache( self, force = False ):
    """ Writes the cache
    """
    method = '__writeCache'
    now = datetime.datetime.utcnow()
    if ( now - self.dateWriteCache ) < datetime.timedelta( minutes = 60 ) and not force:
      return
    while force and self.writingCache:
      # If writing is forced, wait until the previous write is over
      time.sleep( 10 )
    try:
      startTime = time.time()
      if self.writingCache:
        return
      self.writingCache = True
      self.dateWriteCache = now
      # Protect the copy of the cache
      tmpCache = self.replicaCache.copy()
      filesInCache = sum( [len( lfns ) for crs in tmpCache.values() for lfns in crs.values()] )
      # write to a temporary file in order to avoid corrupted files
      tmpFile = self.cacheFile + '.tmp'
      f = open( tmpFile, 'w' )
      pickle.dump( tmpCache, f )
      f.close()
      self.removedFromCache = 0
      # Now rename the file as it shold
      os.rename( tmpFile, self.cacheFile )
      self._logInfo( "Successfully wrote replica cache file (%d files) %s in %.1f seconds" \
                        % ( filesInCache, self.cacheFile, time.time() - startTime ), method = method )
    except Exception:
      self._logException( "Could not write replica cache file %s" % self.cacheFile, method = method )
    finally:
      self.writingCache = False

  def __generatePluginObject( self, plugin, clients ):
    """ This simply instantiates the TransformationPlugin class with the relevant plugin name
    """
    try:
      plugModule = __import__( self.pluginLocation, globals(), locals(), ['TransformationPlugin'] )
    except ImportError, e:
      self._logException( "Failed to import 'TransformationPlugin' %s: %s" % ( plugin, e ),
                           method = "__generatePluginObject" )
      return S_ERROR()
    try:
      plugin_o = getattr( plugModule, 'TransformationPlugin' )( '%s' % plugin,
                                                                transClient = clients['TransformationClient'],
                                                                dataManager = clients['DataManager'] )
      return S_OK( plugin_o )
    except AttributeError, e:
      self._logException( "Failed to create %s(): %s." % ( plugin, e ), method = "__generatePluginObject" )
      return S_ERROR()
    plugin_o.setDirectory( self.workDirectory )
    plugin_o.setCallback( self.pluginCallback )

  @gSynchro
  def pluginCallback( self, transID, invalidateCache = False ):
    """ Standard plugin callback
    """
    save = False
    if invalidateCache:
      try:
        if transID in self.replicaCache:
          self._logInfo( "Removed cached replicas for transformation" , method = 'pluginCallBack', transID = transID )
          self.replicaCache.pop( transID , None )
          save = True
      except:
        pass

      if save:
        self.__writeCache()

