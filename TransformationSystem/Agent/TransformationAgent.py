"""  TransformationAgent processes transformations found in the transformation database.
"""

import time, re, random, Queue, os, datetime, pickle
from DIRAC                                                          import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from DIRAC.Core.Utilities.ThreadPool                                import ThreadPool
from DIRAC.Core.Utilities.ThreadSafe                                import Synchronizer
from DIRAC.Core.Utilities.List                                      import sortList, breakListIntoChunks
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
    self.pluginLocation = self.am_getOption( 'PluginLocation',
                                             'DIRAC.TransformationSystem.Agent.TransformationPlugin' )
    self.transformationStatus = self.am_getOption( 'transformationStatus', ['Active', 'Completing', 'Flush'] )
    self.maxFiles = self.am_getOption( 'MaxFiles', 5000 )

    agentTSTypes = self.am_getOption( 'TransformationTypes', [] )
    if agentTSTypes:
      self.transformationTypes = sortList( agentTSTypes )
    else:
      dataProc = Operations().getValue( 'Transformations/DataProcessing', ['MCSimulation', 'Merge'] )
      dataManip = Operations().getValue( 'Transformations/DataManipulation', ['Replication', 'Removal'] )
      self.transformationTypes = sortList( dataProc + dataManip )

    # clients
    self.transfClient = TransformationClient()

    # for the threading
    self.transQueue = Queue.Queue()
    self.transInQueue = []

    # for caching using a pickle file
    self.workDirectory = self.am_getWorkDirectory()
    self.cacheFile = os.path.join( self.workDirectory, 'ReplicaCache.pkl' )
    self.controlDirectory = self.am_getControlDirectory()
    self.dateWriteCache = datetime.datetime.utcnow()

    # Validity of the cache
    self.replicaCache = None
    self.replicaCacheValidity = self.am_getOption( 'ReplicaCacheValidity', 2 )
    self.writingCache = False

    self.noUnusedDelay = self.am_getOption( 'NoUnusedDelay', 6 )
    self.unusedFiles = {}
    self.unusedTimeStamp = {}

  def initialize( self ):
    """ standard initialize
    """

    self.__readCache()
    self.dateWriteCache = datetime.datetime.utcnow()

    self.am_setOption( 'shifterProxy', 'ProductionManager' )

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
    if self.transInQueue:
      self._logInfo( "Wait for threads to get empty before terminating the agent (%d tasks)" % len( self.transInThread ) )
      self.transInQueue = []
      while self.transInThread:
        time.sleep( 2 )
      self.log.info( "Threads are empty, terminating the agent..." )
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
        self._logInfo( "Processed transformation in %.1f seconds" % ( time.time() - startTime ), transID = transID )
        self._logVerbose( "%d transformations still in queue" % ( len( self.transInQueue ) - 1 ) )
        self.transInThread.pop( transID, None )
        if transID in self.transInQueue:
          self.transInQueue.remove( transID )
    return S_OK()

  def processTransformation( self, transDict, clients, active = True ):
    """ process a single transformation (in transDict)
    """

    transID = transDict['TransformationID']
    replicateOrRemove = transDict['Type'].lower() in ['replication', 'removal']

    # First get the LFNs associated to the transformation
    transFiles = self._getTransformationFiles( transDict, clients )
    if not transFiles['OK']:
      return transFiles
    if not transFiles['Value']:
      return S_OK()

    transFiles = transFiles['Value']
    lfns = [ f['LFN'] for f in transFiles ]

    # Limit the number of LFNs to be considered for replication or removal as they are treated individually
    if replicateOrRemove:
      lfns = self.__applyReduction( lfns )

    unusedFiles = len( lfns )

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
    for se, lfns in tasks:
      res = clients['TransformationClient'].addTaskForTransformation( transID, lfns, se )
      if not res['OK']:
        self._logError( "Failed to add task generated by plug-in: %s." % res['Message'],
                          method = "processTransformation", transID = transID )
        allCreated = False
      else:
        created += 1
        unusedFiles -= len( lfns )
    if created:
      self._logInfo( "Successfully created %d tasks for transformation." % created,
                      method = "processTransformation", transID = transID )
    self.unusedFiles[transID] = unusedFiles

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

  def _getTransformationFiles( self, transDict, clients, statusList = ['Unused', 'ProbInFC'] ):
    """ get the data replicas for a certain transID
    """

    transID = transDict['TransformationID']

    # Files that were problematic (either explicit or because SE was banned) may be recovered,
    # and always removing the missing ones
    statusList = statusList + ['MissingInFC'] if transDict['Type'] == 'Removal' else statusList
    res = clients['TransformationClient'].getTransformationFiles( condDict = {'TransformationID':transID,
                                                                              'Status':statusList} )
    if not res['OK']:
      self._logError( "Failed to obtain input data: %s." % res['Message'],
                       method = "_getTransformationFiles", transID = transID )
      return res
    transFiles = res['Value']

    if not transFiles:
      self._logInfo( "No 'Unused' files found for transformation.",
                      method = "_getTransformationFiles", transID = transID )
      if transDict['Status'] == 'Flush':
        res = clients['TransformationClient'].setTransformationParameter( transID, 'Status', 'Active' )
        if not res['OK']:
          self._logError( "Failed to update transformation status to 'Active': %s." % res['Message'],
                           method = "_getTransformationFiles", transID = transID )
        else:
          self._logInfo( "Updated transformation status to 'Active'.",
                          method = "_getTransformationFiles", transID = transID )
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
    if not kickTrans:
      nextStamp = self.unusedTimeStamp.setdefault( transID, now ) + datetime.timedelta( hours = self.noUnusedDelay )
      skip = now < nextStamp
      if len( transFiles ) == self.unusedFiles.get( transID, 0 ) and transDict['Status'] != 'Flush' and skip:
        self._logInfo( "No new 'Unused' files found for transformation.",
                        method = "_getTransformationFiles", transID = transID )
        return S_OK()

    self.unusedTimeStamp[transID] = now
    return S_OK( transFiles )

  def __applyReduction( self, lfns ):
    """ eventually remove the number of files to be considered
    """
    if len( lfns ) <= self.maxFiles:
      firstFile = 0
    else:
      firstFile = int( random.uniform( 0, len( lfns ) - self.maxFiles ) )
    lfns = lfns[firstFile:firstFile + self.maxFiles - 1]

    return lfns

  def __getDataReplicas( self, transDict, lfns, clients, active = True ):
    """ Get the replicas for the LFNs and check their statuses. It first looks within the cache.
    """
    method = '__getDataReplicas'
    transID = transDict['TransformationID']
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
      self.__cleanCache()
    startTime = time.time()
    dataReplicas = {}
    lfns.sort()
    nLfns = len( lfns )
    self._logVerbose( "Getting replicas for %d files" % nLfns, method = method, transID = transID )
    newLFNs = []
    try:
      cachedReplicaSets = self.replicaCache.get( transID, {} )
      cachedReplicas = {}
      # Merge all sets of replicas
      for crs in cachedReplicaSets:
        cachedReplicas.update( cachedReplicaSets[crs] )
      self._logVerbose( "Number of cached replicas: %d" % len( cachedReplicas ), method = method, transID = transID )
      # Sorted browsing
      for cacheLfn in sorted( cachedReplicas ):
        while lfns and lfns[0] < cacheLfn:
          # All files until cacheLfn are new
          newLFNs.append( lfns.pop( 0 ) )
        if lfns:
          if lfns[0] == cacheLfn:
            # We found a match, copy and go to next cache
            lfn = lfns.pop( 0 )
            dataReplicas[lfn] = sorted( cachedReplicas[lfn] )
            continue
        if not lfns or lfns[0] > cacheLfn:
        # Remove files from the cache that are not in the required list
          for crs in cachedReplicaSets:
            cachedReplicaSets[crs].pop( cacheLfn, None )
      # Add what is left as new files
      newLFNs += lfns
    except Exception:
      self._logException( "Exception when browsing cache", method = method, transID = transID )
    self._logVerbose( "ReplicaCache hit for %d out of %d LFNs" % ( len( dataReplicas ), nLfns ),
                       method = method, transID = transID )
    if newLFNs:
      startTime = time.time()
      self._logVerbose( "Getting replicas for %d files from catalog" % len( newLFNs ),
                         method = method, transID = transID )
      newReplicas = {}
      noReplicas = []
      for chunk in breakListIntoChunks( newLFNs, 1000 ):
        res = self._getDataReplicasRM( transID, chunk, clients, active = active )
        if res['OK']:
          for lfn, ses in res['Value'].items():
            if ses:
              # Keep only the list of SEs as SURLs are useless
              newReplicas[lfn] = sorted( ses )
            else:
              noReplicas.append( lfn )
        else:
          self._logWarn( "Failed to get replicas for %d files" % len( chunk ), res['Message'],
                          method = method, transID = transID )
      if noReplicas:
        self._logWarn( "Found %d files without replicas" % len( noReplicas ),
                         method = method, transID = transID )
      self.__updateCache( transID, newReplicas )
      dataReplicas.update( newReplicas )
      self._logInfo( "Obtained %d replicas from catalog in %.1f seconds" \
                      % ( len( newReplicas ), time.time() - startTime ),
                      method = method, transID = transID )
    return S_OK( dataReplicas )

  def _getDataReplicasRM( self, transID, lfns, clients, active = True ):
    """ Get the replicas for the LFNs and check their statuses, using the replica manager
    """
    method = '_getDataReplicasRM'

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
    for lfn in lfns:
      dataReplicas[lfn] = []
    self._logInfo( "Replica results for %d files obtained in %.2f seconds" % ( len( lfns ), time.time() - startTime ),
                    method = method, transID = transID )
    # If files are neither Successful nor Failed, they are set problematic in the FC
    problematicLfns = [lfn for lfn in lfns if lfn not in replicas['Successful'] and lfn not in replicas['Failed']]
    if problematicLfns:
      self._logInfo( "%d files found problematic in the catalog" % len( problematicLfns ) )
      res = clients['TransformationClient'].setFileStatusForTransformation( transID, 'ProbInFC', problematicLfns )
      if not res['OK']:
        self._logError( "Failed to update status of problematic files: %s." % res['Message'],
                        method = method, transID = transID )
    # Create a dictionary containing all the file replicas
    failoverLfns = []
    for lfn, replicaDict in replicas['Successful'].items():
      for se in replicaDict:
        #### This should definitely be included in the SE definition (i.e. not used for transformations)
        if active and re.search( 'failover', se.lower() ):
          self._logVerbose( "Ignoring failover replica for %s." % lfn, method = method, transID = transID )
        else:
          dataReplicas[lfn].append( se )
      if not dataReplicas[lfn]:
        failoverLfns.append( lfn )
    if failoverLfns:
      self._logInfo( "%d files only found in Failover SE" % len( failoverLfns ) )
    # Make sure that file missing from the catalog are marked in the transformation DB.
    missingLfns = []
    for lfn, reason in replicas['Failed'].items():
      if re.search( "No such file or directory", reason ):
        self._logVerbose( "%s not found in the catalog." % lfn, method = method, transID = transID )
        missingLfns.append( lfn )
    if missingLfns:
      self._logInfo( "%d files not found in the catalog" % len( missingLfns ) )
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
  def __cleanCache( self ):
    """ Cleans the cache
    """
    cacheChanged = False
    try:
      timeLimit = datetime.datetime.utcnow() - datetime.timedelta( days = self.replicaCacheValidity )
      for transID in sorted( self.replicaCache ):
        for updateTime in self.replicaCache[transID].keys():
          if updateTime < timeLimit or not self.replicaCache[transID][updateTime]:
            self._logVerbose( "Clear %d cached replicas for transformation %s" % ( len( self.replicaCache[transID][updateTime] ),
                                                                                    str( transID ) ), method = '__cleanCache' )
            self.replicaCache[transID].pop( updateTime )
            cacheChanged = True
        # Remove empty transformations
        if not self.replicaCache[transID]:
          self.replicaCache.pop( transID )
    except Exception:
      self._logException( "Exception when cleaning replica cache:" )

    # Write the cache file
    try:
      if cacheChanged:
        self.__writeCache()
    except Exception:
      self._logException( "While writing replica cache" )

  def __readCache( self ):
    """ Reads from the cache
    """
    try:
      cacheFile = open( self.cacheFile, 'r' )
      self.replicaCache = pickle.load( cacheFile )
      cacheFile.close()
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
      self.dateWriteCache = now
      if self.writingCache:
        return
      self.writingCache = True
      # Protect the copy of the cache
      tmpCache = self.replicaCache.copy()
      # write to a temporary file in order to avoid corrupted files
      tmpFile = self.cacheFile + '.tmp'
      f = open( tmpFile, 'w' )
      pickle.dump( tmpCache, f )
      f.close()
      # Now rename the file as it shold
      os.rename( tmpFile, self.cacheFile )
      self._logVerbose( "Successfully wrote replica cache file %s in %.1f seconds" \
                        % ( self.cacheFile, time.time() - startTime ), method = method )
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
          self.replicaCache.pop( transID )
          save = True
      except:
        pass

      if save:
        self.__writeCache()

