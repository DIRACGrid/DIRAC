########################################################################
# $HeadURL$
########################################################################

"""  Replication Scheduler assigns replication requests to channels
"""
from DIRAC                                                  import gLogger, gConfig, S_OK, S_ERROR, rootPath
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.Core.Utilities.List                              import sortList
from DIRAC.ConfigurationSystem.Client                       import PathFinder
from DIRAC.ConfigurationSystem.Client.PathFinder            import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB               import TransferDB
from DIRAC.DataManagementSystem.Client.ReplicaManager       import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient    import DataLoggingClient
from DIRAC.RequestManagementSystem.DB.RequestDBMySQL        import RequestDBMySQL
from DIRAC.RequestManagementSystem.Client.RequestContainer  import RequestContainer
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.Resources.Storage.StorageFactory                 import StorageFactory
from DIRAC.Core.Utilities.SiteSEMapping                     import getSitesForSE
from DIRAC.Core.Utilities.SiteSEMapping                     import getSEsForSite
import types, re, random

__RCSID__ = "$Id$"

AGENT_NAME = 'DataManagement/ReplicationScheduler'

class ReplicationScheduler( AgentModule ):

  def initialize( self ):

    self.section = PathFinder.getAgentSection( AGENT_NAME )
    self.RequestDB = RequestDBMySQL()
    self.TransferDB = TransferDB()
    self.DataLog = DataLoggingClient()
    self.factory = StorageFactory()
    self.rm = ReplicaManager()

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  def execute( self ):
    """ The main agent execution method """

    # This allows dynamic changing of the throughput timescale
    self.throughputTimescale = self.am_getOption( 'ThroughputTimescale', 3600 )
    self.throughputTimescale = 60 * 60 * 1
    #print 'ThroughputTimescale:',self.throughputTimescale
    ######################################################################################
    #
    #  Obtain information on the current state of the channel queues
    #

    res = self.TransferDB.getChannelQueues()
    if not res['OK']:
      errStr = "ReplicationScheduler._execute: Failed to get channel queues from TransferDB."
      gLogger.error( errStr, res['Message'] )
      return S_OK()
    if not res['Value']:
      gLogger.info( "ReplicationScheduler._execute: No active channels found for replication." )
      return S_OK()
    channels = res['Value']

    res = self.TransferDB.getChannelObservedThroughput( self.throughputTimescale )
    if not res['OK']:
      errStr = "ReplicationScheduler._execute: Failed to get observed throughput from TransferDB."
      gLogger.error( errStr, res['Message'] )
      return S_OK()
    if not res['Value']:
      gLogger.info( "ReplicationScheduler._execute: No active channels found for replication." )
      return S_OK()
    bandwidths = res['Value']

    self.strategyHandler = StrategyHandler( bandwidths, channels, self.section )

    processedRequests = []
    requestsPresent = True
    while requestsPresent:

      ######################################################################################
      #
      #  The first step is to obtain a transfer request from the RequestDB which should be scheduled.
      #

      gLogger.info( "ReplicationScheduler._execute: Contacting RequestDB for suitable requests." )
      res = self.RequestDB.getRequest( 'transfer' )
      if not res['OK']:
        gLogger.error( "ReplicationScheduler._execute: Failed to get a request list from RequestDB.", res['Message'] )
        continue
      if not res['Value']:
        gLogger.info( "ReplicationScheduler._execute: No requests found in RequestDB." )
        requestsPresent = False
        return S_OK()
      requestString = res['Value']['RequestString']
      requestName = res['Value']['RequestName']
      gLogger.info( "ReplicationScheduler._execute: Obtained Request %s from RequestDB." % ( requestName ) )

      ######################################################################################
      #
      #  The request must then be parsed to obtain the sub-requests, their attributes and files.
      #

      logStr = 'ReplicationScheduler._execute: Parsing Request %s.' % ( requestName )
      gLogger.info( logStr )
      oRequest = RequestContainer( requestString )
      res = oRequest.getAttribute( 'RequestID' )
      if not res['OK']:
        gLogger.error( 'ReplicationScheduler._execute: Failed to get requestID.', res['Message'] )
        return S_ERROR( 'ReplicationScheduler._execute: Failed to get number of sub-requests.' )
      requestID = res['Value']
      if requestID in processedRequests:
        # Break the loop once we have iterated once over all requests
        res = self.RequestDB.updateRequest( requestName, requestString )
        if not res['OK']:
          gLogger.error( "Failed to update request", "%s %s" % ( requestName, res['Message'] ) )
        return S_OK()

      processedRequests.append( requestID )

      res = oRequest.getNumSubRequests( 'transfer' )
      if not res['OK']:
        gLogger.error( 'ReplicationScheduler._execute: Failed to get number of sub-requests.', res['Message'] )
        return S_ERROR( 'ReplicationScheduler._execute: Failed to get number of sub-requests.' )
      numberRequests = res['Value']
      gLogger.info( "ReplicationScheduler._execute: '%s' found with %s sub-requests." % ( requestName, numberRequests ) )

      ######################################################################################
      #
      #  The important request attributes are the source and target SEs.
      #

      for ind in range( numberRequests ):
        gLogger.info( "ReplicationScheduler._execute: Treating sub-request %s from '%s'." % ( ind, requestName ) )
        attributes = oRequest.getSubRequestAttributes( ind, 'transfer' )['Value']
        if attributes['Status'] != 'Waiting':
          #  If the sub-request is already in terminal state
          gLogger.info( "ReplicationScheduler._execute: Sub-request %s is status '%s' and  not to be executed." % ( ind, attributes['Status'] ) )
          continue

        sourceSE = attributes['SourceSE']
        targetSE = attributes['TargetSE']
        """ This section should go in the transfer request class """
        if type( targetSE ) in types.StringTypes:
          if re.search( ',', targetSE ):
            targetSEs = targetSE.split( ',' )
          else:
            targetSEs = [targetSE]
        """----------------------------------------------------- """
        operation = attributes['Operation']
        reqRepStrategy = None
        if operation in self.strategyHandler.getSupportedStrategies():
          reqRepStrategy = operation

        ######################################################################################
        #
        # Then obtain the file attribute of interest are the  LFN and FileID
        #

        res = oRequest.getSubRequestFiles( ind, 'transfer' )
        if not res['OK']:
          gLogger.error( 'ReplicationScheduler._execute: Failed to obtain sub-request files.' , res['Message'] )
          continue
        files = res['Value']
        gLogger.info( "ReplicationScheduler._execute: Sub-request %s found with %s files." % ( ind, len( files ) ) )
        filesDict = {}
        for file in files:
          lfn = file['LFN']
          if file['Status'] != 'Waiting':
            gLogger.debug( "ReplicationScheduler._execute: %s will not be scheduled because it is %s." % ( lfn, file['Status'] ) )
          else:
            fileID = file['FileID']
            filesDict[lfn] = fileID
        if not filesDict:
          gLogger.info( "ReplicationScheduler._execute: No Waiting files found for request" )
          continue
        notSched = len( files ) - len( filesDict )
        if notSched:
          gLogger.info( "ReplicationScheduler._execute: %d files found not Waiting" % notSched )

        ######################################################################################
        #
        #  Now obtain replica information for the files associated to the sub-request.
        #

        lfns = filesDict.keys()
        gLogger.info( "ReplicationScheduler._execute: Obtaining replica information for %d sub-request files." % len( lfns ) )
        res = self.rm.getCatalogReplicas( lfns )
        if not res['OK']:
          gLogger.error( "ReplicationScheduler._execute: Failed to get replica information.", res['Message'] )
          continue
        for lfn, failure in res['Value']['Failed'].items():
          gLogger.error( "ReplicationScheduler._execute: Failed to get replicas.", '%s: %s' % ( lfn, failure ) )
        replicas = res['Value']['Successful']
        if not replicas.keys():
          gLogger.error( "ReplicationScheduler._execute: Failed to get replica information for all files." )
          continue

        ######################################################################################
        #
        #  Now obtain the file sizes for the files associated to the sub-request.
        #

        lfns = replicas.keys()
        gLogger.info( "ReplicationScheduler._execute: Obtaining file sizes for %d sub-request files." % len( lfns ) )
        res = self.rm.getCatalogFileMetadata( lfns )
        if not res['OK']:
          gLogger.error( "ReplicationScheduler._execute: Failed to get file size information.", res['Message'] )
          continue
        for lfn, failure in res['Value']['Failed'].items():
          gLogger.error( 'ReplicationScheduler._execute: Failed to get file size.', '%s: %s' % ( lfn, failure ) )
        metadata = res['Value']['Successful']
        if not metadata.keys():
          gLogger.error( "ReplicationScheduler._execute: Failed to get metadata for all files." )
          continue

        ######################################################################################
        #
        # For each LFN determine the replication tree
        #

        for lfn in sortList( metadata.keys() ):
          fileSize = metadata[lfn]['Size']
          lfnReps = replicas[lfn]
          fileID = filesDict[lfn]

          targets = []
          for targetSE in targetSEs:
            if targetSE in lfnReps.keys():
              gLogger.debug( "ReplicationScheduler.execute: %s already present at %s." % ( lfn, targetSE ) )
            else:
              targets.append( targetSE )
          if not targets:
            gLogger.info( "ReplicationScheduler.execute: %s present at all targets." % lfn )
            oRequest.setSubRequestFileAttributeValue( ind, 'transfer', lfn, 'Status', 'Done' )
            continue
          if not lfnReps:
            gLogger.error( "ReplicationScheduler.execute: The file has no replicas.", lfn )
            continue
          res = self.strategyHandler.determineReplicationTree( sourceSE, targets, lfnReps, fileSize, strategy = reqRepStrategy )
          if not res['OK']:
            gLogger.error( "ReplicationScheduler.execute: Failed to determine replication tree.", res['Message'] )
            continue
          tree = res['Value']

          ######################################################################################
          #
          # For each item in the replication tree obtain the source and target SURLS
          #

          for channelID, dict in tree.items():
            gLogger.info( "ReplicationScheduler.execute: processing for channel %d %s" % ( channelID, str( dict ) ) )
            hopSourceSE = dict['SourceSE']
            hopDestSE = dict['DestSE']
            hopAncestor = dict['Ancestor']

            # Get the sourceSURL
            if hopAncestor:
              status = 'Waiting%s' % ( hopAncestor )
              res = self.obtainLFNSURL( hopSourceSE, lfn )
              if not res['OK']:
                errStr = res['Message']
                gLogger.error( errStr )
                return S_ERROR( errStr )
              sourceSURL = res['Value']
            else:
              status = 'Waiting'
              res = self.resolvePFNSURL( hopSourceSE, lfnReps[hopSourceSE] )
              if not res['OK']:
                sourceSURL = lfnReps[hopSourceSE]
              else:
                sourceSURL = res['Value']

            # Get the targetSURL
            res = self.obtainLFNSURL( hopDestSE, lfn )
            if not res['OK']:
              errStr = res['Message']
              gLogger.error( errStr )
              return S_ERROR( errStr )
            targetSURL = res['Value']

            ######################################################################################
            #
            # For each item in the replication tree add the file to the channel
            #
            res = self.TransferDB.addFileToChannel( channelID, fileID, hopSourceSE, sourceSURL, hopDestSE, targetSURL, fileSize, fileStatus = status )
            if not res['OK']:
              errStr = res['Message']
              gLogger.error( "ReplicationScheduler._execute: Failed to add File to Channel." , "%s %s" % ( fileID, channelID ) )
              return S_ERROR( errStr )
            res = self.TransferDB.addFileRegistration( channelID, fileID, lfn, targetSURL, hopDestSE )
            if not res['OK']:
              errStr = res['Message']
              gLogger.error( "ReplicationScheduler._execute: Failed to add File registration." , "%s %s" % ( fileID, channelID ) )
              result = self.TransferDB.removeFileFromChannel( channelID, fileID )
              if not result['OK']:
                errStr += result['Message']
                gLogger.error( "ReplicationScheduler._execute: Failed to remove File." , "%s %s" % ( fileID, channelID ) )
              return S_ERROR( errStr )
            oRequest.setSubRequestFileAttributeValue( ind, 'transfer', lfn, 'Status', 'Scheduled' )
          res = self.TransferDB.addReplicationTree( fileID, tree )

        if oRequest.isSubRequestEmpty( ind, 'transfer' )['Value']:
          oRequest.setSubRequestStatus( ind, 'transfer', 'Scheduled' )

      ################################################
      #  Generate the new request string after operation
      requestString = oRequest.toXML()['Value']
      res = self.RequestDB.updateRequest( requestName, requestString )
      if not res['OK']:
        gLogger.error( "ReplicationScheduler._execute: Failed to update request", "%s %s" % ( requestName, res['Message'] ) )

  def obtainLFNSURL( self, targetSE, lfn ):
    """ Creates the targetSURL for the storage and LFN supplied
    """
    res = self.factory.getStorages( targetSE, protocolList = ['SRM2'] )
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to create SRM2 storage for %s: %s. ' % ( targetSE, res['Message'] )
      gLogger.error( errStr )
      return S_ERROR( errStr )
    storageObjects = res['Value']['StorageObjects']
    for storageObject in storageObjects:
      res = storageObject.getCurrentURL( lfn )
      if res['OK']:
        return res
    gLogger.error( 'ReplicationScheduler._execute: Failed to get SRM compliant storage.' , targetSE )
    return S_ERROR( 'ReplicationScheduler._execute: Failed to get SRM compliant storage.' )

  def resolvePFNSURL( self, sourceSE, pfn ):
    """ Creates the targetSURL for the storage and LFN supplied
    """
    res = self.rm.getPfnForProtocol( [pfn], sourceSE )
    if not res['OK']:
      return res
    if pfn in res['Value']['Failed'].keys():
      return S_ERROR( res['Value']['Failed'][pfn] )
    return S_OK( res['Value']['Successful'][pfn] )

class StrategyHandler:

  def __init__( self, bandwidths, channels, configSection ):
    """ Standard constructor
    """
    self.supportedStrategies = ['Simple', 'DynamicThroughput', 'Swarm', 'MinimiseTotalWait']
    self.sigma = gConfig.getValue( configSection + '/HopSigma', 0.0 )
    self.schedulingType = gConfig.getValue( configSection + '/SchedulingType', 'File' )
    self.activeStrategies = gConfig.getValue( configSection + '/ActiveStrategies', ['MinimiseTotalWait'] )#['Simple','MinimiseTotalWait','DynamicThroughput'])
    self.numberOfStrategies = len( self.activeStrategies )
    self.acceptableFailureRate = gConfig.getValue( configSection + '/AcceptableFailureRate', 75 )
    self.bandwidths = bandwidths
    self.channels = channels
    self.chosenStrategy = 0

    #print 'Scheduling Type',self.schedulingType
    #print 'Sigma',self.sigma
    #print 'Acceptable failure rate',self.acceptableFailureRate


  def getSupportedStrategies( self ):
    return self.supportedStrategies

  def determineReplicationTree( self, sourceSE, targetSEs, replicas, size, strategy = None, sigma = None ):
    """
    """
    if not strategy:
      strategy = self.__selectStrategy()

    if sigma:
      self.sigma = sigma

    # For each strategy implemented an 'if' must be placed here
    if strategy == 'Simple':
      if not sourceSE in replicas.keys():
        return S_ERROR( 'File does not exist at specified source site.' )
      else:
        tree = self.__simple( sourceSE, targetSEs )

    elif re.search( 'DynamicThroughput', strategy ):
      elements = strategy.split( '_' )
      if len( elements ) > 1:
        self.sigma = float( elements[-1] )
        #print 'SET self.sigma TO BE %s' % self.sigma
      if sourceSE == 'None':
        tree = self.__dynamicThroughput( replicas.keys(), targetSEs )
      else:
        tree = self.__dynamicThroughput( [sourceSE], targetSEs )

    elif re.search( 'MinimiseTotalWait', strategy ):
      elements = strategy.split( '_' )
      if len( elements ) > 1:
        self.sigma = float( elements[-1] )
        #print 'SET self.sigma TO BE %s' % self.sigma
      if sourceSE == 'None':
        tree = self.__minimiseTotalWait( replicas.keys(), targetSEs )
      else:
        tree = self.__minimiseTotalWait( [sourceSE], targetSEs )

    elif strategy == 'Swarm':
      tree = self.__swarm( targetSEs[0], replicas )

    # Now update the queues to reflect the chosen strategies
    for channelID, ancestor in tree.items():
      self.channels[channelID]['Files'] += 1
      self.channels[channelID]['Size'] += size
    return S_OK( tree )

  def __incrementChosenStrategy( self ):
    """ This will increment the counter of the chosen strategy
    """
    self.chosenStrategy += 1
    if self.chosenStrategy == self.numberOfStrategies:
      self.chosenStrategy = 0

  def __selectStrategy( self ):
    """ If more than one active strategy use one after the other
    """
    chosenStrategy = self.activeStrategies[self.chosenStrategy]
    self.__incrementChosenStrategy()
    return chosenStrategy

  def __simple( self, sourceSE, destSEs ):
    """ This just does a simple replication from the source to all the targets
    """
    tree = {}
    if not self.__getActiveSEs( [sourceSE] ):
      return tree
    sourceSites = self.__getChannelSitesForSE( sourceSE )
    for destSE in destSEs:
      destSites = self.__getChannelSitesForSE( destSE )
      for channelID, dict in self.channels.items():
        if tree.has_key( channelID ):
          continue
        if dict['Source'] in sourceSites and dict['Destination'] in destSites:
          tree[channelID] = {}
          tree[channelID]['Ancestor'] = False
          tree[channelID]['SourceSE'] = sourceSE
          tree[channelID]['DestSE'] = destSE
          tree[channelID]['Strategy'] = 'Simple'
    return tree

  def __swarm( self, destSE, replicas ):
    """ This strategy is to be used to the data the the target site as quickly as possible from any source
    """
    selected = False
    res = self.__getTimeToStart()
    if not res['OK']:
      gLogger.error( res['Message'] )
      return {}
    channelInfo = res['Value']
    minTimeToStart = float( "inf" )
    sourceSEs = self.__getActiveSEs( replicas.keys() )
    destSites = self.__getChannelSitesForSE( destSE )
    for destSite in destSites:
      for sourceSE in sourceSEs:
        sourceSites = self.__getChannelSitesForSE( sourceSE )
        for sourceSite in sourceSites:
          channelName = '%s-%s' % ( sourceSite, destSite )
          if channelInfo.has_key( channelName ):
            channelID = channelInfo[channelName]['ChannelID']
            channelTimeToStart = channelInfo[channelName]['TimeToStart']
            if channelTimeToStart <= minTimeToStart:
              minTimeToStart = channelTimeToStart
              selectedSourceSE = sourceSE
              selectedDestSE = destSE
              selectedChannelID = channelID
              selected = True
          else:
            errStr = 'StrategyHandler.__swarm: Channel not defined'
            gLogger.warn( errStr, channelName )

    tree = {}
    if selected:
      tree[selectedChannelID] = {}
      tree[selectedChannelID]['Ancestor'] = False
      tree[selectedChannelID]['SourceSE'] = selectedSourceSE
      tree[selectedChannelID]['DestSE'] = selectedDestSE
      tree[selectedChannelID]['Strategy'] = 'Swarm'
    return tree

  def __dynamicThroughput( self, sourceSEs, destSEs ):
    """ This creates a replication tree based on observed throughput on the channels
    """
    res = self.__getTimeToStart()
    if not res['OK']:
      gLogger.error( res['Message'] )
      return {}
    channelInfo = res['Value']

    timeToSite = {}                # Maintains time to site including previous hops
    siteAncestor = {}              # Maintains the ancestor channel for a site
    tree = {}                      # Maintains replication tree
    while len( destSEs ) > 0:
      minTotalTimeToStart = float( "inf" )
      candidateChannels = []
      sourceActiveSEs = self.__getActiveSEs( sourceSEs )
      for destSE in destSEs:
        destSites = self.__getChannelSitesForSE( destSE )
        localTransfer = False
        for destSite in destSites:
          if localTransfer: break
          for sourceSE in sourceActiveSEs:
            if localTransfer: break
            sourceSites = self.__getChannelSitesForSE( sourceSE )
            for sourceSite in sourceSites:
              if localTransfer: break
              channelName = '%s-%s' % ( sourceSite, destSite )
              if channelInfo.has_key( channelName ):
                channelID = channelInfo[channelName]['ChannelID']
                if tree.has_key( channelID ):
                  continue
                channelTimeToStart = channelInfo[channelName]['TimeToStart']
                if timeToSite.has_key( sourceSE ):
                  totalTimeToStart = timeToSite[sourceSE] + channelTimeToStart + self.sigma
                else:
                  totalTimeToStart = channelTimeToStart
                #print '%s-%s %s %s' % (sourceSE,destSE,channelTimeToStart,totalTimeToStart)
                localTransfer = ( sourceSite == destSite )
                if localTransfer or totalTimeToStart < minTotalTimeToStart:
                  minTotalTimeToStart = totalTimeToStart
                  selectedPathTimeToStart = totalTimeToStart
                  candidateChannels = [( sourceSE, destSE, channelID )]
                elif totalTimeToStart == minTotalTimeToStart:
                  candidateChannels.append( ( sourceSE, destSE, channelID ) )

                  #minTotalTimeToStart = totalTimeToStart
                  #selectedPathTimeToStart = totalTimeToStart
                  #selectedSourceSE = sourceSE
                  #selectedDestSE = destSE
                  #selectedChannelID = channelID
              else:
                errStr = 'StrategyHandler.__dynamicThroughput: Channel not defined'
                gLogger.warn( errStr, channelName )

      #print 'Selected %s \n' % selectedPathTimeToStart

      random.shuffle( candidateChannels )
      selectedSourceSE, selectedDestSE, selectedChannelID = candidateChannels[0]
      timeToSite[selectedDestSE] = selectedPathTimeToStart
      siteAncestor[selectedDestSE] = selectedChannelID

      if siteAncestor.has_key( selectedSourceSE ):
        waitingChannel = siteAncestor[selectedSourceSE]
      else:
        waitingChannel = False
      tree[selectedChannelID] = {}
      tree[selectedChannelID]['Ancestor'] = waitingChannel
      tree[selectedChannelID]['SourceSE'] = selectedSourceSE
      tree[selectedChannelID]['DestSE'] = selectedDestSE
      tree[selectedChannelID]['Strategy'] = 'DynamicThroughput'
      sourceSEs.append( selectedDestSE )
      destSEs.remove( selectedDestSE )
    return tree

  def __minimiseTotalWait( self, sourceSEs, destSEs ):
    """ This creates a replication tree based on observed throughput on the channels
    """
    res = self.__getTimeToStart()
    if not res['OK']:
      gLogger.error( res['Message'] )
      return {}
    channelInfo = res['Value']


    timeToSite = {}                # Maintains time to site including previous hops
    siteAncestor = {}              # Maintains the ancestor channel for a site
    tree = {}                      # Maintains replication tree
    primarySources = sourceSEs

    while len( destSEs ) > 0:
      minTotalTimeToStart = float( "inf" )
      candidateChannels = []
      sourceActiveSEs = self.__getActiveSEs( sourceSEs )
      for destSE in destSEs:
        destSites = self.__getChannelSitesForSE( destSE )
        localTransfer = False
        for destSite in destSites:
          if localTransfer: break
          for sourceSE in sourceActiveSEs:
            if localTransfer: break
            sourceSites = self.__getChannelSitesForSE( sourceSE )
            for sourceSite in sourceSites:
              if localTransfer: break
              channelName = '%s-%s' % ( sourceSite, destSite )
              if channelInfo.has_key( channelName ):
                channelID = channelInfo[channelName]['ChannelID']
                # If this channel is already used, look for another sourceSE
                if tree.has_key( channelID ):
                  continue
                channelTimeToStart = channelInfo[channelName]['TimeToStart']
                if not sourceSE in primarySources:
                  channelTimeToStart += self.sigma

                localTransfer = ( sourceSite == destSite )
                if localTransfer or minTotalTimeToStart == float( "inf" ) or channelTimeToStart < minTotalTimeToStart:
                  minTotalTimeToStart = channelTimeToStart
                  selectedPathTimeToStart = channelTimeToStart
                  candidateChannels = [( sourceSE, destSE, channelID )]

                elif ( channelTimeToStart == minTotalTimeToStart ):
                  candidateChannels.append( ( sourceSE, destSE, channelID ) )
              else:
                errStr = 'StrategyHandler.__minimiseTotalWait: Channel not defined'
                gLogger.warn( errStr, channelName )


      if not candidateChannels:
        return {}
      random.shuffle( candidateChannels )
      selectedSourceSE, selectedDestSE, selectedChannelID = candidateChannels[0]
      timeToSite[selectedDestSE] = selectedPathTimeToStart
      siteAncestor[selectedDestSE] = selectedChannelID

      if siteAncestor.has_key( selectedSourceSE ):
        waitingChannel = siteAncestor[selectedSourceSE]
      else:
        waitingChannel = False
      tree[selectedChannelID] = {}
      tree[selectedChannelID]['Ancestor'] = waitingChannel
      tree[selectedChannelID]['SourceSE'] = selectedSourceSE
      tree[selectedChannelID]['DestSE'] = selectedDestSE
      tree[selectedChannelID]['Strategy'] = 'MinimiseTotalWait'
      sourceSEs.append( selectedDestSE )
      destSEs.remove( selectedDestSE )
    return tree

  def __getTimeToStart( self ):
    """ Generate the matrix of times to start based on task queue contents and observed throughput
    """
    channelInfo = {}
    for channelID, value in self.bandwidths.items():
      channelDict = self.channels[channelID]
      channelFiles = channelDict['Files']
      channelSize = channelDict['Size']
      status = channelDict['Status']
      channelName = channelDict['ChannelName']
      channelInfo[channelName] = {'ChannelID': channelID}

      if status != 'Active':
        throughputTimeToStart = float( 'inf' ) # Make the channel extremely unattractive but still available
        fileTimeToStart = float( 'inf' ) #Make the channel extremely unattractive but still available
      else:
        channelThroughput = value['Throughput']
        channelFileput = value['Fileput']
        channelFileSuccess = value['SuccessfulFiles']
        channelFileFailed = value['FailedFiles']
        attempted = channelFileSuccess + channelFileFailed
        if attempted != 0:
          successRate = 100.0 * ( channelFileSuccess / float( attempted ) )
        else:
          successRate = 100.0
        if successRate < self.acceptableFailureRate:
          #print 'This channel is failing %s' % channelName
          throughputTimeToStart = float( 'inf' ) # Make the channel extremely unattractive but still available
          fileTimeToStart = float( 'inf' ) # Make the channel extremely unattractive but still available
        else:
          if channelFileput > 0:
            fileTimeToStart = channelFiles / float( channelFileput )
          else:
            fileTimeToStart = 0.0

          if channelThroughput > 0:
            throughputTimeToStart = channelSize / float( channelThroughput )
          else:
            throughputTimeToStart = 0.0

      if self.schedulingType == 'File':
        channelInfo[channelName]['TimeToStart'] = fileTimeToStart
      elif self.schedulingType == 'Throughput':
        channelInfo[channelName]['TimeToStart'] = throughputTimeToStart
      else:
        errStr = 'StrategyHandler.__dynamicThroughput: CS SchedulingType entry must be either File or Throughput'
        gLogger.error( errStr )
        return S_ERROR( errStr )

    return S_OK( channelInfo )

  def __getActiveSEs( self, selist, access = "Read" ):
    activeSE = []
    for se in selist:
      
      rssClient = ResourceStatusClient()
      res       = rssClient.getStorageElement( se, access )  
      if res['OK'] and ( res['Value'][1] == 'Active' or res['Value'][1] == 'Bad' ):
        activeSE.append( se )
      
      #res = gConfig.getOption( '/Resources/StorageElements/%s/%sAccess' % ( se, access ), 'Unknown' )
      #if res['OK'] and res['Value'] == 'Active':
      #  activeSE.append( se )
    return activeSE

  def __getChannelSitesForSE( self, se ):
    res = getSitesForSE( se )
    sites = []
    if res['OK']:
      for site in res['Value']:
        s = site.split( '.' )
        if len( s ) > 1:
          if not s[1] in sites:
            sites.append( s[1] )
    return sites



