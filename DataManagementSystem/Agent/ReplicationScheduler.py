"""  Replication Scheduler assigns replication requests to channels
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection

from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.Storage.StorageFactory import StorageFactory
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
import types,re


AGENT_NAME = 'DataManagement/ReplicationScheduler'

class ReplicationScheduler(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDB = RequestDBMySQL()
    self.TransferDB = TransferDB()
    self.factory = StorageFactory()
    try:
      self.lfc = FileCatalog()
    except Exception,x:
      print "Failed to create FileCatalog()"
      print str(x)
    return result

  def execute(self):
    """ The main agent execution method
    """
    # This allows dynamic changing of the throughput timescale
    self.throughputTimescale = gConfig.getValue(self.section+'/ThroughputTimescale',3600)

    ######################################################################################
    #
    #  Obtain information on the current state of the channel queues
    #

    res = self.TransferDB.getChannelQueues()
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to get channel queues from TransferDB: %s.' % res['Message']
      self.log.error(errStr)
      return S_ERROR(errStr)
    if not res['Value']:
      infoStr = 'ReplicationScheduler._execute: No active channels found for replication.'
      self.log.info(infoStr)
      return S_OK()
    channels = res['Value']

    res = self.TransferDB.getChannelObservedThroughput(self.throughputTimescale)
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to get observed throughput from TransferDB: %s.' % res['Message']
      self.log.error(errStr)
      return S_ERROR(errStr)
    if not res['Value']:
      infoStr = 'ReplicationScheduler._execute: No active channels found for replication.'
      self.log.info(infoStr)
      return S_OK()
    bandwidths = res['Value']

    self.strategyHandler = StrategyHandler(bandwidths,channels,self.section)

    ######################################################################################
    #
    #  The first step is to obtain a transfer request from the RequestDB which should be scheduled.
    #

    logStr = 'ReplicationScheduler._execute: Contacting RequestDB for suitable requests.'
    self.log.info(logStr)
    res = self.RequestDB.getRequest('transfer')
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to get a request list from RequestDB: %s.' % res['Message']
      self.log.error(errStr)
      return S_ERROR(errStr)
    if not res['Value']:
      logStr = 'ReplicationScheduler._execute: No requests found in RequestDB.'
      self.log.info(logStr)
      return S_OK()
    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    logStr = 'ReplicationScheduler._execute: Obtained Request %s from RequestDB.' % (requestName)
    self.log.info(logStr)

    ######################################################################################
    #
    #  The request must then be parsed to obtain the sub-requests, their attributes and files.
    #

    logStr = 'ReplicationScheduler._execute: Parsing Request %s.' % (requestName)
    self.log.info(logStr)
    dmRequest = DataManagementRequest(requestString)
    res = dmRequest.getNumSubRequests('transfer')
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to get number of sub-requests: %s.' % res['Message']
      self.log.error(errStr)
      return S_ERROR(errStr)
    numberRequests = res['Value']
    logStr = "ReplicationScheduler._execute: '%s' found with %s sub-requests." % (requestName,numberRequests)
    self.log.info(logStr)

    ######################################################################################
    #
    #  The important request attributes are the source and target SEs.
    #

    for ind in range(numberRequests):
      logStr = "ReplicationScheduler._execute: Treating sub-request %s from '%s'." % (ind,requestName)
      self.log.info(logStr)
      res = dmRequest.getSubRequestAttributes(ind,'transfer')
      if not res['OK']:
        errStr = 'ReplicationScheduler._execute: Failed to obtain sub-request attributes: %s.' % res['Message']
        self.log.error(errStr)
        return S_ERROR(errStr)
      attributes = res['Value']
      sourceSE = attributes['SourceSE']
      targetSE = attributes['TargetSE']
      """ This section should go in the transfer request class """
      if type(targetSE) in types.StringTypes:
        if re.search(',',targetSE):
          targetSEs = targetSE.split(',')
        else:
          targetSEs = [targetSE]
      """----------------------------------------------------- """

      operation = attributes['Operation']
      spaceToken = attributes['SpaceToken']

      ######################################################################################
      #
      # Then obtain the file attribute of interest are the  LFN and FileID
      #

      res = dmRequest.getSubRequestFiles(ind,'transfer')
      if not res['OK']:
        errStr = 'ReplicationScheduler._execute: Failed to obtain sub-request files: %s.' % res['Message']
        self.log.error(errStr)
        return S_ERROR(errStr)
      files = res['Value']
      logStr = 'ReplicationScheduler._execute: Sub-request %s found with %s files.' % (ind,len(files))
      self.log.info(logStr)
      filesDict = {}
      for file in files:
        lfn = file['LFN']
        fileID = file['FileID']
        filesDict[lfn] = fileID

      ######################################################################################
      #
      #  Now obtain replica information for the files associated to the sub-request.
      #

      logStr = 'ReplicationScheduler._execute: Obtaining replica information for sub-request files.'
      self.log.info(logStr)
      lfns = filesDict.keys()
      res = self.lfc.getReplicas(lfns)
      if not res['OK']:
        errStr = 'ReplicationScheduler._execute: Failed to get replica infomation: %s.' % res['Message']
        self.log.error(errStr)
        return S_ERROR(errStr)
      replicas = res['Value']['Successful']

      ######################################################################################
      #
      #  Now obtain the file sizes for the files associated to the sub-request.
      #

      logStr = 'ReplicationScheduler._execute: Obtaining file sizes for sub-request files.'
      self.log.info(logStr)
      lfns = filesDict.keys()
      res = self.lfc.getFileMetadata(lfns)
      if not res['OK']:
        errStr = 'ReplicationScheduler._execute: Failed to get file size infomation: %s.' % res['Message']
        self.log.error(errStr)
        return S_ERROR(errStr)
      metadata = res['Value']['Successful']

      ######################################################################################
      #
      # For each lfn determine the replication tree
      #

      for lfn in filesDict.keys():
        fileSize = metadata[lfn]['Size']
        lfnReps = replicas[lfn]
        fileID = filesDict[lfn]
        res = self.strategyHandler.determineReplicationTree(sourceSE,targetSEs,lfnReps,fileSize)
        if not res['OK']:
          errStr = res['Message']
          gLogger.error(errStr)
          return S_ERROR(errStr)
        tree = res['Value']

        ######################################################################################
        #
        # For each item in the replication tree obtain the source and target SURLS
        #

        for channelID,dict in tree.items():
          sourceSE = dict['SourceSE']
          destSE = dict['DestSE']
          ancestor = dict['Ancestor']
          if ancestor:
            status = 'Waiting%s' % (ancestor)
            res = self.obtainLFNSURL(sourceSE,lfn)
            if not res['OK']:
              errStr = res['Message']
              gLogger.error(errStr)
              return S_ERROR(errStr)
            sourceSURL = res['Value']['SURL']
          else:
            status = 'Waiting'
            res  = self.resolvePFNSURL(sourceSE,lfnReps[sourceSE])
            if not res['OK']:
              sourceSURL = lfnReps[sourceSE]
            else:
              sourceSURL = res['Value']
          res = self.obtainLFNSURL(destSE,lfn)
          if not res['OK']:
            errStr = res['Message']
            gLogger.error(errStr)
            return S_ERROR(errStr)
          targetSURL = res['Value']['SURL']
          spaceToken = res['Value']['SpaceToken']

          ######################################################################################
          #
          # For each item in the replication tree add the file to the channel
          #

          res = self.TransferDB.addFileToChannel(channelID, fileID, sourceSURL, targetSURL,fileSize,spaceToken,fileStatus=status)
          if not res['OK']:
            errStr = "ReplicationScheduler._execute: Failed to add File %s to Channel %s." % (fileID,channelID)
            gLogger.error(errStr)
            return S_ERROR(errStr)
          res = self.TransferDB.addFileRegistration(channelID,fileID,lfn,targetSURL,destSE)
          if not res['OK']:
            errStr = "ReplicationScheduler._execute: Failed to add registration entry %s to %s." % (fileID,destSE)
            gLogger.error(errStr)
            return S_ERROR(errStr)
        res = self.TransferDB.addReplicationTree(fileID,tree)
    return res


  def obtainLFNSURL(self,targetSE,lfn):
    """ Creates the targetSURL for the storage and LFN supplied
    """
    res = self.factory.getStorages(targetSE,protocolList=['SRM2'])
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to create SRM2 storage for %s: %s. ' % (targetSE,res['Message'])
      self.log.error(errStr)
      return S_ERROR(errStr)
    storageObjects = res['Value']['StorageObjects']
    for storageObject in storageObjects:
       res =  storageObject.getCurrentURL(lfn)
       if res['OK']:
         resDict = {'SURL':res['Value']}
         res = storageObject.getParameters()
         if res['OK']:
           resDict['SpaceToken'] = res['Value']['SpaceToken']
           return S_OK(resDict)
    errStr = 'ReplicationScheduler._execute: Failed to get SRM compliant storage for %s.' % targetSE
    self.log.error(errStr)
    return S_ERROR(errStr)

  def resolvePFNSURL(self,sourceSE,pfn):
    """ Creates the targetSURL for the storage and LFN supplied
    """
    storageElement = StorageElement(sourceSE)
    if storageElement.isValid()['Value']:
      res = storageElement.getPfnForProtocol(pfn,['SRM2'])
      return res
    else:
      errStr = "ReplicationScheduler._execute: Failed to get source PFN for %s." % sourceSE
      self.log.error(errStr)
      return S_ERROR(errStr)

class StrategyHandler:

  def __init__(self,bandwidths,channels,configSection):
    """ Standard constructor
    """
    self.sigma = gConfig.getValue(configSection+'/HopSigma',1)
    self.schedulingType = gConfig.getValue(configSection+'/SchedulingType','File')
    self.activeStrategies = gConfig.getValue(configSection+'/ActiveStrategies',['Simple'])
    self.numberOfStrategies = len(self.activeStrategies)
    self.chosenStrategy = 0

    self.acceptableFailureRate = gConfig.getValue(configSection+'/AcceptableFailureRate',75)
    self.bandwidths = bandwidths
    self.channels = channels

  def determineReplicationTree(self,sourceSE,targetSEs,replicas,size,strategy=None):
    """
    """
    if not strategy:
      strategy = self.__selectStrategy()

    # For each strategy implemented an 'if' must be placed here
    if strategy == 'Simple':
      tree = self.__simple(sourceSE,targetSEs)
    elif strategy == 'DynamicThroughput':
      tree = self.__dynamicThroughput(sourceSE,targetSEs)

    # Now update the queues to reflect the chosen strategies
    for channelID,ancestor in tree.items():
      self.channels[channelID]['Files'] += 1
      self.channels[channelID]['Size'] += size
    return S_OK(tree)

  def __incrementChosenStrategy(self):
    """ This will increment the counter of the chosen strategy
    """
    self.chosenStrategy += 1
    if self.chosenStrategy == self.numberOfStrategies:
      self.chosenStrategy = 0

  def __selectStrategy(self):
    """ If more than one active strategy use one after the other
    """
    chosenStrategy = self.activeStrategies[self.chosenStrategy]
    self.__incrementChosenStrategy()
    return chosenStrategy

  def __simple(self,sourceSE,targetSEs):
    """ This just does a simple replication from the source to all the targets
    """
    tree = {}
    for targetSE in targetSEs:
      for channelID,dict in self.channels.items():
        if re.search(dict['Source'],sourceSE) and re.search(dict['Destination'],targetSE):
          tree[channelID] = {}
          tree[channelID]['Ancestor'] = False
          tree[channelID]['SourceSE'] = sourceSE
          tree[channelID]['DestSE'] = targetSE
          tree[channelID]['Strategy'] = 'Simple'
    return tree

  def __dynamicThroughput(self,sourceSE,targetSEs):
    """ This creates a replication tree based on observed throughput on the channels
    """
    ############################################################################
    #
    # First generate the matrix of times to start based on task queue contents and observed throughput
    #

    channelIDs = {}
    channelsTimeToStart = {}
    for channelID,value in self.bandwidths.items():
      channelDict = self.channels[channelID]
      channelFiles = channelDict['Files']
      channelSize = channelDict['Size']
      status = channelDict['Status']
      channelName = channelDict['ChannelName']
      channelIDs[channelName] = channelID

      if status == 'Disabled':
        throughputTimeToStart = 1e10 # Make the channel extremely unattractive but still available
        fileTimeToStart = 1e10 #Make the channel extremely unattractive but still available
      else:
        channelThroughput = value['Throughput']
        channelFileput = value['Fileput']
        channelFileSuccess = value['SuccessfulFiles']
        channelFileFailed = value['FailedFiles']
        attempted = channelFileSuccess+channelFileFailed
        if attempted != 0:
          successRate = 100.0*(channelFileSuccess/float(attempted))
        else:
          successRate = 100.0
        if successRate < self.acceptableFailureRate:
          throughputTimeToStart = 1e10 # Make the channel extremely unattractive but still available
          fileTimeToStart = 1e10 # Make the channel extremely unattractive but still available
        else:
          if channelFiles > 0:
            fileTimeToStart = channelFiles/channelFileput
          else:
            fileTimeToStart = 0.0
          if channelSize > 0:
            throughputTimeToStart = channelSize/channelThroughput
          else:
            throughputTimeToStart = 0.0

      if self.schedulingType == 'File':
        channelsTimeToStart[channelID] = fileTimeToStart
      elif self.schedulingType == 'Throughput':
        channelsTimeToStart[channelID] = throughputTimeToStart
      else:
        errStr = 'StrategyHandler.__dynamicThroughput: CS SchedulingType entry must be either File or Throughput'
        gLogger.error(errStr)
        return S_ERROR(errStr)

    ############################################################################
    #
    # Use the matrix of time to start to create scheduling tree
    #

    timeToSite = {}                # Maintains time to site including previous hops
    siteAncestor = {}              # Maintains the ancestor channel for a site
    tree = {}                      # Maintains replication tree
    sourceSEs = [sourceSE]
    destSEs = targetSEs

    while len(destSEs) > 0:
      minTotalTimeToStart = float("inf")
      for destSE in destSEs:
        destSite = destSE.split('-')[0].split('_')[0]
        for sourceSE in sourceSEs:
          sourceSite = sourceSE.split('-')[0].split('_')[0]
          channelName = '%s-%s' % (sourceSite,destSite)
          if channelIDs.has_key(channelName):
            channelID = channelIDs[channelName]
            channelTimeToStart = channelsTimeToStart[channelID]
            if timeToSite.has_key(sourceSE):
              totalTimeToStart = timeToSite[sourceSE]+channelTimeToStart+self.sigma
            else:
              totalTimeToStart = channelTimeToStart
            if totalTimeToStart <= minTotalTimeToStart:
              minTotalTimeToStart = totalTimeToStart
              selectedPathTimeToStart = totalTimeToStart
              selectedSourceSE = sourceSE
              selectedDestSE = destSE
              selectedChannelID = channelID
          else:
            errStr = 'StrategyHandler.__dynamicThroughput: Channel not defined'
            gLogger.error(errStr,channelName)
      timeToSite[selectedDestSE] = selectedPathTimeToStart
      siteAncestor[selectedDestSE] = selectedChannelID

      if siteAncestor.has_key(selectedSourceSE):
        waitingChannel = siteAncestor[selectedSourceSE]
      else:
        waitingChannel = False
      tree[selectedChannelID] = {}
      tree[selectedChannelID]['Ancestor'] = waitingChannel
      tree[selectedChannelID]['SourceSE'] = selectedSourceSE
      tree[selectedChannelID]['DestSE'] = selectedDestSE
      tree[selectedChannelID]['Strategy'] = 'DynamicThroughput'
      sourceSEs.append(selectedDestSE)
      destSEs.remove(selectedDestSE)
    return tree
