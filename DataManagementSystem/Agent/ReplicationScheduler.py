"""  Replication Scheduler assigns replication requests to channels
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
 

from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.Storage.StorageFactory import StorageFactory
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

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
    self.DataLog = DataLoggingClient()
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
      errStr = "ReplicationScheduler._execute: Failed to get channel queues from TransferDB."
      gLogger.error(errStr, res['Message'])
      return S_OK()
    if not res['Value']:
      gLogger.info("ReplicationScheduler._execute: No active channels found for replication.")
      return S_OK()
    channels = res['Value']

    res = self.TransferDB.getChannelObservedThroughput(self.throughputTimescale)
    if not res['OK']:
      errStr = "ReplicationScheduler._execute: Failed to get observed throughput from TransferDB."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    if not res['Value']:
      gLogger.info("ReplicationScheduler._execute: No active channels found for replication.")
      return S_OK()
    bandwidths = res['Value']

    self.strategyHandler = StrategyHandler(bandwidths,channels,self.section)

    ######################################################################################
    #
    #  The first step is to obtain a transfer request from the RequestDB which should be scheduled.
    #

    gLogger.info("ReplicationScheduler._execute: Contacting RequestDB for suitable requests.")
    res = self.RequestDB.getRequest('transfer')
    if not res['OK']:
      errStr = "ReplicationScheduler._execute: Failed to get a request list from RequestDB."
      gLogger.error(errStr, res['Message'])
      return S_OK()
    if not res['Value']:
      gLogger.info("ReplicationScheduler._execute: No requests found in RequestDB.")
      return S_OK()
    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    gLogger.info("ReplicationScheduler._execute: Obtained Request %s from RequestDB.' % (requestName)")

    ######################################################################################
    #
    #  The request must then be parsed to obtain the sub-requests, their attributes and files.
    #

    logStr = 'ReplicationScheduler._execute: Parsing Request %s.' % (requestName)
    gLogger.info(logStr)
    oRequest = RequestContainer(requestString)
    res = oRequest.getNumSubRequests('transfer')
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to get number of sub-requests: %s.' % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    numberRequests = res['Value']
    logStr = "ReplicationScheduler._execute: '%s' found with %s sub-requests." % (requestName,numberRequests)
    gLogger.info(logStr)

    ######################################################################################
    #
    #  The important request attributes are the source and target SEs.
    #

    for ind in range(numberRequests):
     gLogger.info("ReplicationScheduler._execute: Treating sub-request %s from '%s'." % (ind,requestName))
     attributes = oRequest.getSubRequestAttributes(ind,'transfer')['Value']
     if attributes['Status'] != 'Waiting':
      #  If the sub-request is already in terminal state
      gLogger.info("ReplicationScheduler._execute: Sub-request %s is status '%s' and  not to be executed." % (ind,attributes['Status']))
     else:     
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
      reqRepStrategy = None
      if operation in self.strategyHandler.getSupportedStrategies():
        reqRepStrategy = operation

      ######################################################################################
      #
      # Then obtain the file attribute of interest are the  LFN and FileID
      #

      res = oRequest.getSubRequestFiles(ind,'transfer')
      if not res['OK']:
        errStr = 'ReplicationScheduler._execute: Failed to obtain sub-request files: %s.' % res['Message']
        gLogger.error(errStr)
        return S_ERROR(errStr)
      files = res['Value']
      logStr = 'ReplicationScheduler._execute: Sub-request %s found with %s files.' % (ind,len(files))
      gLogger.info(logStr)
      filesDict = {}
      for file in files:
        lfn = file['LFN']
        fileID = file['FileID']
        filesDict[lfn] = fileID

      ######################################################################################
      #
      #  Now obtain replica information for the files associated to the sub-request.
      #

      gLogger.info("ReplicationScheduler._execute: Obtaining replica information for sub-request files.")
      lfns = filesDict.keys()
      res = self.lfc.getReplicas(lfns)
      if not res['OK']:
        errStr = "ReplicationScheduler._execute: Failed to get replica infomation."
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      for lfn,failure in res['Value']['Failed'].items(): 
        gLogger.error("ReplicationScheduler._execute: Failed to get replicas.",'%s: %s' % (lfn,failure))
      replicas = res['Value']['Successful']
      if not replicas.keys():
        gLogger.error("ReplicationScheduler._execute: Failed to get replica information for all files.")

      ######################################################################################
      #
      #  Now obtain the file sizes for the files associated to the sub-request.
      #

      gLogger.info("ReplicationScheduler._execute: Obtaining file sizes for sub-request files.")
      lfns = replicas.keys()
      res = self.lfc.getFileMetadata(lfns)
      if not res['OK']:
        errStr = "ReplicationScheduler._execute: Failed to get file size information."
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      for lfn,failure in res['Value']['Failed'].items():
        gLogger.error('ReplicationScheduler._execute: Failed to get file size.','%s: %s' % (lfn,failure))
      metadata = res['Value']['Successful']
      if not metadata.keys():
        gLogger.error("ReplicationScheduler._execute: Failed to get metadata for all files.")


      ######################################################################################
      #
      # For each lfn determine the replication tree
      #

      for lfn in metadata.keys():
        fileSize = metadata[lfn]['Size']
        lfnReps = replicas[lfn]
        fileID = filesDict[lfn]

        targets = []
        for targetSE in targetSEs:
          if targetSE in lfnReps.keys():
            gLogger.info("ReplicationScheduler.execute: %s already present at %s." % (lfn,targetSE))
          else:
            targets.append(targetSE)

        if not targets:
          gLogger.info("ReplicationScheduler.execute: %s present at all targets." % lfn)
        else:
          res = self.strategyHandler.determineReplicationTree(sourceSE,targets,lfnReps,fileSize,strategy=reqRepStrategy)
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
            hopSourceSE = dict['SourceSE']
            hopDestSE = dict['DestSE']
            hopAncestor = dict['Ancestor']
            if hopAncestor:
              status = 'Waiting%s' % (hopAncestor)
              res = self.obtainLFNSURL(hopSourceSE,lfn)
              if not res['OK']:
                errStr = res['Message']
                gLogger.error(errStr)
                return S_ERROR(errStr)
              sourceSURL = res['Value']['SURL']
            else:
              status = 'Waiting'
              res  = self.resolvePFNSURL(hopSourceSE,lfnReps[hopSourceSE])
              if not res['OK']:
                sourceSURL = lfnReps[hopSourceSE]
              else:
                sourceSURL = res['Value']
            res = self.obtainLFNSURL(hopDestSE,lfn)
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
            channelName = '%s-%s' % (hopSourceSE,hopDestSE)
            self.DataLog.addFileRecord(str(lfn),'ReplicationScheduled',channelName,'','ReplicationScheduler')
            res = self.TransferDB.addFileToChannel(channelID, fileID, sourceSURL, targetSURL,fileSize,spaceToken,fileStatus=status)
            if not res['OK']:
              errStr = "ReplicationScheduler._execute: Failed to add File %s to Channel %s." % (fileID,channelID)
              gLogger.error(errStr)
              return S_ERROR(errStr)
            res = self.TransferDB.addFileRegistration(channelID,fileID,lfn,targetSURL,hopDestSE)
            if res['OK']:
              oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Scheduled')               
            else:
              errStr = "ReplicationScheduler._execute: Failed to add registration entry."  
              gLogger.error(errStr, "%s to %s." % (fileID,hopDestSE))
          res = self.TransferDB.addReplicationTree(fileID,tree)

        if oRequest.isSubRequestEmpty(ind,'transfer')['Value']:
          oRequest.setSubRequestStatus(ind,'transfer','Scheduled')

    ################################################
    #  Generate the new request string after operation
    requestString = oRequest.toXML()['Value']
    res = self.RequestDB.updateRequest(requestName,requestString)
    return res

  def obtainLFNSURL(self,targetSE,lfn):
    """ Creates the targetSURL for the storage and LFN supplied
    """
    res = self.factory.getStorages(targetSE,protocolList=['SRM2'])
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to create SRM2 storage for %s: %s. ' % (targetSE,res['Message'])
      gLogger.error(errStr)
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
    gLogger.error(errStr)
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
      gLogger.error(errStr)
      return S_ERROR(errStr)

class StrategyHandler:

  def __init__(self,bandwidths,channels,configSection):
    """ Standard constructor
    """
    self.supportedStrategies = ['Simple','DynamicThroughput','Swarm']
    self.sigma = gConfig.getValue(configSection+'/HopSigma',1)
    self.schedulingType = gConfig.getValue(configSection+'/SchedulingType','File')
    self.activeStrategies = gConfig.getValue(configSection+'/ActiveStrategies',['Simple','DynamicThroughput'])
    self.numberOfStrategies = len(self.activeStrategies)
    self.acceptableFailureRate = gConfig.getValue(configSection+'/AcceptableFailureRate',75)
    self.bandwidths = bandwidths
    self.channels = channels
    self.chosenStrategy = 0


  def getSupportedStrategies(self):
    return self.supportedStrategies

  def determineReplicationTree(self,sourceSE,targetSEs,replicas,size,strategy=None):
    """
    """
    if not strategy:
      strategy = self.__selectStrategy()

    # For each strategy implemented an 'if' must be placed here
    if strategy == 'Simple':
      tree = self.__simple(sourceSE,targetSEs)

    elif strategy == 'DynamicThroughput':
      if sourceSE:
        tree = self.__dynamicThroughput([sourceSE],targetSEs)
      else:
        tree = self.__dynamicThroughput(replica.keys(),targetSEs)

    elif strategy == 'Swarm':
      tree = self.__swarm(targetSEs[0],replicas)

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

  def __swarm(self,destSE,replicas):
    """ This strategy is to be used to the data the the target site as quickly as possible from any source
    """
    res = self.__getTimeToStart()
    if not res['OK']:
      gLogger.error(res['Message'])
      return {}
    channelInfo = res['Value']
    minTimeToStart = float("inf")
    destSite = destSE.split('-')[0].split('_')[0]
    for sourceSE in replicas.keys():
      sourceSite = sourceSE.split('-')[0].split('_')[0]
      channelName = '%s-%s' % (sourceSite,destSite)
      if channelInfo.has_key(channelName):
        channelID = channelInfo[channelName]['ChannelID']
        channelTimeToStart = channelInfo[channelName]['TimeToStart']
        if channelTimeToStart <= minTimeToStart:
          minTimeToStart = channelTimeToStart
          selectedSourceSE = sourceSE
          selectedDestSE = destSE
          selectedChannelID = channelID
      else:
        errStr = 'StrategyHandler.__swarm: Channel not defined'
        gLogger.error(errStr,channelName)
        waitingChannel = False

    tree = {}
    tree[selectedChannelID] = {}
    tree[selectedChannelID]['Ancestor'] = False
    tree[selectedChannelID]['SourceSE'] = selectedSourceSE
    tree[selectedChannelID]['DestSE'] = selectedDestSE
    tree[selectedChannelID]['Strategy'] = 'Swarm'
    return tree

  def __dynamicThroughput(self,sourceSEs,destSEs):
    """ This creates a replication tree based on observed throughput on the channels
    """
    res = self.__getTimeToStart()
    if not res['OK']:
      gLogger.error(res['Message'])
      return {}
    channelInfo = res['Value']

    timeToSite = {}                # Maintains time to site including previous hops
    siteAncestor = {}              # Maintains the ancestor channel for a site
    tree = {}                      # Maintains replication tree

    while len(destSEs) > 0:
      minTotalTimeToStart = float("inf")
      for destSE in destSEs:
        destSite = destSE.split('-')[0].split('_')[0]
        for sourceSE in sourceSEs:
          sourceSite = sourceSE.split('-')[0].split('_')[0]
          channelName = '%s-%s' % (sourceSite,destSite)
          if channelInfo.has_key(channelName):
            channelID = channelInfo[channelName]['ChannelID']
            channelTimeToStart = channelInfo[channelName]['TimeToStart']
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

  def __getTimeToStart(self):
    """ Generate the matrix of times to start based on task queue contents and observed throughput
    """
    channelInfo = {}
    for channelID,value in self.bandwidths.items():
      channelDict = self.channels[channelID]
      channelFiles = channelDict['Files']
      channelSize = channelDict['Size']
      status = channelDict['Status']
      channelName = channelDict['ChannelName']
      channelInfo[channelName] = {'ChannelID': channelID}
  
      if status != 'Active':
        throughputTimeToStart = float('inf') # Make the channel extremely unattractive but still available
        fileTimeToStart = float('inf') #Make the channel extremely unattractive but still available
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
          throughputTimeToStart = foat('inf') # Make the channel extremely unattractive but still available
          fileTimeToStart = float('inf') # Make the channel extremely unattractive but still available
        else:
          if channelFiles > 0:
            fileTimeToStart = channelFiles/(channelFileput+(1/1e100))
          else:
            fileTimeToStart = 0.0
          if channelSize > 0:
            throughputTimeToStart = channelSize/(channelThroughput+(1/1e100))
          else:
            throughputTimeToStart = 0.0

      if self.schedulingType == 'File':
        channelInfo[channelName]['TimeToStart'] = fileTimeToStart
      elif self.schedulingType == 'Throughput':
        channelInfo[channelName]['TimeToStart'] = throughputTimeToStart
      else:
        errStr = 'StrategyHandler.__dynamicThroughput: CS SchedulingType entry must be either File or Throughput'
        gLogger.error(errStr)
        return S_ERROR(errStr)
    return S_OK(channelInfo)


