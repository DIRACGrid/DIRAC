"""  Replication Scheduler assigns replication requests to channels
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
from DIRAC.Core.Storage.StorageFactory import StorageFactory
import types,re

AGENT_NAME = 'DataManagement/ReplicationScheduler'

class ReplicationScheduler(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDB = RequestDB('mysql')
    self.TransferDB = TransferDB()
    self.factory = StorageFactory()
    try:
      self.lfc = LcgFileCatalogCombinedClient() #infosys=infosysUrl,host=serverUrl)
      self.throughputTimescale = gConfig.getValue(self.section+'/ThroughputTimescale',3600) 
      self.activeStrategies = gConfig.getValue(self.section+'/ActiveStrategies',['Simple'])
      self.failureRate = gConfig.getValue(self.section+'/AcceptableFailureRate',75)
    except Exception,x:
      print "Failed to create LcgFileCatalogClient"
      print str(x)
    return result

  def execute(self):
    """ The main agent execution method
    """

    ######################################################################################
    #
    #  Obtain information on the current state of the channel queues
    #

    res = self.TransferDB.getActiveChannelQueues()
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to get channel queues from TransferDB: %s.' % res['Message']
      self.log.error(errStr)
      return S_ERROR(errStr)
    if not res['Value']:
      infoStr = 'ReplicationScheduler._execute: No active channels found for replication.'
      self.log.info(infoStr)
      return S_OK()
    channelQueues = res['Value']['ChannelQueues']
    channelSites = res['Value']['ChannelSites']

    res = self.TransferDB.getActiveChannelObservedThroughput(self.throughputTimescale)
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to get observed throughput from TransferDB: %s.' % res['Message']
      self.log.error(errStr)
      return S_ERROR(errStr)
    if not res['Value']:
      infoStr = 'ReplicationScheduler._execute: No active channels found for replication.'
      self.log.info(infoStr)
      return S_OK()
    observedThroughput = res['Value']

    self.strategyHandler = StrategyHandler(observedThroughput,channelQueues,channelSites,self.activeStrategies,self.failureRate)
 
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
          ancestor = dict['Ancestor']
          status = 'Waiting'
          if ancestor:
            status = '%s%s' % (status,ancestor)
          sourceSite = dict['SourceSite']
          destSite = dict['DestSite']
          sourceSURL = lfnReps[sourceSE]
          res = self.obtainTargetSURL(destSite,lfn)
          if not res['OK']:
            errStr = res['Message']
            gLogger.error(errStr)
            return S_ERROR(errStr)
          targetSURL = res['Value']

          ######################################################################################
          #
          # For each item in the replication tree add the file to the channel
          #

          res = self.TransferDB.addFileToChannel(channelID, fileID, sourceSURL, targetSURL,fileSize,spaceToken,fileStatus=status)
          if not res['OK']:
            errStr = "ReplicationScheduler._execute: Failed to add File %s to Channel %s." % (fileID,channelID)
            gLogger.error(errStr)
            return S_ERROR(errStr)
    return res


  def obtainTargetSURL(self,targetSE,lfn):
    """ Creates the targetSURL for the storage and LFN supplied
    """
    res = self.factory.getStorages(targetSE)
    if not res['OK']:
      errStr = 'ReplicationScheduler._execute: Failed to create SRM2 storage for %s: %s. ' % (targetSE,res['Message'])
      self.log.error(errStr)
      return S_ERROR(errStr)
    storageObjects = res['Value']['StorageObjects']
    targetSURL = ''
    for storageObject in storageObjects:
      if storageObject.getProtocol() == 'SRM2':
        res = storageObject.getUrl(lfn)
        if not res['OK']:
          errStr = 'ReplicationScheduler._execute: Failed to get target SURL: %s.' % res['Message']
          self.log.error(errStr)
          return S_ERROR(errStr)
        targetSURL = res['Value']
    if not targetSURL:
      errStr = 'ReplicationScheduler._execute: Failed to get SRM compliant storage for %s.' % targetSE
      self.log.error(errStr)
      return S_ERROR(errStr)
    return S_OK(targetSURL)

class StrategyHandler:

  def __init__(self,bandwidths,queues,channelsites,activeStrategies,failureRate):
    """ Standard constructor
    """
    self.chosenStrategy = 0
    self.activeStrategies = activeStrategies 
    self.numberOfStrategies = len(self.activeStrategies)
    self.bandwidths = bandwidths
    self.queues = queues
    self.channelSites = channelsites
    self.acceptableFailureRate = failureRate
              	
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
      self.queues[channelID]['Files'] += 1
      self.queues[channelID]['Size'] += size
    return S_OK(tree)   

  def __simple(self,sourceSE,targetSEs):
    """ This just does a simple replication from the source to all the targets 
    """
    tree = {}
    for targetSE in targetSEs:
      for channelID,dict in self.channelSites.items():
        if dict['SourceSite'] == sourceSE and dict['DestinationSite'] == targetSE:
          tree[channelID] = {}
          tree[channelID]['Ancestor'] = False
          tree[channelID]['SourceSite'] = sourceSE
          tree[channelID]['DestSite'] = targetSE           
    return tree     

  def __dynamicThroughput(self,sourceSEs,targetSEs):
    """ 
    """ 
    for channelID,value in self.bandwidths.items():
      channelThroughput = value['Throughput']
      channelFileput = value['Fileput']
      channelFileSuccess = value['SuccessfulFiles']
      channelFileFailed = value['FailedFiles']
      attempted = channelFileSuccess+channelFileFailed
      if attempted != 0:
        successRate = 100.0*(channelFileSuccess/float(attempted))
      else:
        successRate = 0.0
      if successRate < self.acceptableFailureRate:
        pass
      channelFiles = self.queues[channelID]['Files']
      channelSize = self.queues[channelID]['Size'] 
      fileTimeToStart = channelFiles/channelFileput
      throughputTimeToStart = channelSize/channelThroughput
      
      print fileTimeToStart
      print throughputTimeToStart
    

