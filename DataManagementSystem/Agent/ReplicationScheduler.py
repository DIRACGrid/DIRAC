"""  Replication Scheduler assigns replication requests to channels
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.LcgFileCatalogClient import LcgFileCatalogClient
from DIRAC.Core.Storage.StorageElement import StorageElement

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
    try:
      serverUrl = gConfig.getValue('/DataManagement/FileCatalogs/LFC/LFCMaster')
      infosysUrl = gConfig.getValue('/DataManagement/FileCatalogs/LFC/LcgGfalInfosys')
      self.lfc = LcgFileCatalogClient(infosys=infosysUrl,host=serverUrl)
    except Exception,x:
      print "Failed to create LcgFileCatalogClient"
      print str(x)
    return result

  def execute(self):
    """ The main agent execution method
    """

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
    #  The important request attributes are the source and target SEs.

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
      operation = attributes['Operation']
      spaceToken = attributes['SpaceToken']

      ######################################################################################
      # Then obtain the file attribute of interest are the  LFN and FileID

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
      res = self.lfc.getPfnsByLfnList(lfns)
      if not res['OK']:
        errStr = 'ReplicationScheduler._execute: Failed to get replica infomation: %s.' % res['Message']
        self.log.error(errStr)
        return S_ERROR(errStr)
      replicas = res['Value']

      ######################################################################################
      # Take the sourceSURL from the catalog entries and contruct the target SURL from the CS

      for lfn in filesDict.keys():
        lfnReps = replicas[lfn]
        fileID = filesDict[lfn]
        #res = self.determineReplicationTree(lfnReps, sourceSE, targetSE)
        if lfnReps.has_key(sourceSE):
          sourceSURL = lfnReps[sourceSE]
          targetStorage = StorageElement(targetSE)
          res = targetStorage.getPfnForLfnForProtocol(lfn,'SRM2')
          if not res['OK']:
            errStr = 'ReplicationScheduler._execute: Failed to get target SURL: %s.' % res['Message']
            self.log.error(errStr)
            return S_ERROR(errStr)
          targetSURL = res['Value']

          ######################################################################################
          # Obtain the ChannelID and insert the file into that channel (done at the file level to allow file by file scheduling)

          res = self.TransferDB.getChannelID(sourceSE,targetSE)
          if not res['OK']:
            logStr = 'ReplicationScheduler._execute: Creating channel from %s to %s.' % (sourceSE,targetSE)
            self.log.info(logStr)
            res = self.TransferDB.createChannel(sourceSE, targetSE)
            channelID = res['Value']['ChannelID']
            logStr = 'ReplicationScheduler._execute: ChannelID = %s.' % (channelID)
            self.log.info(logStr)
          else:
            channelID = res['Value']
          res = self.TransferDB.addFileToChannel(channelID, fileID, sourceSURL, targetSURL,spaceToken)
          if not res['OK']:
            errStr = "ReplicationScheduler._execute: Failed to add File %s to Channel %s." % (fileID,channelID)
            gLogger.error(errStr)
            return S_ERROR(errStr)
        else:
          errStr = "ReplicationScheduler._execute: %s does not have a replica at %s." % (lfn,sourceSE)
          gLogger.error(errStr)
    return res

  def determineReplicationTree(self,replicas,sourceSE,targetSE):
    # This is where the intelligence is supposed to go. At the moment we just do source->target.
    # The dictionary stores the replication tree. It is done with the heirarchy of the dictionary.
    # The first level i.e. dict[ind] are transfers from currently existing sources.
    # The second level dict[ind][ind] ar dependant on the transfers in the first etc.
    dict = {}
    dict[0] = {'Source':sourceSE,'Target':targetSE,'Dependants':None}
    return S_OK(dict)
