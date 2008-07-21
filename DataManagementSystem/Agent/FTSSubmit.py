"""  FTS Submit Agent takes files from the TransferDB and submits them to the FTS
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.DataManagementSystem.Client.FTSRequest import FTSRequest
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
import os,time
from types import *

AGENT_NAME = 'DataManagement/FTSSubmit'

class FTSSubmit(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.TransferDB = TransferDB()
    self.filesPerJob = gConfig.getValue(self.section+'/MaxFilesPerJob',50)
    self.maxJobsPerChannel = gConfig.getValue(self.section+'/MaxJobsPerChannel',2)
    self.submissionsPerLoop = gConfig.getValue(self.section+'/SubmissionsPerLoop',1)
    self.DataLog = DataLoggingClient()

    self.useProxies = gConfig.getValue(self.section+'/UseProxies','True').lower() in ( "y", "yes", "true" )
    self.proxyLocation = gConfig.getValue( self.section+'/ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False
    return result

  def execute(self):

    if self.useProxies:
      result = setupShifterProxyInEnv( "DataManager", self.proxyLocation )
      if not result[ 'OK' ]:
        self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
        return result
       
    for i in range(self.submissionsPerLoop):
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting submission loop %s of %s\n\n" % (infoStr,i+1, self.submissionsPerLoop)
      gLogger.info(infoStr)
      res = self.submitTransfer()
    return S_OK()

  def submitTransfer(self):
    """ This method creates and submits FTS jobs based on information it gets from the DB
    """
    # Create the FTSRequest object for preparing the submission
    ftsRequest = FTSRequest()

    #########################################################################
    #  Request the channel to submit to from the TransferDB.
    res = self.TransferDB.selectChannelForSubmission(self.maxJobsPerChannel)
    if not res['OK']:
      errStr = 'FTSAgent.%s' % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    if not res['Value']:
      infoStr = "FTSAgent: No channels eligable for submission."
      gLogger.info(infoStr)
      return S_OK()
    channelDict = res['Value']
    channelID = channelDict['ChannelID']
    sourceSE = channelDict['Source']
    targetSE = channelDict['Destination']
    ftsRequest.setSourceSE(sourceSE)
    ftsRequest.setTargetSE(targetSE)

    #########################################################################
    #  Obtain the first files in the selected channel.
    res = self.TransferDB.getFilesForChannel(channelID,self.filesPerJob)
    if not res['OK']:
      errStr = 'FTSAgent.%s' % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    if not res['Value']:
      infoStr = "FTSAgent: No files to be submitted on Channel %s." % channelID
      gLogger.info(infoStr)
      return S_OK()
    channelDict = res['Value']
    if channelDict.has_key('SpaceToken'):
      spaceToken = channelDict['SpaceToken']
      ftsRequest.setSpaceToken(spaceToken)
    files = channelDict['Files']

    #########################################################################
    #  Populate the FTS Request with the files.
    fileIDs = []
    totalSize = 0
    fileIDSizes = {}
    for file in files:
      lfn = file['LFN']
      ftsRequest.setLFN(lfn)
      ftsRequest.setSourceSURL(lfn,file['SourceSURL'])
      ftsRequest.setDestinationSURL(lfn,file['TargetSURL'])
      fileID = file['FileID']
      fileIDs.append(fileID)
      totalSize += file['Size']
      fileIDSizes[fileID] = file['Size']

    #########################################################################
    #  Submit the FTS request and retrieve the FTS GUID/Server
    res = ftsRequest.submit()
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    ftsGUID = ftsRequest.getFTSGUID()
    ftsServer = ftsRequest.getFTSServer()
    infoStr = "Submitted FTS Job:\n\n\
              FTS Guid:%s\n\
              FTS Server:%s\n\
              ChannelID:%s\n\
              SourceSE:%s\n\
              TargetSE:%s\n\
              Files:%s\n\n" % (ftsGUID.ljust(15),ftsServer.ljust(15),str(channelID).ljust(15),sourceSE.ljust(15),targetSE.ljust(15),str(len(files)).ljust(15))
    gLogger.info(infoStr)

    #########################################################################
    #  Insert the FTS Req details and add the number of files and size
    res = self.TransferDB.insertFTSReq(ftsGUID,ftsServer,channelID)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    ftsReqID = res['Value']
    res = self.TransferDB.setFTSReqAttribute(ftsReqID,'NumberOfFiles',len(fileIDs))
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
    res = self.TransferDB.setFTSReqAttribute(ftsReqID,'TotalSize',totalSize)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)

    #########################################################################
    #  Insert the submission event in the FTSReqLogging table
    event = 'Submitted'
    res = self.TransferDB.addLoggingEvent(ftsReqID,event)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)

    #########################################################################
    #  Insert the FileToFTS details and remove the files from the channel
    res = self.TransferDB.setFTSReqFiles(ftsReqID,fileIDs,channelID)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    for file in files:
      lfn = file['LFN']
      fileID = file['FileID']
      self.DataLog.addFileRecord(lfn,'FTSSubmit',str(ftsReqID),'','FTSSubmitAgent')
      res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,fileID,'FileSize',fileIDSizes[fileID])
      if not res['OK']:
        errStr = "FTSAgent.%s" % res['Message']
        gLogger.error(errStr)
    res = self.TransferDB.setChannelFilesExecuting(channelID,fileIDs)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
