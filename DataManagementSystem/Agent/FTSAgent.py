"""  FTS Agent takes files from the TransferDB and submits them to the FTS
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.Core.FileCatalog.LcgFileCatalogProxyClient import LcgFileCatalogProxyClient
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.DataManagementSystem.Client.FTSRequest import FTSRequest

import time
from types import *

AGENT_NAME = 'DataManagement/FTSAgent'

class FTSAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.TransferDB = TransferDB()
    self.filesPerJob = gConfig.getValue('DataManagement/Agent/FTSAgent/MaxFilesPerJob',50)
    self.maxJobsPerChannel = gConfig.getValue('DataManagement/Agent/FTSAgent/MaxJobsPerChannel',2)
    self.submissionsPerLoop = gConfig.getValue('DataManagement/Agent/FTSAgent/SubmissionsPerLoop',1)
    self.monitorsPerLoop = gConfig.getValue('DataManagement/Agent/FTSAgent/MonitorsPerLoop',1)
    return result

  def execute(self):

    for i in range(self.submissionsPerLoop):
      res = self.submitTransfer()
    for i in range(self.monitorsPerLoop):
      res = self.monitorTransfer()
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
    sourceSE = channelDict['SourceSE']
    targetSE = channelDict['TargetSE']
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
      infoStr = "FTSAgent: No files to be submitted on Channel %s." % ChannelID
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
    for file in files:
      lfn = file['LFN']
      ftsRequest.setLFN(lfn)
      ftsRequest.setSourceSURL(lfn,file['SourceSURL'])
      ftsRequest.setDestinationSURL(lfn,file['TargetSURL'])
      fileIDs.append(file['FileID'])

    #########################################################################
    #  Submit the FTS request and retrieve the FTS GUID/Server
    res = ftsRequest.submit()
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    ftsGUID = ftsRequest.getFTSGUID()
    ftsServer = ftsRequest.getFTSServer()
    infoStr = "FTSAgent: Submitted FTS Job:\n\
              FTS Guid:%s\
              FTS Server:%s\
              ChannelID:%s\
              SourceSE:%s\
              TargetSE:%s\
              Files:%s\n" % (ftsGUID.ljust(15),ftsServer.ljust(15),channelID.ljust(15),sourceSE.ljust(15),targetSE.ljust(15),len(files).ljust(15))
    gLogger.info(infoStr)

    #########################################################################
    #  Insert the FTS Req details and remove the files from the channel.
    res = self.TransferDB.insertFTSReq(ftsGUID,ftsServer,channelID)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    ftsReqID = res['Value']
    res = self.TransferDB.setFTSReqFiles(ftsReqID,fileIDs)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    res = self.TransferDB.removeFilesFromChannel(channelID,fileIDs)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)

  def monitorTransfer(self):
    """ Monitors transfers it obtains from TransferDB
    """
    # Create the FTSRequest object for monitoring
    ftsReq = FTSRequest()

    #########################################################################
    #  Get details for FTS request monitored the most time ago.
    res = self.TransferDB.getFTSReq()
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    if not res['Value']:
      infoStr = "FTSAgent. No FTS requests found to monitor."
      gLogger.info(infoStr)
      return S_OK()
    ftsReqDict = res['Value']
    ftsReqID = ftsReqDict['FTSReqID']
    ftsGUID = ftsReqDict['FTSGuid']
    ftsServer = ftsReqDict['FTSServer']
    ftsReq.setFTSGUID(ftsGUID)
    ftsReq.setFTSServer(ftsServer)

    #########################################################################
    # Get the LFNS associated to the FTS request
    res = self.TransferDB.getFTSReqLFNs(ftsReqID)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    files = res['Value']
    ftsReq.setLFNs(files.keys())

    #########################################################################
    # Perform summary update of the FTS Request and update FTSReq entries.
    res = ftsReq.updateSummary()
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    infoStr = "FTSAgent: Monitoring FTS Job:\n"
    infoStr = "%sFTSGUID:%s\n" % (infoStr,ftsGUID.ljust(20))
    infoStr = "%sFTSServer:%s\n\n" % (infoStr,ftsServer.ljust(20))
    infoStr = "%sRequest Summary:%s\n\n" % (infoStr,ftsReq.getStatusSummary().ljust(20))
    gLogger.info(infoStr)
    percentComplete = ftsReq.getPercentageComplete()
    res = setFTSReqAttribute(ftsReqID,'PercentageComplete',percentComplete)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    res = setFTSReqLastMonitor(ftsReqID)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)

    #########################################################################
    # Update the information in the TransferDB if the transfer is terminal.
    if ftsReq.isRequestTerminal():
      res = test.updateFileStates()
      if not res['OK']:
        errStr = "FTSAgent.%s" % res['Message']
        gLogger.error(errStr)
        return S_ERROR(errStr)
      # Update the failed files status' and fail reasons
      for lfn in ftsReq.getFailed():
        res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Status','Failed')
        res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Reason',ftsReq.getFailReason(lfn))
      # Update the successful files status and transfer time
      for lfn in ftsReq.getCompleted():
        res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Status','Completed')
        res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Duration',ftsReq.getTransferTime(lfn))
      # Now set the FTSReq status to terminal so that it is not monitored again
      res = self.TransferDB.setFTSReqStatus(ftsReqID,'Finished')
    return S_OK()
