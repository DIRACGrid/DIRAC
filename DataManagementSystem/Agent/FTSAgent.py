"""  FTS Agent takes files from the TransferDB and submits them to the FTS
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
#from DIRAC.Core.FileCatalog.LcgFileCatalogProxyClient import LcgFileCatalogProxyClient
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
    self.filesPerJob = gConfig.getValue(self.section+'/MaxFilesPerJob',50)
    self.maxJobsPerChannel = gConfig.getValue(self.section+'/MaxJobsPerChannel',2)
    self.submissionsPerLoop = gConfig.getValue(self.section+'/SubmissionsPerLoop',1)
    self.monitorsPerLoop = gConfig.getValue(self.section+'/MonitorsPerLoop',1)
    return result

  def execute(self):

    for i in range(self.submissionsPerLoop):
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting submission loop %s of %s\n\n" % (infoStr,i+1, self.submissionsPerLoop)
      gLogger.info(infoStr)
      res = self.submitTransfer()
    for i in range(self.monitorsPerLoop):
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting monitoring loop %s of %s\n\n" % (infoStr,i+1, self.monitorsPerLoop)
      gLogger.info(infoStr)
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
    for fileID in fileIDs:
      res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,fileID,'FileSize',fileIDSizes[fileID])
      if not res['OK']:
        errStr = "FTSAgent.%s" % res['Message']
        gLogger.error(errStr)
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
    infoStr = "Monitoring FTS Job:\n\n"
    infoStr = "%s%s%s\n" % (infoStr,'FTS GUID:'.ljust(20),ftsGUID)
    infoStr = "%s%s%s\n\n" % (infoStr,'FTS Server:'.ljust(20),ftsServer)
    infoStr = "%s%s%s\n\n" % (infoStr,'Request Summary:'.ljust(20),ftsReq.getStatusSummary())
    gLogger.info(infoStr)
    percentComplete = ftsReq.getPercentageComplete()
    res = self.TransferDB.setFTSReqAttribute(ftsReqID,'PercentageComplete',percentComplete)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
    res = self.TransferDB.addLoggingEvent(ftsReqID,percentComplete)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
    res = self.TransferDB.setFTSReqLastMonitor(ftsReqID)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)

    #########################################################################
    # Update the information in the TransferDB if the transfer is terminal.
    if ftsReq.isRequestTerminal():
      res = ftsReq.updateFileStates()
      if not res['OK']:
        errStr = "FTSAgent.%s" % res['Message']
        gLogger.error(errStr)
        return S_ERROR(errStr)
      # Update the failed files status' and fail reasons
      failed = False
      for lfn in ftsReq.getFailed():
        res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Status','Failed')
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
        res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Reason',ftsReq.getFailReason(lfn))
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
        res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Retries',ftsReq.getRetries(lfn))
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
        res = self.TransferDB.setFileToFTSTerminalTime(ftsReqID,files[lfn])
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
      # Update the successful files status and transfer time
      for lfn in ftsReq.getCompleted():
        res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Status','Completed')
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
        res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Duration',ftsReq.getTransferTime(lfn))
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
        retries = ftsReq.getRetries(lfn)
        res = self.TransferDB.setFileToFTSTerminalTime(ftsReqID,files[lfn])
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
        if retries:
          res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Reason',ftsReq.getFailReason(lfn))
          if not res['OK']:
            errStr = "FTSAgent.%s" % res['Message']
            gLogger.error(errStr)
            failed = True
          res = self.TransferDB.setFileToFTSFileAttribute(ftsReqID,files[lfn],'Retries',ftsReq.getRetries(lfn))
          if not res['OK']:
            errStr = "FTSAgent.%s" % res['Message']
            gLogger.error(errStr)
            failed = True

      # Now set the FTSReq status to terminal so that it is not monitored again
      if not failed:
        res = self.TransferDB.addLoggingEvent(ftsReqID,'Finished')
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
        res = self.TransferDB.setFTSReqStatus(ftsReqID,'Finished')
      else:  
        infoStr = "FTSAgent.monitor: Updating attributes in FileToFTS table failed for some files. Will monitor again."
        gLogger.info(infoStr) 
    return S_OK()
