"""  FTS Agent takes files from the TransferDB and submits them to the FTS
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.DataManagementSystem.Client.FTSRequest import FTSRequest
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.GridCredentials import setupProxy,setDIRACGroup, getProxyTimeLeft
import os,time
from types import *


AGENT_NAME = 'DataManagement/FTSMonitor'

class FTSMonitor(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.TransferDB = TransferDB()
    self.monitorsPerLoop = gConfig.getValue(self.section+'/MonitorsPerLoop',1)
    self.DataLog = RPCClient('DataManagement/DataLogging')

    self.useProxies = gConfig.getValue(self.section+'/UseProxies','True')
    if self.useProxies == 'True':
      self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
      self.proxyDN = gConfig.getValue(self.section+'/ProxyDN','')
      self.proxyGroup = gConfig.getValue(self.section+'/ProxyGroup','')
      self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',12)
      self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','')
      if os.path.exists(self.proxyLocation):
        os.remove(self.proxyLocation)
    return result

  def execute(self):

    if self.useProxies == 'True':
      ############################################################
      #
      # Get a valid proxy for the current activity
      #
      self.log.info("TransferAgent.execute: Determining the length of the %s proxy." %self.proxyDN)
      obtainProxy = False
      if not os.path.exists(self.proxyLocation):
        self.log.info("TransferAgent.execute: No proxy found.")
        obtainProxy = True
      else:
        currentProxy = open(self.proxyLocation,'r')
        oldProxyStr = currentProxy.read()
        res = getProxyTimeLeft(oldProxyStr)
        if not res["OK"]:
          gLogger.error("TransferAgent.execute: Could not determine the time left for proxy.", res['Message'])
          return S_OK()
        proxyValidity = int(res['Value'])
        gLogger.debug("TransferAgent.execute: Current proxy found to be valid for %s seconds." % proxyValidity)
        self.log.info("TransferAgent.execute: %s proxy found to be valid for %s seconds."% (self.proxyDN,proxyValidity))
        if proxyValidity <= 60:
          obtainProxy = True

      if obtainProxy:
        self.log.info("TransferAgent.execute: Attempting to renew %s proxy." %self.proxyDN)
        res = self.wmsAdmin.getProxy(self.proxyDN,self.proxyGroup,self.proxyLength)
        if not res['OK']:
          gLogger.error("TransferAgent.execute: Could not retrieve proxy from WMS Administrator", res['Message'])
          return S_OK()
        proxyStr = res['Value']
        if not os.path.exists(os.path.dirname(self.proxyLocation)):
          os.makedirs(os.path.dirname(self.proxyLocation))
        res = setupProxy(proxyStr,self.proxyLocation)
        if not res['OK']:
          gLogger.error("TransferAgent.execute: Could not create environment for proxy.", res['Message'])
          return S_OK()
        setDIRACGroup(self.proxyGroup)
        self.log.info("TransferAgent.execute: Successfully renewed %s proxy." %self.proxyDN)

    for i in range(self.monitorsPerLoop):
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting monitoring loop %s of %s\n\n" % (infoStr,i+1, self.monitorsPerLoop)
      gLogger.info(infoStr)
      res = self.monitorTransfer()
    return S_OK()

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
    channelID = ftsReqDict['ChannelID']
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
        res = self.TransferDB.resetFileChannelStatus(channelID,files[lfn])
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
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
        print self.DataLog.addFileRecord(lfn,'FTSFailed',str(ftsReqID),'','FTSMonitorAgent')
      # Update the successful files status and transfer time
      completedFileIDs = []
      for lfn in ftsReq.getCompleted():
        completedFileIDs.append(files[lfn])
        res = self.TransferDB.setRegistrationWaiting(channelID,files[lfn])
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
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
        print self.DataLog.addFileRecord(lfn,'FTSDone',str(ftsReqID),'','FTSMonitorAgent')

      # Update the status of the files waiting for the completion of this transfer
      res = self.TransferDB.updateAncestorChannelStatus(channelID,completedFileIDs)
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
