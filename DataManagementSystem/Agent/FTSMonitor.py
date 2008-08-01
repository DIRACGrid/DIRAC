"""  FTS Agent takes files from the TransferDB and submits them to the FTS
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.DataManagementSystem.Client.FTSRequest import FTSRequest
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.Core.Utilities import Time
import os,time,re
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

    #########################################################################
    #  Get the details for all active FTS requests
    res = self.TransferDB.getFTSReq()
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    if not res['Value']:
      infoStr = "FTSAgent. No FTS requests found to monitor."
      gLogger.info(infoStr)
      return S_OK()
    ftsReqs = res['Value']

    #######################################################################
    # Check them all....
    i = 1
    for ftsReqDict in ftsReqs:
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting monitoring loop %s of %s\n\n" % (infoStr,i, len(ftsReqs))
      gLogger.info(infoStr)
      res = self.monitorTransfer(ftsReqDict)
      i += 1
    return S_OK()

  def monitorTransfer(self,ftsReqDict):
    """ Monitors transfer  obtained from TransferDB
    """
    # Create the FTSRequest object for monitoring
    ftsReq = FTSRequest()
    ftsReqID = ftsReqDict['FTSReqID']
    ftsGUID = ftsReqDict['FTSGuid']
    ftsServer = ftsReqDict['FTSServer']
    channelID = ftsReqDict['ChannelID']
    submitTime = ftsReqDict['SubmitTime']
    numberOfFiles = ftsReqDict['NumberOfFiles']
    totalSize = ftsReqDict['TotalSize']
    sourceSite = ftsReqDict['Source']
    targetSite = ftsReqDict['Target']
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
      res = self.TransferDB.setFTSReqLastMonitor(ftsReqID)
      if not res['OK']:
        errStr = "FTSAgent.%s" % res['Message']
        gLogger.error(errStr)
      return S_ERROR(errStr)
    infoStr = "Monitoring FTS Job:\n\n"
    infoStr = "%sglite-transfer-status -s %s -l %s\n" % (infoStr,ftsServer,ftsGUID)
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
        failReason = ftsReq.getFailReason(lfn)
        if self.corruptedTarget(failReason):
          gLogger.info('Removing target file with a hack.',ftsReq.getDestinationSURL(lfn))
          res = self.removeTargetSURL(ftsReq.getDestinationSURL(lfn))
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
        self.DataLog.addFileRecord(lfn,'FTSFailed',str(ftsReqID),'','FTSMonitorAgent')
      # Update the successful files status and transfer time
      completedFileIDs = []
      for lfn in ftsReq.getCompleted():
        completedFileIDs.append(files[lfn])
        res = self.TransferDB.updateCompletedChannelStatus(channelID,files[lfn])
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
          failed = True
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
        self.DataLog.addFileRecord(lfn,'FTSDone',str(ftsReqID),'','FTSMonitorAgent')

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
        if not res['OK']:
          errStr = "FTSAgent.%s" % res['Message']
          gLogger.error(errStr)
        else:
          gLogger.info("FTSAgent. preparing accounting message.")
          transferSize = 0
          if completedFileIDs:
            res = self.TransferDB.getSizeOfCompletedFiles(ftsReqID,completedFileIDs)
            if res['OK']:
              transferSize = int(res['Value'])
          oAccounting = self.initialiseAccountingObject(submitTime)
          oAccounting.setValueByKey('TransferOK',len(completedFileIDs))
          oAccounting.setValueByKey('TransferTotal',numberOfFiles)
          oAccounting.setValueByKey('TransferSize',transferSize)
          oAccounting.setValueByKey('FinalStatus',ftsReq.getRequestStatus())
          oAccounting.setValueByKey('Source',sourceSite)
          oAccounting.setValueByKey('Destination',targetSite)
          startTime = submitTime.utcnow()
          endTime = Time.dateTime()
          c = endTime-startTime
          transferTime = c.days * 86400 + c.seconds
          oAccounting.setValueByKey('TransferTime',transferTime)
          oAccounting.commit()
          gLogger.info("FTSAgent. Accounting sent.")
      else:
        infoStr = "FTSAgent.monitor: Updating attributes in FileToFTS table failed for some files. Will monitor again."
        gLogger.info(infoStr)
    return S_OK()

  def removeTargetSURL(self,surl):
    import gfal
    gfalDict = {}
    gfalDict['surls'] = [surl]
    gfalDict['nbfiles'] =  len(gfalDict['surls'])
    #gfalDict['defaultsetype'] = 'srmv2'
    #gfalDict['no_bdii_check'] = 1
    errCode,gfalObject,errMessage = gfal.gfal_init(gfalDict)
    if errCode == 0:
      errCode,gfalObject,errMessage = gfal.gfal_deletesurls(gfalObject)
      if errCode == 0:
        numberOfResults,gfalObject,listOfResults = gfal.gfal_get_results(gfalObject)
        print listOfResults
      else:
        print errMessage
    else:
      print errMessage

  def initialiseAccountingObject(self,submitTime):
    oAccounting = DataOperation()
    oAccounting.setEndTime()
    oAccounting.setStartTime(submitTime)
    accountingDict = {}
    accountingDict['OperationType'] = 'Replicate'
    accountingDict['User'] = 'acsmith'
    accountingDict['Protocol'] = 'FTS'
    accountingDict['RegistrationTime'] = 0.0
    accountingDict['RegistrationOK'] = 0
    accountingDict['RegistrationTotal'] = 0
    oAccounting.setValuesFromDict(accountingDict)
    return oAccounting

  def corruptedTarget(self,failReason):
    corruptionErrors = ['FILE_EXISTS','Device or resource busy','Marking Space as Being Used failed']#,'TRANSFER error during TRANSFER phase']
    for error in corruptionErrors:
      if re.search(error,failReason):
        return 1
    return 0

