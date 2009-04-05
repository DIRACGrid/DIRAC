"""  FTS Monitor takes FTS Requests from the TransferDB and monitors them
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
    gLogger.info('Obtaining requests to monitor')
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
    gLogger.info('Found %s FTS jobs' % len(ftsReqs))

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
    gLogger.info('Obtaining the LFNs associated to this request')
    res = self.TransferDB.getFTSReqLFNs(ftsReqID)
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      return S_ERROR(errStr)
    files = res['Value']
    if not files:
      gLogger.error('No files present for transfer')
      return S_ERROR('No files were found in the DB') 
    ftsReq.setLFNs(files.keys())
    gLogger.info('Obtained %s files' % len(files))

    #########################################################################
    # Perform summary update of the FTS Request and update FTSReq entries.
    gLogger.info('Perform summary update of the FTS Request')
    infoStr = "Monitoring FTS Job:\n\n"
    infoStr = "%sglite-transfer-status -s %s -l %s\n" % (infoStr,ftsServer,ftsGUID)
    infoStr = "%s%s%s\n" % (infoStr,'FTS GUID:'.ljust(20),ftsGUID)
    infoStr = "%s%s%s\n\n" % (infoStr,'FTS Server:'.ljust(20),ftsServer)
    gLogger.info(infoStr)
    res = ftsReq.updateSummary()
    if not res['OK']:
      errStr = "FTSAgent.%s" % res['Message']
      gLogger.error(errStr)
      res = self.TransferDB.setFTSReqLastMonitor(ftsReqID)
      if not res['OK']:
        errStr = "FTSAgent.%s" % res['Message']
        gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("%s%s\n\n" % ('Request Summary:'.ljust(20),ftsReq.getStatusSummary()))
    percentComplete = ftsReq.getPercentageComplete()
    gLogger.info('FTS Request found to be %s percent complete' % int(percentComplete))
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
      gLogger.info('FTS Request found to be terminal, updating file states')
      res = ftsReq.updateFileStates()
      if not res['OK']:
        errStr = "FTSAgent.%s" % res['Message']
        gLogger.error(errStr)
        return S_ERROR(errStr)

      # Update the entries in the data logging for successful and failed files
      gLogger.info('Updating data logging entries for files')
      failedLFNs = ftsReq.getFailed()
      if failedLFNs:
        self.DataLog.addFileRecord(failedLFNs,'FTSFailed',str(ftsReqID),'','FTSMonitorAgent')
      completedLFNs = ftsReq.getCompleted()
      if completedLFNs:
        self.DataLog.addFileRecord(completedLFNs,'FTSDone',str(ftsReqID),'','FTSMonitorAgent')

      targetsToRemove = []
      filesToReschedule = []
      filesToRetry = []
      fileToFTSUpdates = []
      completedFileIDs = []
      gLogger.info('Obtaining file information for failed files')
      for lfn in failedLFNs:
        fileID = files[lfn]
        failReason = ftsReq.getFailReason(lfn)
        gLogger.error('Failed to replicate file on channel.', "%s %s" % (channelID,failReason))  
        if self.corruptedTarget(failReason):
          targetsToRemove.append(ftsReq.getDestinationSURL(lfn))
        if self.missingSource(failReason):
          # TODO: Create an integrity DB entry and reschedule the file
          gLogger.error('The source SURL does not exist.', '%s %s' % (lfn,ftsReq.getSourceSURL(lfn)))
          filesToReschedule.append(fileID)
        else:
          filesToRetry.append(fileID)
        fileToFTSUpdates.append((fileID,'Failed',ftsReq.getFailReason(lfn),ftsReq.getRetries(lfn),0))
      gLogger.info('Obtaining file information for successful files') 
      for lfn in completedLFNs:
        fileID = files[lfn]
        completedFileIDs.append(fileID)
        fileToFTSUpdates.append((fileID,'Completed',ftsReq.getFailReason(lfn),ftsReq.getRetries(lfn),ftsReq.getTransferTime(lfn)))

      if filesToReschedule:
        gLogger.info('Updating the Channel table for files to reschedule')
        res = self.TransferDB.setFileChannelStatus(channelID,filesToReschedule,'Failed')
        if not res['OK']:
          gLogger.error('Failed to update Channel table for failed files.', res['Message'])

      if filesToRetry:
        gLogger.info('Updating the Channel table for files to retry')
        res = self.TransferDB.resetFileChannelStatus(channelID,filesToRetry)
        if not res['OK']:
          gLogger.error('Failed to update the Channel table for file to retry.', res['Message'])

      if completedFileIDs:
        gLogger.info('Updating the Channel table for successful files')
        res = self.TransferDB.updateCompletedChannelStatus(channelID,completedFileIDs)
        if not res['OK']:
          gLogger.error('Failed to update the Channel table for successful files.', res['Message'])

        gLogger.info('Updating the Channel table for ancestors of successful files')
        res = self.TransferDB.updateAncestorChannelStatus(channelID,completedFileIDs)
        if not res['OK']:
          gLogger.error('Failed to update the Channel table for ancestors of successful files.', res['Message'])

        gLogger.info('Updating the FileToCat table for successful files')
        res = self.TransferDB.setRegistrationWaiting(channelID,completedFileIDs)
        if not res['OK']:
          gLogger.error('Failed to update the FileToCat table for successful files.', res['Message'])
             
      gLogger.info('Updating the FileToFTS table for files')
      res = self.TransferDB.setFileToFTSFileAttributes(ftsReqID,channelID,fileToFTSUpdates)
      if not res['OK']:
        gLogger.error('Failed to update the FileToFTS table for files.', res['Message'])

      gLogger.info('Adding logging event for FTS request')
      # Now set the FTSReq status to terminal so that it is not monitored again
      res = self.TransferDB.addLoggingEvent(ftsReqID,'Finished')
      if not res['OK']:
        gLogger.error('Failed to add logging event for FTS Request', res['Message'])

      gLogger.info('Updating FTS request status')
      res = self.TransferDB.setFTSReqStatus(ftsReqID,'Finished')
      if not res['OK']:
        gLogger.error('Failed update FTS Request status', res['Message'])
      else:
        gLogger.info("FTSAgent. preparing accounting message.")
        transferSize = 0
        if completedFileIDs:
          gLogger.info("FTSAgent. getting the size of the completed files.")
          res = self.TransferDB.getSizeOfCompletedFiles(ftsReqID,completedFileIDs)
          if res['OK']:
            transferSize = int(res['Value'])
        gLogger.debug("FTSAgent. transfer size of the completed files: %s." % transferSize)

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
        gLogger.info("FTSAgent. accounting message prepared. sending....")
        oAccounting.commit()
        gLogger.info("FTSAgent. Accounting sent.")
      
      if targetsToRemove:
        gLogger.info('Removing problematic target files')
        self.removeTargetSURL(targetsToRemove)

    return S_OK()

  def removeTargetSURL(self,surls):
    import gfal
    gfalDict = {}
    gfalDict['surls'] = surls
    gfalDict['nbfiles'] =  len(gfalDict['surls'])
    #gfalDict['defaultsetype'] = 'srmv2'
    #gfalDict['no_bdii_check'] = 1
    errCode,gfalObject,errMessage = gfal.gfal_init(gfalDict)
    if errCode == 0:
      errCode,gfalObject,errMessage = gfal.gfal_deletesurls(gfalObject)
      if errCode == 0:
        numberOfResults,gfalObject,listOfResults = gfal.gfal_get_results(gfalObject)
        for result in listOfResults:
          print result
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
    corruptionErrors = ['file exists','FILE_EXISTS','Device or resource busy','Marking Space as Being Used failed']#,'TRANSFER error during TRANSFER phase']
    for error in corruptionErrors:
      if re.search(error,failReason):
        return 1
    return 0

  def missingSource(self,failReason):
    missingSourceErrors = ['SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] Failed','SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] No such file or directory','SOURCE error during PREPARATION phase: \[INVALID_PATH\] Failed','SOURCE error during PREPARATION phase: \[INVALID_PATH\] The requested file either does not exist','TRANSFER error during TRANSFER phase: \[INVALID_PATH\] the server sent an error response: 500 500 Command failed. : open error: No such file or directory']
    for error in missingSourceErrors:
      if re.search(error,failReason):
        return 1
    return 0
