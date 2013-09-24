########################################################################
# $HeadURL$
########################################################################
"""
  :mod: FTSMonitorAgent
  =====================

  The FTSMonitorAgent takes FTS Requests from the TransferDB and monitors their execution
  using FTSRequest helper class.

  :deprecated:
"""
# # imports
import time
import re
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.DataManagementSystem.Client.FTSRequest import FTSRequest
from DIRAC.Core.Utilities.ThreadPool import ThreadPool

# # RCSID
__RCSID__ = "$Id$"
# # agent's name

AGENT_NAME = 'DataManagement/FTSMonitorAgent'

class FTSMonitorAgent( AgentModule ):
  """
  .. class:: FTSMonitorAgent

  Monitor submitted FTS jobs.
  """
  # # transfer DB handle
  transferDB = None
  # # thread pool
  threadPool = None
  # # min threads
  minThreads = 1
  # # max threads
  maxThreads = 10

  # # missing source regexp patterns
  missingSourceErrors = [
    re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] Failed" ),
    re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] No such file or directory" ),
    re.compile( r"SOURCE error during PREPARATION phase: \[INVALID_PATH\] Failed" ),
    re.compile( r"SOURCE error during PREPARATION phase: \[INVALID_PATH\] The requested file either does not exist" ),
    re.compile( r"TRANSFER error during TRANSFER phase: \[INVALID_PATH\] the server sent an error response: 500 500"\
               " Command failed. : open error: No such file or directory" ),
    re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[USER_ERROR\] source file doesnt exist" ) ]

  def initialize( self ):
    """ agent's initialisation """
    self.transferDB = TransferDB()
    self.am_setOption( "shifterProxy", "DataManager" )
    self.minThreads = self.am_getOption( "MinThreads", self.minThreads )
    self.maxThreads = self.am_getOption( "MaxThreads", self.maxThreads )
    minmax = ( abs( self.minThreads ), abs( self.maxThreads ) )
    self.minThreads, self.maxThreads = min( minmax ), max( minmax )
    self.log.info( "ThreadPool min threads = %s" % self.minThreads )
    self.log.info( "ThreadPool max threads = %s" % self.maxThreads )
    self.threadPool = ThreadPool( self.minThreads, self.maxThreads )
    self.threadPool.daemonize()
    return S_OK()

  def execute( self ):
    """ push jobs to the thread pool """
    self.log.info( "Obtaining requests to monitor" )
    res = self.transferDB.getFTSReq()
    if not res["OK"]:
      self.log.error( "Failed to get FTS requests", res['Message'] )
      return res
    if not res["Value"]:
      self.log.info( "No FTS requests found to monitor." )
      return S_OK()
    ftsReqs = res["Value"]
    self.log.info( "Found %s FTS jobs" % len( ftsReqs ) )
    i = 1
    for ftsJob in ftsReqs:
      while True:
        self.log.debug( "submitting FTS Job %s FTSReqID=%s to monitor" % ( i, ftsJob["FTSReqID"] ) )
        ret = self.threadPool.generateJobAndQueueIt( self.monitorTransfer, args = ( ftsJob, ), )
        if ret["OK"]:
          i += 1
          break
        # # sleep 1 second to proceed
        time.sleep( 1 )

    self.threadPool.processAllResults()
    return S_OK()

  def ftsJobExpired( self, ftsReqID, channelID ):
    """ clean up when FTS job had expired on the server side

    :param int ftsReqID: FTSReq.FTSReqID
    :param int channelID: FTSReq.ChannelID
    """
    log = gLogger.getSubLogger( "@%s" % str( ftsReqID ) )
    fileIDs = self.transferDB.getFTSReqFileIDs( ftsReqID )
    if not fileIDs["OK"]:
      log.error( "Unable to retrieve FileIDs associated to %s request" % ftsReqID )
      return fileIDs
    fileIDs = fileIDs["Value"]

    # # update FileToFTS table, this is just a clean up, no worry if somethings goes wrong
    for fileID in fileIDs:
      fileStatus = self.transferDB.setFileToFTSFileAttribute( ftsReqID, fileID,
                                                              "Status", "Failed" )
      if not fileStatus["OK"]:
        log.error( "Unable to set FileToFTS status to 'Failed' for FileID %s: %s" % ( fileID,
                                                                                     fileStatus["Message"] ) )

      failReason = self.transferDB.setFileToFTSFileAttribute( ftsReqID, fileID,
                                                              "Reason", "FTS job expired on server" )
      if not failReason["OK"]:
        log.error( "Unable to set FileToFTS reason for FileID %s: %s" % ( fileID,
                                                                         failReason["Message"] ) )
    # # update Channel table
    resetChannels = self.transferDB.resetFileChannelStatus( channelID, fileIDs )
    if not resetChannels["OK"]:
      log.error( "Failed to reset Channel table for files to retry" )
      return resetChannels

    # # update FTSReq table
    log.info( "Setting FTS request status to 'Finished'" )
    ftsReqStatus = self.transferDB.setFTSReqStatus( ftsReqID, "Finished" )
    if not ftsReqStatus["OK"]:
      log.error( "Failed update FTS Request status", ftsReqStatus["Message"] )
      return ftsReqStatus

    # # if we land here, everything should be OK
    return S_OK()

  def monitorTransfer( self, ftsReqDict ):
    """ monitors transfer obtained from TransferDB

    :param dict ftsReqDict: FTS job dictionary
    """
    ftsReqID = ftsReqDict.get( "FTSReqID" )
    ftsGUID = ftsReqDict.get( "FTSGuid" )
    ftsServer = ftsReqDict.get( "FTSServer" )
    channelID = ftsReqDict.get( "ChannelID" )
    sourceSE = ftsReqDict.get( "SourceSE" )
    targetSE = ftsReqDict.get( "TargetSE" )

    oFTSRequest = FTSRequest()
    oFTSRequest.setFTSServer( ftsServer )
    oFTSRequest.setFTSGUID( ftsGUID )
    oFTSRequest.setSourceSE( sourceSE )
    oFTSRequest.setTargetSE( targetSE )

    log = gLogger.getSubLogger( "@%s" % str( ftsReqID ) )

    #########################################################################
    # Perform summary update of the FTS Request and update FTSReq entries.
    log.info( "Perform summary update of the FTS Request" )
    infoStr = [ "glite-transfer-status -s %s -l %s" % ( ftsServer, ftsGUID ) ]
    infoStr.append( "FTS GUID:   %s" % ftsGUID )
    infoStr.append( "FTS Server: %s" % ftsServer )
    log.info( "\n".join( infoStr ) )
    res = oFTSRequest.summary()
    self.transferDB.setFTSReqLastMonitor( ftsReqID )
    if not res["OK"]:
      log.error( "Failed to update the FTS request summary", res["Message"] )
      if "getTransferJobSummary2: Not authorised to query request" in res["Message"]:
        log.error( "FTS job is not existing at the FTS server anymore, will clean it up on TransferDB side" )
        cleanUp = self.ftsJobExpired( ftsReqID, channelID )
        if not cleanUp["OK"]:
          log.error( cleanUp["Message"] )
        return cleanUp
      return res

    res = oFTSRequest.dumpSummary()
    if not res['OK']:
      log.error( "Failed to get FTS request summary", res["Message"] )
      return res
    log.info( res['Value'] )
    res = oFTSRequest.getPercentageComplete()
    if not res['OK']:
      log.error( "Failed to get FTS percentage complete", res["Message"] )
      return res
    log.info( 'FTS Request found to be %.1f percent complete' % res["Value"] )
    self.transferDB.setFTSReqAttribute( ftsReqID, "PercentageComplete", res["Value"] )
    self.transferDB.addLoggingEvent( ftsReqID, res["Value"] )

    #########################################################################
    # Update the information in the TransferDB if the transfer is terminal.
    res = oFTSRequest.isRequestTerminal()
    if not res["OK"]:
      log.error( "Failed to determine whether FTS request terminal", res["Message"] )
      return res
    if not res["Value"]:
      return S_OK()
    # # request is terminal
    return self.terminalRequest( oFTSRequest, ftsReqID, channelID, sourceSE )

  def terminalRequest( self, oFTSRequest, ftsReqID, channelID, sourceSE ):
    """ process terminal FTS job

    :param FTSRequest oFTSRequest: FTSRequest instance
    :param int ftsReqID: FTSReq.FTSReqID
    :param int channelID: FTSReq.ChannelID
    :param str sourceSE: FTSReq.SourceSE
    """
    log = gLogger.getSubLogger( "@%s" % ftsReqID )

    log.info( "FTS Request found to be terminal, updating file states" )
    #########################################################################
    # Get the LFNS associated to the FTS request
    log.info( "Obtaining the LFNs associated to this request" )
    res = self.transferDB.getFTSReqLFNs( ftsReqID, channelID, sourceSE )
    if not res["OK"]:
      log.error( "Failed to obtain FTS request LFNs", res['Message'] )
      return res
    files = res["Value"]
    if not files:
      log.error( "No files present for transfer" )
      return S_ERROR( "No files were found in the DB" )

    lfns = files.keys()
    log.debug( "Obtained %s files" % len( lfns ) )
    for lfn in lfns:
      oFTSRequest.setLFN( lfn )

    res = oFTSRequest.monitor()
    if not res["OK"]:
      log.error( "Failed to perform detailed monitoring of FTS request", res["Message"] )
      return res
    res = oFTSRequest.getFailed()
    if not res["OK"]:
      log.error( "Failed to obtained failed files for FTS request", res["Message"] )
      return res
    failedFiles = res["Value"]
    res = oFTSRequest.getDone()
    if not res["OK"]:
      log.error( "Failed to obtained successful files for FTS request", res["Message"] )
      return res
    completedFiles = res["Value"]

    # An LFN can be included more than once if it was entered into more than one Request.
    # FTS will only do the transfer once. We need to identify all FileIDs
    res = self.transferDB.getFTSReqFileIDs( ftsReqID )
    if not res["OK"]:
      log.error( "Failed to get FileIDs associated to FTS Request", res["Message"] )
      return res
    fileIDs = res["Value"]
    res = self.transferDB.getAttributesForFilesList( fileIDs, ["LFN"] )
    if not res["OK"]:
      log.error( "Failed to get LFNs associated to FTS Request", res["Message"] )
      return res
    fileIDDict = res["Value"]

    fileToFTSUpdates = []
    completedFileIDs = []
    filesToRetry = []
    filesToFail = []

    for fileID, fileDict in fileIDDict.items():
      lfn = fileDict['LFN']
      if lfn in completedFiles:
        completedFileIDs.append( fileID )
        transferTime = 0
        res = oFTSRequest.getTransferTime( lfn )
        if res["OK"]:
          transferTime = res["Value"]
        fileToFTSUpdates.append( ( fileID, "Completed", "", 0, transferTime ) )

      if lfn in failedFiles:
        failReason = ""
        res = oFTSRequest.getFailReason( lfn )
        if res["OK"]:
          failReason = res["Value"]
        if "Source file/user checksum mismatch" in failReason:
          filesToFail.append( fileID )
          continue
        if self.missingSource( failReason ):
          log.error( "The source SURL does not exist.", "%s %s" % ( lfn, oFTSRequest.getSourceSURL( lfn ) ) )
          filesToFail.append( fileID )
        else:
          filesToRetry.append( fileID )
        log.error( "Failed to replicate file on channel.", "%s %s" % ( channelID, failReason ) )
        fileToFTSUpdates.append( ( fileID, "Failed", failReason, 0, 0 ) )

    # # update TransferDB.FileToFTS table
    updateFileToFTS = self.updateFileToFTS( ftsReqID, channelID,
                                            filesToRetry, filesToFail,
                                            completedFileIDs, fileToFTSUpdates )

    if updateFileToFTS["OK"] and updateFileToFTS["Value"]:
      res = oFTSRequest.finalize()
      if not res["OK"]:
        log.error( "Failed to perform the finalization for the FTS request", res["Message"] )
        return res

      log.info( 'Adding logging event for FTS request' )
      # Now set the FTSReq status to terminal so that it is not monitored again
      res = self.transferDB.addLoggingEvent( ftsReqID, 'Finished' )
      if not res['OK']:
        log.error( 'Failed to add logging event for FTS Request', res['Message'] )

      # update TransferDB.FileToCat table
      updateFileToCat = self.updateFileToCat( oFTSRequest, channelID, fileIDDict, completedFiles, filesToFail )
      if not updateFileToCat["OK"]:
        log.error( updateFileToCat["Message"] )

      log.debug( "Updating FTS request status" )
      res = self.transferDB.setFTSReqStatus( ftsReqID, 'Finished' )
      if not res['OK']:
        log.error( 'Failed update FTS Request status', res['Message'] )
    return S_OK()


  def updateFileToFTS( self, ftsReqID, channelID, filesToRetry, filesToFail, completedFileIDs, fileToFTSUpdates ):
    """ update TransferDB.FileToFTS table for finished request

    :param int ftsReqID: FTSReq.FTSReqID
    :param int channelID: FTSReq.ChannelID
    :param list filesToRetry: FileIDs to retry
    :param list filesToFail: FileIDs for failed files
    :param list completedFileIDs: files completed
    :param list fileToFTSUpdates: ???
    """
    log = gLogger.getSubLogger( "@%s" % ftsReqID )

    allUpdated = True

    res = self.transferDB.resetFileChannelStatus( channelID, filesToRetry ) if filesToRetry else S_OK()
    if not res["OK"]:
      log.error( "Failed to update the Channel table for file to retry.", res["Message"] )
      allUpdated = False

    for fileID in filesToFail:
      log.info( "Updating the Channel table for files to reschedule" )
      res = self.transferDB.setFileToReschedule( fileID )
      if not res["OK"]:
        log.error( "Failed to update Channel table for failed files.", res["Message"] )
        allUpdated = False
      elif res["Value"] == "max reschedule attempt reached":
        log.error( "setting Channel status to 'Failed' : " % res["Value"] )
        res = self.transferDB.setFileChannelStatus( channelID, fileID, 'Failed' )
        if not res["OK"]:
          log.error( "Failed to update Channel table for failed files.", res["Message"] )
          allUpdated = False

    if completedFileIDs:
      res = self.transferDB.updateCompletedChannelStatus( channelID, completedFileIDs )
      if not res["OK"]:
        log.error( "Failed to update the Channel table for successful files.", res["Message"] )
        allUpdated = False
      res = self.transferDB.updateAncestorChannelStatus( channelID, completedFileIDs )
      if not res["OK"]:
        log.error( 'Failed to update the Channel table for ancestors of successful files.', res['Message'] )
        allUpdated = False

    if fileToFTSUpdates:
      res = self.transferDB.setFileToFTSFileAttributes( ftsReqID, channelID, fileToFTSUpdates )
      if not res["OK"]:
        log.error( "Failed to update the FileToFTS table for files.", res["Message"] )
        allUpdated = False

    return S_OK( allUpdated )

  def updateFileToCat( self, oFTSRequest, channelID, fileIDDict, completedFiles, filesToFail ):
    """ update TransferDB.FileToCat table for finished request

    :param FTSRequest oFTSRequest: FTSRequest instance
    :param int ftsReqID: FTSReq.FTSReqID
    :param dict fileIDDict: fileIDs dictionary
    :param int channelID: FTSReq.ChannelID
    """
    res = oFTSRequest.getFailedRegistrations()
    failedRegistrations = res["Value"]
    regFailedFileIDs = []
    regDoneFileIDs = []
    regForgetFileIDs = []
    for fileID, fileDict in fileIDDict.items():
      lfn = fileDict['LFN']

      if lfn in failedRegistrations:
        regFailedFileIDs.append( fileID )
        # if the LFN appears more than once, FileToCat needs to be reset only once
        del failedRegistrations[lfn]
      elif lfn in completedFiles:
        regDoneFileIDs.append( fileID )
      elif fileID in filesToFail:
        regForgetFileIDs.append( fileID )

    res = self.transferDB.setRegistrationWaiting( channelID, regFailedFileIDs ) if regFailedFileIDs else S_OK()
    if not res["OK"]:
      res["Message"] = "Failed to reset entries in FileToCat: %s" % res["Message"]
      return res

    res = self.transferDB.setRegistrationDone( channelID, regDoneFileIDs ) if regDoneFileIDs else S_OK()
    if not res["OK"]:
      res["Message"] = "Failed to set entries Done in FileToCat: %s" % res["Message"]
      return res

    # This entries could also be set to Failed, but currently there is no method to do so.
    res = self.transferDB.setRegistrationDone( channelID, regForgetFileIDs ) if regForgetFileIDs else S_OK()
    if not res["OK"]:
      res["Message"] = "Failed to set entries Done in FileToCat: %s" % res["Message"]
      return res

    return S_OK()

  @classmethod
  def missingSource( cls, failReason ):
    """ check if message sent by FTS server is concering missing source file

    :param str failReason: message sent by FTS server
    """
    for error in cls.missingSourceErrors:
      if error.search( failReason ):
        return 1
    return 0
