########################################################################
# $HeadURL$
########################################################################
""" 
  :mod: FTSMonitorAgent
  =====================

  The FTSMonitorAgent takes FTS Requests from the TransferDB and monitors their execution
  using FTSRequest helper class.

"""
## imports
import re
## from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
#from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.DataManagementSystem.Client.FTSRequest import FTSRequest
## RCSID
__RCSID__ = "$Id$"
## agent's name
AGENT_NAME = 'DataManagement/FTSMonitorAgent'

class FTSMonitorAgent( AgentModule ):
  """
  .. class:: FTSMonitorAgent

  """
  ## transfer DB handle
  transferDB = None

  def initialize( self ):
    """ agent's initialisation """
    self.transferDB = TransferDB()
    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )
    return S_OK()

  def execute( self ):
    """ execution in one cycle """

    #########################################################################
    #  Get the details for all active FTS requests
    self.log.info( 'Obtaining requests to monitor' )
    res = self.transferDB.getFTSReq()
    if not res['OK']:
      self.log.error( "Failed to get FTS requests", res['Message'] )
      return res
    if not res['Value']:
      self.log.info( "FTSMonitorAgent. No FTS requests found to monitor." )
      return S_OK()
    ftsReqs = res['Value']
    self.log.info( 'Found %s FTS jobs' % len( ftsReqs ) )

    #######################################################################
    # Check them all....
    i = 1
    for ftsReqDict in ftsReqs:
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting monitoring loop %s of %s\n\n" % ( infoStr, i, len( ftsReqs ) )
      self.log.info( infoStr )
      res = self.monitorTransfer( ftsReqDict )
      i += 1
    return S_OK()

  def monitorTransfer( self, ftsReqDict ):
    """ monitors transfer  obtained from TransferDB """

    ftsReqID = ftsReqDict['FTSReqID']
    ftsGUID = ftsReqDict['FTSGuid']
    ftsServer = ftsReqDict['FTSServer']
    channelID = ftsReqDict['ChannelID']
    sourceSE = ftsReqDict['SourceSE']
    targetSE = ftsReqDict['TargetSE']

    oFTSRequest = FTSRequest()
    oFTSRequest.setFTSServer( ftsServer )
    oFTSRequest.setFTSGUID( ftsGUID )
    oFTSRequest.setSourceSE( sourceSE )
    oFTSRequest.setTargetSE( targetSE )

    #########################################################################
    # Perform summary update of the FTS Request and update FTSReq entries.
    self.log.info( 'Perform summary update of the FTS Request' )
    infoStr = "Monitoring FTS Job:\n\n"
    infoStr = "%sglite-transfer-status -s %s -l %s\n" % ( infoStr, ftsServer, ftsGUID )
    infoStr = "%s%s%s\n" % ( infoStr, 'FTS GUID:'.ljust( 20 ), ftsGUID )
    infoStr = "%s%s%s\n\n" % ( infoStr, 'FTS Server:'.ljust( 20 ), ftsServer )
    self.log.info( infoStr )
    res = oFTSRequest.summary()
    self.transferDB.setFTSReqLastMonitor( ftsReqID )
    if not res['OK']:
      self.log.error( "Failed to update the FTS request summary", res['Message'] )
      if "getTransferJobSummary2: Not authorised to query request" in res["Message"]:
        self.log.error("FTS job is not existing at the FTS server anymore, will clean it up on TransferDB side")

        ## get fileIDs
        fileIDs = self.transferDB.getFTSReqFileIDs( ftsReqID )
        if not fileIDs["OK"]:
          self.log.error("Unable to retrieve FileIDs associated to %s request" % ftsReqID )
          return fileIDs
        fileIDs = fileIDs["Value"]
      
        ## update FileToFTS table, this is just a clean up, no worry if somethings goes wrong
        for fileID in fileIDs:
          fileStatus = self.transferDB.setFileToFTSFileAttribute( ftsReqID, fileID, 
                                                                  "Status", "Failed" )
          if not fileStatus["OK"]:
            self.log.error("Unable to set FileToFTS status to Failed for FileID %s: %s" % ( fileID, 
                                                                                           fileStatus["Message"] ) )
                          
          failReason = self.transferDB.setFileToFTSFileAttribute( ftsReqID, fileID, 
                                                                  "Reason", "FTS job expired on server" )
          if not failReason["OK"]:
            self.log.error("Unable to set FileToFTS reason for FileID %s: %s" % ( fileID, 
                                                                                 failReason["Message"] ) )

        ## update Channel table
        resetChannels = self.transferDB.resetFileChannelStatus( channelID, fileIDs )
        if not resetChannels["OK"]:
          self.log.error("Failed to reset Channel table for files to retry")
          return resetChannels   

        ## update FTSReq table
        self.log.info( 'Setting FTS request status to Finished' )
        ftsReqStatus = self.transferDB.setFTSReqStatus( ftsReqID, 'Finished' )
        if not ftsReqStatus['OK']:
          self.log.error( 'Failed update FTS Request status', ftsReqStatus['Message'] )
          return ftsReqStatus
        ## if we land here, everything should be OK
        return S_OK()

      return res
    res = oFTSRequest.dumpSummary()
    if not res['OK']:
      self.log.error( "Failed to get FTS request summary", res['Message'] )
      return res
    self.log.info( res['Value'] )
    res = oFTSRequest.getPercentageComplete()
    if not res['OK']:
      self.log.error( "Failed to get FTS percentage complete", res['Message'] )
      return res
    self.log.info( 'FTS Request found to be %.1f percent complete' % res['Value'] )
    self.transferDB.setFTSReqAttribute( ftsReqID, 'PercentageComplete', res['Value'] )
    self.transferDB.addLoggingEvent( ftsReqID, res['Value'] )

    #########################################################################
    # Update the information in the TransferDB if the transfer is terminal.
    res = oFTSRequest.isRequestTerminal()
    if not res['OK']:
      self.log.error( "Failed to determine whether FTS request terminal", res['Message'] )
      return res
    if not res['Value']:
      return S_OK()
    self.log.info( 'FTS Request found to be terminal, updating file states' )

    #########################################################################
    # Get the LFNS associated to the FTS request
    self.log.info( 'Obtaining the LFNs associated to this request' )
    res = self.transferDB.getFTSReqLFNs( ftsReqID, channelID, sourceSE )
    if not res['OK']:
      self.log.error( "Failed to obtain FTS request LFNs", res['Message'] )
      return res
    files = res['Value']
    if not files:
      self.log.error( 'No files present for transfer' )
      return S_ERROR( 'No files were found in the DB' )
    lfns = files.keys()
    self.log.info( 'Obtained %s files' % len( lfns ) )
    for lfn in lfns:
      oFTSRequest.setLFN( lfn )

    res = oFTSRequest.monitor()
    if not res['OK']:
      self.log.error( "Failed to perform detailed monitoring of FTS request", res['Message'] )
      return res
    res = oFTSRequest.getFailed()
    if not res['OK']:
      self.log.error( "Failed to obtained failed files for FTS request", res['Message'] )
      return res
    failedFiles = res['Value']
    res = oFTSRequest.getDone()
    if not res['OK']:
      self.log.error( "Failed to obtained successful files for FTS request", res['Message'] )
      return res
    completedFiles = res['Value']

    # An LFN can be included more than once if it was entered into more than one Request. 
    # FTS will only do the transfer once. We need to identify all FileIDs
    res = self.transferDB.getFTSReqFileIDs( ftsReqID )
    if not res['OK']:
      self.log.error( 'Failed to get FileIDs associated to FTS Request', res['Message'] )
      return res
    fileIDs = res['Value']
    res = self.transferDB.getAttributesForFilesList( fileIDs, ['LFN'] )
    if not res['OK']:
      self.log.error( 'Failed to get LFNs associated to FTS Request', res['Message'] )
      return res
    fileIDDict = res['Value']

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
        if res['OK']:
          transferTime = res['Value']
        fileToFTSUpdates.append( ( fileID, 'Completed', '', 0, transferTime ) )

      if lfn in failedFiles:
        failReason = ''
        res = oFTSRequest.getFailReason( lfn )
        if res['OK']:
          failReason = res['Value']
        if self.missingSource( failReason ):
          self.log.error( 'The source SURL does not exist.', '%s %s' % ( lfn, oFTSRequest.getSourceSURL( lfn ) ) )
          filesToFail.append( fileID )
        else:
          filesToRetry.append( fileID )
        self.log.error( 'Failed to replicate file on channel.', "%s %s" % ( channelID, failReason ) )
        fileToFTSUpdates.append( ( fileID, 'Failed', failReason, 0, 0 ) )

    allUpdated = True
    if filesToRetry:
      self.log.info( 'Updating the Channel table for files to retry' )
      res = self.transferDB.resetFileChannelStatus( channelID, filesToRetry )
      if not res['OK']:
        self.log.error( 'Failed to update the Channel table for file to retry.', res['Message'] )
        allUpdated = False
    for fileID in filesToFail:
      self.log.info( 'Updating the Channel table for files to reschedule' )
      res = self.transferDB.setFileChannelStatus( channelID, fileID, 'Failed' )
      if not res['OK']:
        self.log.error( 'Failed to update Channel table for failed files.', res['Message'] )
        allUpdated = False

    if completedFileIDs:
      self.log.info( 'Updating the Channel table for successful files' )
      res = self.transferDB.updateCompletedChannelStatus( channelID, completedFileIDs )
      if not res['OK']:
        self.log.error( 'Failed to update the Channel table for successful files.', res['Message'] )
        allUpdated = False
      self.log.info( 'Updating the Channel table for ancestors of successful files' )
      res = self.transferDB.updateAncestorChannelStatus( channelID, completedFileIDs )
      if not res['OK']:
        self.log.error( 'Failed to update the Channel table for ancestors of successful files.', res['Message'] )
        allUpdated = False

    if fileToFTSUpdates:
      self.log.info( 'Updating the FileToFTS table for files' )
      res = self.transferDB.setFileToFTSFileAttributes( ftsReqID, channelID, fileToFTSUpdates )
      if not res['OK']:
        self.log.error( 'Failed to update the FileToFTS table for files.', res['Message'] )
        allUpdated = False

    if allUpdated:
      res = oFTSRequest.finalize()
      if not res['OK']:
        self.log.error( "Failed to perform the finalization for the FTS request", res['Message'] )
        return res

      self.log.info( 'Adding logging event for FTS request' )
      # Now set the FTSReq status to terminal so that it is not monitored again
      res = self.transferDB.addLoggingEvent( ftsReqID, 'Finished' )
      if not res['OK']:
        self.log.error( 'Failed to add logging event for FTS Request', res['Message'] )

      res = oFTSRequest.getFailedRegistrations()
      failedRegistrations = res['Value']
      regFailedFileIDs = []
      regDoneFileIDs = []
      regForgetFileIDs = []
      for fileID, fileDict in fileIDDict.items():
        lfn = fileDict['LFN']
        if lfn in failedRegistrations:
          self.log.info( 'Setting failed registration in FileToCat back to Waiting', lfn )
          regFailedFileIDs.append( fileID )
          # if the LFN appears more than once, FileToCat needs to be reset only once 
          del failedRegistrations[lfn]
        elif lfn in completedFiles:
          regDoneFileIDs.append( fileID )
        elif fileID in filesToFail:
          regForgetFileIDs.append( fileID )


      if regFailedFileIDs:
        res = self.transferDB.setRegistrationWaiting( channelID, regFailedFileIDs )
        if not res['OK']:
          self.log.error( 'Failed to reset entries in FileToCat', res['Message'] )
          return res

      if regDoneFileIDs:
        res = self.transferDB.setRegistrationDone( channelID, regDoneFileIDs )
        if not res['OK']:
          self.log.error( 'Failed to set entries Done in FileToCat', res['Message'] )
          return res

      if regForgetFileIDs:
        # This entries could also be set to Failed, but currently there is no method to do so.
        res = self.transferDB.setRegistrationDone( channelID, regForgetFileIDs )
        if not res['OK']:
          self.log.error( 'Failed to set entries Done in FileToCat', res['Message'] )
          return res

      self.log.info( 'Updating FTS request status' )
      res = self.transferDB.setFTSReqStatus( ftsReqID, 'Finished' )
      if not res['OK']:
        self.log.error( 'Failed update FTS Request status', res['Message'] )
    return S_OK()

  def missingSource( self, failReason ):
    """ check if message sent by FTS server is concering missing source file 
    
    :param self: self reference
    :param str failReason: message sent by FTS server
    """
    missingSourceErrors = [ 
      'SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] Failed',
      'SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] No such file or directory',
      'SOURCE error during PREPARATION phase: \[INVALID_PATH\] Failed',
      'SOURCE error during PREPARATION phase: \[INVALID_PATH\] The requested file either does not exist',
      'TRANSFER error during TRANSFER phase: \[INVALID_PATH\] the server sent an error response: 500 500 Command failed. : open error: No such file or directory',
      'SOURCE error during TRANSFER_PREPARATION phase: \[USER_ERROR\] source file doesnt exist' ]
    for error in missingSourceErrors:
      if re.search( error, failReason ):
        return 1
    return 0
