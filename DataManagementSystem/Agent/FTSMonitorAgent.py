########################################################################
# $HeadURL$
########################################################################

"""  FTS Monitor takes FTS Requests from the TransferDB and monitors them
"""
from DIRAC                                                   import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                             import AgentModule
from DIRAC.ConfigurationSystem.Client.PathFinder             import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB                import TransferDB
from DIRAC.DataManagementSystem.Client.FTSRequest            import FTSRequest
from DIRAC.Core.DISET.RPCClient                              import RPCClient
from DIRAC.AccountingSystem.Client.Types.DataOperation       import DataOperation
from DIRAC.Core.Utilities import Time
import os, time, re
from types import *

__RCSID__ = "$Id$"

AGENT_NAME = 'DataManagement/FTSMonitorAgent'

class FTSMonitorAgent( AgentModule ):

  def initialize( self ):
    self.TransferDB = TransferDB()

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  def execute( self ):

    #########################################################################
    #  Get the details for all active FTS requests
    gLogger.info( 'Obtaining requests to monitor' )
    res = self.TransferDB.getFTSReq()
    if not res['OK']:
      gLogger.error( "Failed to get FTS requests", res['Message'] )
      return res
    if not res['Value']:
      gLogger.info( "FTSMonitorAgent. No FTS requests found to monitor." )
      return S_OK()
    ftsReqs = res['Value']
    gLogger.info( 'Found %s FTS jobs' % len( ftsReqs ) )

    #######################################################################
    # Check them all....
    i = 1
    for ftsReqDict in ftsReqs:
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting monitoring loop %s of %s\n\n" % ( infoStr, i, len( ftsReqs ) )
      gLogger.info( infoStr )
      res = self.monitorTransfer( ftsReqDict )
      i += 1
    return S_OK()

  def monitorTransfer( self, ftsReqDict ):
    """ Monitors transfer  obtained from TransferDB
    """

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
    gLogger.info( 'Perform summary update of the FTS Request' )
    infoStr = "Monitoring FTS Job:\n\n"
    infoStr = "%sglite-transfer-status -s %s -l %s\n" % ( infoStr, ftsServer, ftsGUID )
    infoStr = "%s%s%s\n" % ( infoStr, 'FTS GUID:'.ljust( 20 ), ftsGUID )
    infoStr = "%s%s%s\n\n" % ( infoStr, 'FTS Server:'.ljust( 20 ), ftsServer )
    gLogger.info( infoStr )
    res = oFTSRequest.summary()
    self.TransferDB.setFTSReqLastMonitor( ftsReqID )
    if not res['OK']:
      gLogger.error( "Failed to update the FTS request summary", res['Message'] )
      return res
    res = oFTSRequest.dumpSummary()
    if not res['OK']:
      gLogger.error( "Failed to get FTS request summary", res['Message'] )
      return res
    gLogger.info( res['Value'] )
    res = oFTSRequest.getPercentageComplete()
    if not res['OK']:
      gLogger.error( "Failed to get FTS percentage complete", res['Message'] )
      return res
    gLogger.info( 'FTS Request found to be %.1f percent complete' % res['Value'] )
    self.TransferDB.setFTSReqAttribute( ftsReqID, 'PercentageComplete', res['Value'] )
    self.TransferDB.addLoggingEvent( ftsReqID, res['Value'] )

    #########################################################################
    # Update the information in the TransferDB if the transfer is terminal.
    res = oFTSRequest.isRequestTerminal()
    if not res['OK']:
      gLogger.error( "Failed to determine whether FTS request terminal", res['Message'] )
      return res
    if not res['Value']:
      return S_OK()
    gLogger.info( 'FTS Request found to be terminal, updating file states' )

    #########################################################################
    # Get the LFNS associated to the FTS request
    gLogger.info( 'Obtaining the LFNs associated to this request' )
    res = self.TransferDB.getFTSReqLFNs( ftsReqID, channelID, sourceSE )
    if not res['OK']:
      gLogger.error( "Failed to obtain FTS request LFNs", res['Message'] )
      return res
    files = res['Value']
    if not files:
      gLogger.error( 'No files present for transfer' )
      return S_ERROR( 'No files were found in the DB' )
    lfns = files.keys()
    gLogger.info( 'Obtained %s files' % len( lfns ) )
    for lfn in lfns:
      oFTSRequest.setLFN( lfn )

    res = oFTSRequest.monitor()
    if not res['OK']:
      gLogger.error( "Failed to perform detailed monitoring of FTS request", res['Message'] )
      return res
    res = oFTSRequest.getFailed()
    if not res['OK']:
      gLogger.error( "Failed to obtained failed files for FTS request", res['Message'] )
      return res
    failedFiles = res['Value']
    res = oFTSRequest.getDone()
    if not res['OK']:
      gLogger.error( "Failed to obtained successful files for FTS request", res['Message'] )
      return res
    completedFiles = res['Value']

    fileToFTSUpdates = []
    completedFileIDs = []
    for lfn in completedFiles:
      fileID = files[lfn]
      completedFileIDs.append( fileID )
      transferTime = 0
      res = oFTSRequest.getTransferTime( lfn )
      if res['OK']:
        transferTime = res['Value']
      fileToFTSUpdates.append( ( fileID, 'Completed', '', 0, transferTime ) )

    filesToRetry = []
    filesToReschedule = []
    for lfn in failedFiles:
      fileID = files[lfn]
      failReason = ''
      res = oFTSRequest.getFailReason( lfn )
      if res['OK']:
        failReason = res['Value']
      if self.missingSource( failReason ):
        gLogger.error( 'The source SURL does not exist.', '%s %s' % ( lfn, oFTSRequest.getSourceSURL( lfn ) ) )
        filesToReschedule.append( fileID )
      else:
        filesToRetry.append( fileID )
      gLogger.error( 'Failed to replicate file on channel.', "%s %s" % ( channelID, failReason ) )
      fileToFTSUpdates.append( ( fileID, 'Failed', failReason, 0, 0 ) )

    allUpdated = True
    if filesToRetry:
      gLogger.info( 'Updating the Channel table for files to retry' )
      res = self.TransferDB.resetFileChannelStatus( channelID, filesToRetry )
      if not res['OK']:
        gLogger.error( 'Failed to update the Channel table for file to retry.', res['Message'] )
        allUpdated = False
    for fileID in filesToReschedule:
      gLogger.info( 'Updating the Channel table for files to reschedule' )
      res = self.TransferDB.setFileChannelStatus( channelID, fileID, 'Failed' )
      if not res['OK']:
        gLogger.error( 'Failed to update Channel table for failed files.', res['Message'] )
        allUpdated = False

    if completedFileIDs:
      gLogger.info( 'Updating the Channel table for successful files' )
      res = self.TransferDB.updateCompletedChannelStatus( channelID, completedFileIDs )
      if not res['OK']:
        gLogger.error( 'Failed to update the Channel table for successful files.', res['Message'] )
        allUpdated = False
      gLogger.info( 'Updating the Channel table for ancestors of successful files' )
      res = self.TransferDB.updateAncestorChannelStatus( channelID, completedFileIDs )
      if not res['OK']:
        gLogger.error( 'Failed to update the Channel table for ancestors of successful files.', res['Message'] )
        allUpdated = False
      
      gLogger.info( 'Updating the FileToCat table for successful files' )
      res = self.TransferDB.setRegistrationWaiting( channelID, completedFileIDs )
      if not res['OK']:
        gLogger.error( 'Failed to update the FileToCat table for successful files.', res['Message'] )
        allUpdated = False

    if fileToFTSUpdates:
      gLogger.info( 'Updating the FileToFTS table for files' )
      res = self.TransferDB.setFileToFTSFileAttributes( ftsReqID, channelID, fileToFTSUpdates )
      if not res['OK']:
        gLogger.error( 'Failed to update the FileToFTS table for files.', res['Message'] )
        allUpdated = False

    if allUpdated:
      res = oFTSRequest.finalize()
      if not res['OK']:
        gLogger.error( "Failed to perform the finalization for the FTS request", res['Message'] )
        return res

      gLogger.info( 'Adding logging event for FTS request' )
      # Now set the FTSReq status to terminal so that it is not monitored again
      res = self.TransferDB.addLoggingEvent( ftsReqID, 'Finished' )
      if not res['OK']:
        gLogger.error( 'Failed to add logging event for FTS Request', res['Message'] )

      gLogger.info( 'Updating FTS request status' )
      res = self.TransferDB.setFTSReqStatus( ftsReqID, 'Finished' )
      if not res['OK']:
        gLogger.error( 'Failed update FTS Request status', res['Message'] )
    return S_OK()

  def missingSource( self, failReason ):
    missingSourceErrors = ['SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] Failed',
                           'SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] No such file or directory',
                           'SOURCE error during PREPARATION phase: \[INVALID_PATH\] Failed',
                           'SOURCE error during PREPARATION phase: \[INVALID_PATH\] The requested file either does not exist',
                           'TRANSFER error during TRANSFER phase: \[INVALID_PATH\] the server sent an error response: 500 500 Command failed. : open error: No such file or directory',
                           'SOURCE error during TRANSFER_PREPARATION phase: \[USER_ERROR\] source file doesnt exist']
    for error in missingSourceErrors:
      if re.search( error, failReason ):
        return 1
    return 0
