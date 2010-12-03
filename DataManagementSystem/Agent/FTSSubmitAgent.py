########################################################################
# $HeadURL$
########################################################################

"""  FTS Submit Agent takes files from the TransferDB and submits them to the FTS
"""
from DIRAC                                                           import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                     import AgentModule
from DIRAC.ConfigurationSystem.Client.PathFinder                     import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB                        import TransferDB
from DIRAC.DataManagementSystem.Client.FTSRequest                    import FTSRequest
from DIRAC.Core.DISET.RPCClient                                      import RPCClient
import os, time
from types import *

__RCSID__ = "$Id$"

class FTSSubmitAgent( AgentModule ):

  def initialize( self ):

    self.TransferDB = TransferDB()
    self.maxJobsPerChannel = self.am_getOption( 'MaxJobsPerChannel', 2 )

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )
    return S_OK()

  def execute( self ):

    #########################################################################
    #  Obtain the eligible channels for submission.
    gLogger.info( 'Obtaining channels eligible for submission.' )
    res = self.TransferDB.selectChannelsForSubmission( self.maxJobsPerChannel )
    if not res['OK']:
      gLogger.error( "Failed to retrieve channels for submission.", res['Message'] )
      return S_OK()
    elif not res['Value']:
      gLogger.info( "FTSSubmitAgent. No channels eligable for submission." )
      return S_OK()
    channelDicts = res['Value']
    gLogger.info( 'Found %s eligible channels.' % len( channelDicts ) )

    #########################################################################
    # Submit to all the eligible waiting channels.
    i = 1
    for channelDict in channelDicts:
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting submission loop %s of %s\n\n" % ( infoStr, i, len( channelDicts ) )
      gLogger.info( infoStr )
      res = self.submitTransfer( channelDict )
      i += 1
    return S_OK()

  def submitTransfer( self, channelDict ):
    """ This method creates and submits FTS jobs based on information it gets from the DB
    """

    # Create the FTSRequest object for preparing the submission
    oFTSRequest = FTSRequest()
    channelID = channelDict['ChannelID']
    filesPerJob = channelDict['NumFiles']

    #########################################################################
    #  Obtain the first files in the selected channel.
    gLogger.info( "FTSSubmitAgent.submitTransfer: Attempting to obtain files to transfer on channel %s" % channelID )
    res = self.TransferDB.getFilesForChannel( channelID, 2 * filesPerJob )
    if not res['OK']:
      errStr = 'FTSSubmitAgent.%s' % res['Message']
      gLogger.error( errStr )
      return S_OK()
    if not res['Value']:
      gLogger.info( "FTSSubmitAgent.submitTransfer: No files to found for channel." )
      return S_OK()
    filesDict = res['Value']
    gLogger.info( 'Obtained %s files for channel' % len( filesDict['Files'] ) )

    sourceSE = filesDict['SourceSE']
    oFTSRequest.setSourceSE( sourceSE )
    targetSE = filesDict['TargetSE']
    oFTSRequest.setTargetSE( targetSE )
    gLogger.info( "FTSSubmitAgent.submitTransfer: Attempting to obtain files for %s to %s channel." % ( sourceSE, targetSE ) )
    files = filesDict['Files']

    #########################################################################
    #  Populate the FTS Request with the files.
    gLogger.info( 'Populating the FTS request with file information' )
    fileIDs = []
    totalSize = 0
    fileIDSizes = {}
    for file in files:
      lfn = file['LFN']
      oFTSRequest.setLFN( lfn )
      oFTSRequest.setSourceSURL( lfn, file['SourceSURL'] )
      oFTSRequest.setTargetSURL( lfn, file['TargetSURL'] )
      fileID = file['FileID']
      fileIDs.append( fileID )
      totalSize += file['Size']
      fileIDSizes[fileID] = file['Size']

    #########################################################################
    #  Submit the FTS request and retrieve the FTS GUID/Server
    gLogger.info( 'Submitting the FTS request' )
    res = oFTSRequest.submit()
    if not res['OK']:
      errStr = "FTSSubmitAgent.%s" % res['Message']
      gLogger.error( errStr )
      gLogger.info( 'Updating the Channel table for files to retry' )
      res = self.TransferDB.resetFileChannelStatus( channelID, fileIDs )
      if not res['OK']:
        gLogger.error( 'Failed to update the Channel table for file to retry.', res['Message'] )
      return S_ERROR( errStr )
    res = oFTSRequest.getFTSGUID()
    if not res['OK']:
      gLogger.error( "Failed to get FTS GUID after submission", res['Message'] )
      return res
    ftsGUID = res['Value']
    res = oFTSRequest.getFTSServer()
    if not res['OK']:
      gLogger.error( "Failed to get FTS Server after submission", res['Message'] )
      return res
    ftsServer = res['Value']

    infoStr = "Submitted FTS Job:\n\n\
              FTS Guid:%s\n\
              FTS Server:%s\n\
              ChannelID:%s\n\
              SourceSE:%s\n\
              TargetSE:%s\n\
              Files:%s\n\n" % ( ftsGUID.ljust( 15 ), ftsServer.ljust( 15 ), str( channelID ).ljust( 15 ), sourceSE.ljust( 15 ), targetSE.ljust( 15 ), str( len( files ) ).ljust( 15 ) )
    gLogger.info( infoStr )

    #########################################################################
    #  Insert the FTS Req details and add the number of files and size
    res = self.TransferDB.insertFTSReq( ftsGUID, ftsServer, channelID )
    if not res['OK']:
      errStr = "FTSSubmitAgent.%s" % res['Message']
      gLogger.error( errStr )
      return S_ERROR( errStr )
    ftsReqID = res['Value']
    gLogger.info( 'Obtained FTS RequestID %s' % ftsReqID )
    res = self.TransferDB.setFTSReqAttribute( ftsReqID, 'SourceSE', sourceSE )
    if not res['OK']:
      gLogger.error( "Failed to set SourceSE for FTSRequest", res['Message'] )
    res = self.TransferDB.setFTSReqAttribute( ftsReqID, 'TargetSE', targetSE )
    if not res['OK']:
      gLogger.error( "Failed to set TargetSE for FTSRequest", res['Message'] )
    res = self.TransferDB.setFTSReqAttribute( ftsReqID, 'NumberOfFiles', len( fileIDs ) )
    if not res['OK']:
      gLogger.error( "Failed to set NumberOfFiles for FTSRequest", res['Message'] )
    res = self.TransferDB.setFTSReqAttribute( ftsReqID, 'TotalSize', totalSize )
    if not res['OK']:
      gLogger.error( "Failed to set TotalSize for FTSRequest", res['Message'] )

    #########################################################################
    #  Insert the submission event in the FTSReqLogging table
    event = 'Submitted'
    res = self.TransferDB.addLoggingEvent( ftsReqID, event )
    if not res['OK']:
      errStr = "FTSSubmitAgent.%s" % res['Message']
      gLogger.error( errStr )

    #########################################################################
    #  Insert the FileToFTS details and remove the files from the channel
    gLogger.info( 'Setting the files as Executing in the Channel table' )
    res = self.TransferDB.setChannelFilesExecuting( channelID, fileIDs )
    if not res['OK']:
      gLogger.error( 'Failed to update the Channel tables for files.', res['Message'] )

    lfns = []
    fileToFTSFileAttributes = []
    for file in files:
      lfn = file['LFN']
      fileID = file['FileID']
      lfns.append( lfn )
      fileToFTSFileAttributes.append( ( fileID, fileIDSizes[fileID] ) )

    gLogger.info( 'Populating the FileToFTS table with file information' )
    res = self.TransferDB.setFTSReqFiles( ftsReqID, channelID, fileToFTSFileAttributes )
    if not res['OK']:
      gLogger.error( 'Failed to populate the FileToFTS table with files.' )
