########################################################################
# $HeadURL$
########################################################################
""" :mod: FTSSubmitAgent
    ====================

    FTS Submit Agent takes files from the TransferDB and submits them to the FTS using
    FTSRequest helper class.

    :deprecated:
"""

# # imports
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.DataManagementSystem.Client.FTSRequest import FTSRequest

__RCSID__ = "$Id$"

class FTSSubmitAgent( AgentModule ):
  """
  .. class:: FTSSubmitAgent

  This class is submitting previously scheduled files to the FTS system using helper class FTSRequest.

  Files to be transferred are read from TransferDB.Channel table, only those with Status = 'Waiting'.
  After submission TransferDB.Channel.Status is set to 'Executing'. The rest of state propagation is
  done in FTSMonitorAgent.

  An information about newly created FTS request is hold in TransferDB.FTSReq (request itself) table and
  TransferDB.FileToFTS (files in request) and TransferDB.FileToCat (files to be registered, for
  failover only).
  """
  # # placaholder for TransferDB reference
  transferDB = None
  # # placeholder for max job per channel
  maxJobsPerChannel = 2
  # # placeholder for checksum test option flag
  cksmTest = False
  # # placeholder for checksum type
  cksmType = ""
  # # default checksum type
  __defaultCksmType = "ADLER32"

  def initialize( self ):
    """ agent's initalisation

    :param self: self reference
    """
    # # save tarsnferDB handler
    self.transferDB = TransferDB()
    # # read config options
    self.maxJobsPerChannel = self.am_getOption( 'MaxJobsPerChannel', self.maxJobsPerChannel )
    self.log.info( "max jobs/channel = %s" % self.maxJobsPerChannel )

    # # checksum test
    self.cksmTest = bool( self.am_getOption( "ChecksumTest", False ) )
    # # ckecksum type
    if self.cksmTest:
      self.cksmType = str( self.am_getOption( "ChecksumType", self.__defaultCksmType ) ).upper()
      if self.cksmType and self.cksmType not in ( "ADLER32", "MD5", "SHA1" ):
        self.log.warn( "unknown checksum type: %s, will use default %s" % ( self.cksmType, self.__defaultCksmType ) )
        self.cksmType = self.__defaultCksmType

    self.log.info( "checksum test is %s" % ( { True : "enabled using %s checksum" % self.cksmType,
                                               False : "disabled"}[self.cksmTest] ) )


    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.filesBeingStaged = {}
    return S_OK()

  def execute( self ):
    """ execution in one agent's cycle

    :param self: self reference
    """

    #########################################################################
    #  Obtain the eligible channels for submission.
    self.log.info( 'Obtaining channels eligible for submission.' )
    res = self.transferDB.selectChannelsForSubmission( self.maxJobsPerChannel )
    if not res['OK']:
      self.log.error( "Failed to retrieve channels for submission.", res['Message'] )
      return S_OK()
    elif not res['Value']:
      self.log.info( "FTSSubmitAgent. No channels eligible for submission." )
      return S_OK()
    channelDicts = res['Value']
    self.log.info( 'Found %s eligible channels.' % len( channelDicts ) )

    #########################################################################
    # Submit to all the eligible waiting channels.
    i = 1
    for channelDict in channelDicts:
      infoStr = "\n\n##################################################################################\n\n"
      infoStr = "%sStarting submission loop %s of %s\n\n" % ( infoStr, i, len( channelDicts ) )
      self.log.info( infoStr )
      res = self.submitTransfer( channelDict )
      i += 1
    return S_OK()

  def submitTransfer( self, channelDict ):
    """ create and submit FTS jobs based on information it gets from the DB

    :param self: self reference
    :param dict channelDict: dict with channel info as read from TransferDB.selectChannelsForSubmission
    """

    channelID = channelDict['ChannelID']
    filesPerJob = channelDict['NumFiles']

    #########################################################################
    #  Obtain the first files in the selected channel.
    self.log.info( "FTSSubmitAgent.submitTransfer: Attempting to obtain files for channel %s: %s to %s"
                   % ( channelID, channelDict['Source'], channelDict['Destination'] ) )
    res = self.transferDB.getFilesForChannel( channelID, 2 * filesPerJob )
    if not res['OK']:
      errStr = 'FTSSubmitAgent.%s' % res['Message']
      self.log.error( errStr )
      return S_OK()
    if not res['Value']:
      self.log.info( "FTSSubmitAgent.submitTransfer: No files found for channel." )
      return S_OK()
    filesDict = res['Value']
    sourceSE = filesDict['SourceSE']
    targetSE = filesDict['TargetSE']
    self.log.info( 'Obtained %s files for channel %s to %s' % ( len( filesDict['Files'] ), sourceSE, targetSE ) )
    if self.filesBeingStaged.get( channelID ):
      self.log.info( '%d files are currently in staging status' % len( self.filesBeingStaged[channelID] ) )

    # Create the FTSRequest object for preparing the submission
    oFTSRequest = FTSRequest()
    oFTSRequest.setSourceSE( sourceSE )
    oFTSRequest.setTargetSE( targetSE )
    files = filesDict['Files']

    # # enable/disable cksm test
    oFTSRequest.setCksmTest( self.cksmTest )
    if self.cksmType:
      oFTSRequest.setCksmType( self.cksmType )

    #########################################################################
    #  Populate the FTS Request with the files.
    self.log.info( 'Populating the FTS request with file information' )
    fileIDs = []
    totalSize = 0
    fileIDSizes = {}
    lfns = set()
    for fileMeta in files:
      lfn = fileMeta['LFN']
      lfns.add( lfn )
      oFTSRequest.setLFN( lfn )
      oFTSRequest.setSourceSURL( lfn, fileMeta['SourceSURL'] )
      oFTSRequest.setTargetSURL( lfn, fileMeta['TargetSURL'] )
      if lfn in self.filesBeingStaged.get( channelID, [] ):
        oFTSRequest.setStatus( lfn, 'Staging' )
      fileID = fileMeta['FileID']
      fileIDs.append( fileID )
      totalSize += fileMeta['Size']
      fileIDSizes[fileID] = fileMeta['Size']

    oFTSRequest.resolveSource()
    noSource = [ lfn for lfn, fileInfo in oFTSRequest.fileDict.items()
                     if fileInfo.get( "Status", "" ) == "Failed" and fileInfo.get( "Reason", "" ) in ( "No replica at SourceSE",
                                                                                                   "Source file does not exist" ) ]
    toReschedule = [fileMeta["FileID"] for fileMeta in files if fileMeta["LFN"] in noSource]

    if toReschedule:
      self.log.info( "Found %s files to reschedule" % len( toReschedule ) )
      for fileID in toReschedule:
        res = self.transferDB.setFileToReschedule( fileID )
        if not res["OK"]:
          self.log.error( "Failed to update Channel table for failed files.", res["Message"] )
        elif res["Value"] == "max reschedule attempt reached":
          self.log.error( "setting Channel status to 'Failed' : " % res["Value"] )
          res = self.transferDB.setFileChannelStatus( channelID, fileID, 'Failed' )
          if not res["OK"]:
            self.log.error( "Failed to update Channel table for failed files.", res["Message"] )

    #########################################################################
    #  Submit the FTS request and retrieve the FTS GUID/Server
    self.log.info( 'Submitting the FTS request' )
    res = oFTSRequest.submit()
    if not res['OK']:
      errStr = "FTSSubmitAgent.submit: %s" % res['Message']
      self.log.error( errStr )
      self.log.info( 'Updating the Channel table for files to retry' )
      res = self.transferDB.resetFileChannelStatus( channelID, fileIDs )
      if not res['OK']:
        self.log.error( 'Failed to update the Channel table for file to retry.', res['Message'] )
      return S_ERROR( errStr )
    ftsGUID = res['Value']['ftsGUID']
    ftsServer = res['Value']['ftsServer']
    nbSubmitted = res['Value']['submittedFiles']
    infoStr = """Submitted FTS Job:

              FTS Guid: %s
              FTS Server: %s
              ChannelID: %s
              SourceSE: %s
              TargetSE: %s
              Files: %s

""" % ( ftsGUID, ftsServer, str( channelID ), sourceSE, targetSE, str( nbSubmitted ) )
    self.log.info( infoStr )

    # # filter out skipped files
    failedFiles = oFTSRequest.getFailed()['Value']
    stagingFiles = oFTSRequest.getStaging()['Value']
    # cache files being staged
    self.filesBeingStaged.setdefault( channelID, set() ).update( stagingFiles )
    submittedFiles = lfns.difference( failedFiles, stagingFiles )
    # files being submitted are staged
    self.filesBeingStaged[channelID] -= submittedFiles
    failedIDs = set( [ meta["FileID"] for meta in files if meta["LFN"] in failedFiles ] )
    stagingIDs = set( [ meta["FileID"] for meta in files if meta["LFN"] in stagingFiles ] )
    # # only submitted
    submittedIDs = set( fileIDs ) - failedIDs - stagingIDs
    # # only count the submitted size
    totalSize = sum( [ meta["Size"] for meta in files if meta["FileID"] in submittedIDs ] )

    #########################################################################
    #  Insert the FTS Req details and add the number of files and size
    res = self.transferDB.insertFTSReq( ftsGUID, ftsServer, channelID )
    if not res['OK']:
      errStr = "FTSSubmitAgent.%s" % res['Message']
      self.log.error( errStr )
      return S_ERROR( errStr )
    ftsReqID = res['Value']
    self.log.info( 'Obtained FTS RequestID %s' % ftsReqID )
    res = self.transferDB.setFTSReqAttribute( ftsReqID, 'SourceSE', sourceSE )
    if not res['OK']:
      self.log.error( "Failed to set SourceSE for FTSRequest", res['Message'] )
    res = self.transferDB.setFTSReqAttribute( ftsReqID, 'TargetSE', targetSE )
    if not res['OK']:
      self.log.error( "Failed to set TargetSE for FTSRequest", res['Message'] )
    res = self.transferDB.setFTSReqAttribute( ftsReqID, 'NumberOfFiles', len( submittedIDs ) )
    if not res['OK']:
      self.log.error( "Failed to set NumberOfFiles for FTSRequest", res['Message'] )
    res = self.transferDB.setFTSReqAttribute( ftsReqID, 'TotalSize', totalSize )
    if not res['OK']:
      self.log.error( "Failed to set TotalSize for FTSRequest", res['Message'] )

    #########################################################################
    #  Insert the submission event in the FTSReqLogging table
    event = 'Submitted'
    res = self.transferDB.addLoggingEvent( ftsReqID, event )
    if not res['OK']:
      errStr = "FTSSubmitAgent.%s" % res['Message']
      self.log.error( errStr )

    #########################################################################
    #  Insert the FileToFTS details and remove the files from the channel
    self.log.info( 'Setting the files as Executing in the Channel table' )
    res = self.transferDB.setChannelFilesExecuting( channelID, list( submittedIDs ) )
    if not res['OK']:
      self.log.error( 'Failed to update the Channel tables for files.', res['Message'] )

    lfns = []
    fileToFTSFileAttributes = []
    for fileMeta in files:
      fileID = fileMeta['FileID']
      # Staging is not an error case
      if fileID not in stagingIDs:
        lfns.append( fileMeta['LFN'] )
        fileToFTSFileAttributes.append( ( fileID, fileIDSizes[fileID] ) )

    self.log.info( 'Populating the FileToFTS table with file information' )
    res = self.transferDB.setFTSReqFiles( ftsReqID, channelID, fileToFTSFileAttributes )
    if not res['OK']:
      self.log.error( 'Failed to populate the FileToFTS table with files.' )
