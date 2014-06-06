########################################################################
# $HeadURL$
# File: TransferDB.py
########################################################################
""" :mod: TransferDB
    ================

    TransferDB is a front end to the TransferDB mysql database, built
    on top of RequestDB.

    This database holds all information used by DIRAC FTS subsystem.
    It is mainly used by FTSSubmitAgent, FTSMonitorAgent and TransferAgent.


    :deprecated:
"""

__RCSID__ = "$Id$"

# # imports
import threading
from types import ListType
import time
import datetime
import random
# # from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR, Time
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.List import intListToString
from DIRAC.Core.Utilities import Time
from DIRAC.Resources.Storage.StorageElement import StorageElement

# # it's a magic!
# MAGIC_EPOC_NUMBER = 1270000000
# # This is a better one, using only datetime (DIRAC.Time) to avoid jumps when there is a change in time
NEW_MAGIC_EPOCH_2K = 323322400

# # create logger
gLogger.initialize( "DMS", "/Databases/TransferDB/Test" )

class TransferDB( DB ):
  """
  .. class:: TransferDB

  This db is holding all information used by FTS systems.
  """

  def __init__( self, systemInstance = "Default", maxQueueSize = 10 ):
    """c'tor

    :param self: self reference
    :param str systemInstance: ???
    :param int maxQueueSize: size of queries queue
    """
    DB.__init__( self, "TransferDB", "RequestManagement/RequestDB", maxQueueSize )
    self.getIdLock = threading.Lock()
    # # max attmprt for reschedule
    self.maxAttempt = 100

  def __getFineTime( self ):
    """
      Return a "small" number of seconds with millisecond precision
    """
    return Time.to2K() - NEW_MAGIC_EPOCH_2K

  #################################################################################
  # These are the methods for managing the Channels table

  def createChannel( self, sourceSE, destSE ):
    """ create a new Channels record

    :param self: self reference
    :param str sourceSE: source SE
    :param str destSE: destination SE
    """
    self.getIdLock.acquire()
    res = self.checkChannelExists( sourceSE, destSE )
    if res['OK']:
      if res['Value']['Exists']:
        self.getIdLock.release()
        msg = 'TransferDB._createChannel: Channel %s already exists from %s to %s.' % ( res['Value']['ChannelID'],
                                                                                        sourceSE,
                                                                                        destSE )
        gLogger.debug( msg )
        return res
    req = "INSERT INTO Channels (SourceSite, DestinationSite, Status, ChannelName) VALUES ('%s','%s','%s','%s-%s');"
    req = req % ( sourceSE, destSE, 'Active', sourceSE, destSE )
    res = self._update( req )
    if not res['OK']:
      self.getIdLock.release()
      err = 'TransferDB._createChannel: Failed to create channel from %s to %s.' % ( sourceSE, destSE )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    self.getIdLock.release()
    res = self.checkChannelExists( sourceSE, destSE )
    return res

  def checkChannelExists( self, sourceSE, destSE ):
    """ check existence of FTS channel between :sourceSE: and :destSE:

    :param self: self reference
    :param str soucreSE: source SE
    :param str destSE: target SE
    """
    req = "SELECT ChannelID FROM Channels WHERE SourceSite = '%s' AND DestinationSite = '%s';" % ( sourceSE, destSE )
    res = self._query( req )
    if not res["OK"]:
      err = 'TransferDB._checkChannelExists: Failed to retrieve ChannelID for %s to %s.' % ( sourceSE, destSE )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    resultDict = { "Exists" : False }
    if res["Value"]:
      resultDict["Exists"] = True
      resultDict["ChannelID"] = res["Value"][0][0]
    return S_OK( resultDict )

  def getChannelID( self, sourceSE, destSE ):
    """ get Channels.ChannelID for given source and destination SE

    :param self: self reference
    :param str sourceSE: source SE
    :param str destSE: destination SE
    """
    res = self.checkChannelExists( sourceSE, destSE )
    if res['OK']:
      if res['Value']['Exists']:
        return S_OK( res['Value']['ChannelID'] )
      else:
        return S_ERROR( 'TransferDB._getChannelID: Channel from %s to %s does not exist.' % ( sourceSE, destSE ) )
    return res

  def getChannelAttribute( self, channelID, attrName ):
    """ select attribute :attrName: from Channels table given ChannelID

    :param self: self reference
    :param int channelID: Channels.ChannelID
    :param attrName: one of Channels table column name
    """
    req = "SELECT %s FROM Channels WHERE ChannelID = %s;" % ( attrName, channelID )
    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB._getChannelAttribute: Failed to get %s for Channel %s.' % ( attrName, channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if not res['Value']:
      err = 'TransferDB._getChannelAttribute: No Channel %s defined.' % channelID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return S_OK( res['Value'][0][0] )

  def setChannelAttribute( self, channelID, attrName, attrValue ):
    """ set Channels attribute :attrName: to new value :attrValue: given channelID

    :param self: self reference
    :param str attrName: one of Channels table column name
    :param mixed attrValue: new value to be set
    """
    req = "UPDATE Channels SET %s = '%s' WHERE ChannelID = %s;" % ( attrName, attrValue, channelID )
    res = self._update( req )
    if not res['OK']:
      err = 'TransferDB._setChannelAttribute: Failed to update %s to %s for Channel %s.' % ( attrName,
                                                                                             attrValue,
                                                                                             channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def getChannels( self ):
    """ read all records from Channels table

    :param self: self reference
    """
    req = "SELECT ChannelID,SourceSite,DestinationSite,Status,ChannelName from Channels;"
    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB._getChannels: Failed to retrieve channels information.'
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    channels = {}
    keyTuple = ( "Source", "Destination", "Status", "ChannelName" )
    for record in res['Value']:
      channelID = record[0]
      channelTuple = record[1:]
      channels[channelID] = dict( zip( keyTuple, channelTuple ) )
    return S_OK( channels )

  def getChannelsForState( self, status ):
    """ select Channels records for Status :status:

    :param self: self reference
    :param str status: required Channels.Status
    """
    req = "SELECT ChannelID,SourceSite,DestinationSite FROM Channels WHERE Status = '%s';" % status
    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB._getChannelsInState: Failed to get Channels for Status = %s.' % status
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if not res['Value']:
      return S_OK()
    channels = {}
    channelIDs = []
    for channelID, sourceSite, destinationSite in res['Value']:
      channels[channelID] = { 'SourceSite' : sourceSite, 'DestinationSite' : destinationSite }
      channelIDs.append( channelID )
    return S_OK( { 'ChannelIDs' : channelIDs, 'Channels' : channels } )

  def decreaseChannelFiles( self, channelID ):
    """ decrease Channels.Files for given Channels.ChannelID

    :param self: self reference
    :param int channelID: Channels.ChannelID
    """
    req = "UPDATE Channels SET Files = Files-1 WHERE ChannelID = %s;" % channelID
    res = self._update( req )
    if not res['OK']:
      err = 'TransferDB.decreaseChannelFiles: Failed to update Files for Channel %s.' % channelID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def increaseChannelFiles( self, channelID ):
    """ increase Channels.Files for given Channels.ChannelID

    :param self: self reference
    :param int channelID: Channels.ChannelID
    """
    req = "UPDATE Channels SET Files = Files+1 WHERE ChannelID = %s;" % channelID
    res = self._update( req )
    if not res['OK']:
      err = 'TransferDB.increaseChannelFiles: Failed to update Files for Channel %s.' % channelID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  #################################################################################
  # These are the methods for managing the Channel table

  def selectChannelsForSubmission( self, maxJobsPerChannel ):
    """ select active channels

    :param self: self reference
    :param int maxJobsPerChannel: max number of simultaneous FTS transfers for channel
    """
    res = self.getChannelQueues( status = 'Waiting' )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK()
    channels = res['Value']
    candidateChannels = {}
    for channelID in channels:
      if channels[channelID]['Status'] == 'Active':
        if channels[channelID]['Files'] > 0:
          candidateChannels[channelID] = channels[channelID]['Files']
    if not candidateChannels:
      return S_OK()

    strChannelIDs = intListToString( candidateChannels.keys() )
    req = "SELECT ChannelID,%s-SUM(Status='Submitted') FROM FTSReq WHERE ChannelID IN (%s) GROUP BY ChannelID;" % ( maxJobsPerChannel, strChannelIDs )
    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB.selectChannelsForSubmission: Failed to count FTSJobs on Channels %s.' % strChannelIDs
      return S_ERROR( err )

    channelJobs = {}
    for channelID, jobs in res['Value']:
      channelJobs[channelID] = jobs
    for channelID in candidateChannels:
      if channelID not in channelJobs:
        channelJobs[channelID] = maxJobsPerChannel

    req = "SELECT ChannelID,SourceSite,DestinationSite,FTSServer,Files FROM Channels WHERE ChannelID IN (%s);" % strChannelIDs
    res = self._query( req )

    channels = []
    keyTuple = ( "ChannelID", "Source", "Destination", "FTSServer", "NumFiles" )
    for recordTuple in res['Value']:
      resDict = dict( zip( keyTuple, recordTuple ) )
      for i in range( channelJobs[channelID] ):
        channels.append( resDict )
    return S_OK( channels )

  def selectChannelForSubmission( self, maxJobsPerChannel ):
    """ select one channel for submission

    :param self: self reference
    :param int maxJobsPerChannel: max nb of simultanious FTS requests
    """
    res = self.getChannelQueues( status = 'Waiting' )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK()
    channels = res['Value']
    candidateChannels = {}
    for channelID in channels:
      if channels[channelID]['Status'] == 'Active':
        if channels[channelID]['Files'] > 0:
          candidateChannels[channelID] = channels[channelID]['Files']
    if not candidateChannels:
      return S_OK()

    strChannelIDs = intListToString( candidateChannels.keys() )
    req = "SELECT ChannelID,%s-SUM(Status='Submitted') FROM FTSReq WHERE ChannelID IN (%s) GROUP BY ChannelID;"
    req = req % ( maxJobsPerChannel, strChannelIDs )
    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB._selectChannelForSubmission: Failed to count FTSJobs on Channels %s.' % strChannelIDs
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )

    # # c'tor using a tuple of pairs ;)
    withJobs = dict( res["Value"] )

    minJobs = maxJobsPerChannel
    maxFiles = 0
    possibleChannels = []
    for channelID, files in candidateChannels.items():
      numberOfJobs = withJobs[channelID] if channelID in withJobs else 0
      if numberOfJobs < maxJobsPerChannel:
        if numberOfJobs < minJobs:
          minJobs = numberOfJobs
          maxFiles = files
          possibleChannels.append( channelID )
        elif numberOfJobs == minJobs:
          if files > maxFiles:
            maxFiles = files
            possibleChannels = []
            possibleChannels.append( channelID )
          elif candidateChannels[channelID] == maxFiles:
            possibleChannels.append( channelID )
    if not possibleChannels:
      return S_OK()

    selectedChannel = random.choice( possibleChannels )  # randomize(possibleChannels)[0]
    resDict = channels[selectedChannel]
    resDict['ChannelID'] = selectedChannel
    return S_OK( resDict )

  def addFileToChannel( self,
                        channelID,
                        fileID,
                        sourceSE,
                        sourceSURL,
                        targetSE,
                        targetSURL,
                        fileSize,
                        fileStatus = 'Waiting' ):
    """ insert new Channel record

    :param self: self reference
    :param int channelID: Channels.ChannelID
    :param int fileID: Files.FileID
    :param str sourceSE: source SE
    :param str sourceSURL: source storage URL
    :param str targetSE: destination SE
    :param str targetSURL: destination storage URL
    :param int fileSize: file size in bytes
    :param str fileStatus: Channel.Status
    """
    res = self.checkFileChannelExists( channelID, fileID )
    if not res['OK']:
      err = 'TransferDB.addFileToChannel: Failed check existance of File %s on Channel %s.' % ( fileID, channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if res['Value']:
      err = 'TransferDB.addFileToChannel: File %s already exists on Channel %s.' % ( fileID, channelID )
      return S_ERROR( err )
    time_order = self.__getFineTime()
    values = "%s,%s,'%s','%s','%s','%s',UTC_TIMESTAMP(),%s,UTC_TIMESTAMP(),%s,%s,'%s'" % ( channelID, fileID,
                                                                                           sourceSE, sourceSURL,
                                                                                           targetSE, targetSURL,
                                                                                           time_order, time_order,
                                                                                           fileSize, fileStatus )
    columns = ",".join( [ "ChannelID" , "FileID",
                          "SourceSE", "SourceSURL",
                          "TargetSE", "TargetSURL",
                          "SchedulingTime" , " SchedulingTimeOrder",
                          "LastUpdate", "LastUpdateTimeOrder",
                          "FileSize", "Status" ] )

    req = "INSERT INTO Channel (%s) VALUES (%s);" % ( columns, values )
    res = self._update( req )
    if not res['OK']:
      err = 'TransferDB.addFileToChannel: Failed to insert File %s to Channel %s.' % ( fileID, channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def checkFileChannelExists( self, channelID, fileID ):
    """ check if record with :channelID: and :fileID: has been already put into Channel table

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param int fileID: Files.FileID
    """

    req = "SELECT FileID FROM Channel WHERE ChannelID = %s and FileID = %s;" % ( channelID, fileID )
    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB.checkFileChannelExists: Failed to check existance of File %s on Channel %s.' % ( fileID,
                                                                                                          channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if res['Value']:
      return S_OK( True )
    return S_OK( False )

  def setChannelFilesExecuting( self, channelID, fileIDs ):
    """ update Channel.Status to 'Executing' given :channelID: and list of :fileID:

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param list fileIDs: list of Channel.FileID
    """
    strFileIDs = intListToString( fileIDs )
    time_order = self.__getFineTime()
    req = "UPDATE Channel SET Status='Executing', LastUpdate=UTC_TIMESTAMP(), " \
        "LastUpdateTimeOrder=%s WHERE FileID IN (%s) AND ChannelID = %s;" % ( time_order,
                                                                              strFileIDs,
                                                                              channelID )
    res = self._update( req )
    if not res['OK']:
      err = 'TransferDB.setChannelFilesExecuting: Failed to set file executing.'
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def updateAncestorChannelStatus( self, channelID, fileIDs ):
    """ update Channel.Status

          WaitingN => Waiting
          DoneN => Done

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param list fileIDs: list of Files.FileID
    """
    if not fileIDs:
      return S_OK()
    strFileIDs = intListToString( fileIDs )
    req = "UPDATE Channel SET Status = "
    req += "CASE WHEN Status = 'Waiting%s' THEN 'Waiting' WHEN Status = 'Done%s' THEN 'Done' END " % ( channelID,
                                                                                                       channelID )
    req += "WHERE FileID IN (%s) AND ( Status = 'Waiting%s' OR Status = 'Done%s');" % ( strFileIDs,
                                                                                        channelID,
                                                                                        channelID )
    res = self._update( req )
    if not res['OK']:
      err = "TransferDB.updateAncestorChannelStatus: Failed to update status"
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def removeFilesFromChannel( self, channelID, fileIDs ):
    """ remove Files from Channel given list of FileIDs and ChannelID

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param list fileIDs: list of Files.FileID
    """
    for fileID in fileIDs:
      res = self.removeFileFromChannel( channelID, fileID )
      if not res['OK']:
        return res
    return res

  def setFileToReschedule( self, fileID ):
    """ allow reschedule for file

    :param int fileID: Files.FileID
    """
    req = "SELECT `Attempt` FROM `Files` WHERE FileID = %s;" % fileID
    res = self._update( req )
    if not res["OK"]:
      gLogger.error( "setFileToReschedule: %s" % res["Message"] )
      return res
    res = res["Value"]
    if res > self.maxAttempt:
      return S_OK( "max reschedule attempt reached" )

    req = "DELETE FROM `Channel` WHERE `FileID` = %s;" % fileID
    res = self._update( req )
    if not res["OK"]:
      gLogger.error( "setFileToReschedule: %s" % res["Message"] )
      return res
    req = "DELETE FROM `ReplicationTree` WHERE `FileID` = %s;" % fileID
    res = self._update( req )
    if not res["OK"]:
      gLogger.error( "setFileToReschedule: %s" % res["Message"] )
      return res
    req = "DELETE FROM `FileToCat` WHERE `FileID` = %s;" % fileID
    res = self._update( req )
    if not res["OK"]:
      gLogger.error( "setFileToReschedule: %s" % res["Message"] )
      return res
    req = "UPDATE `Files` SET `Status`='Waiting',`Attempt`=`Attempt`+1 WHERE `Status` = 'Scheduled' AND  `FileID` = %s;" % fileID
    res = self._update( req )
    if not res["OK"]:
      gLogger.error( "setFileToReschedule: %s" % res["Message"] )
      return res
    return S_OK()


  def removeFileFromChannel( self, channelID, fileID ):
    """ remove single file from Channel given FileID and ChannelID

    :param self: self refernce
    :param int channelID: Channel.ChannelID
    :param int FileID: Files.FileID
    """
    req = "DELETE FROM Channel WHERE ChannelID = %s and FileID = %s;" % ( channelID, fileID )
    res = self._update( req )
    if not res['OK']:
      err = 'TransferDB._removeFileFromChannel: Failed to remove File %s from Channel %s.' % ( fileID, channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def updateCompletedChannelStatus( self, channelID, fileIDs ):
    """ update Channel.Status and Files.Status to 'Done' given :channelID: and list of :fileIDs:

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param list fileIDs: list of Files.FileID
    """
    time_order = self.__getFineTime()
    strFileIDs = intListToString( fileIDs )
    req = "SELECT FileID,Status,COUNT(*) FROM Channel WHERE FileID IN (%s) GROUP BY FileID,Status;" % strFileIDs
    res = self._query( req )
    if not res['OK']:
      return res
    fileDict = {}
    for fileID, status, count in res['Value']:
      if fileID not in fileDict:
        fileDict[fileID] = 0
      if status != 'Done':
        fileDict[fileID] += count
    toUpdate = [ fileID for fileID, notDone in fileDict.items() if notDone == 1 ]
    if toUpdate:
      req = "UPDATE Files SET Status = 'Done' WHERE FileID IN (%s);" % intListToString( toUpdate )
      res = self._update( req )
      if not res['OK']:
        return res
      req = "UPDATE Channel SET Status = 'Done',LastUpdate=UTC_TIMESTAMP(),LastUpdateTimeOrder=%s" \
          ",CompletionTime=UTC_TIMESTAMP() WHERE FileID IN (%s) AND ChannelID = %s;" % ( time_order,
                                                                                         strFileIDs,
                                                                                         channelID )
    res = self._update( req )
    if not res['OK']:
      err = 'TransferDB.updateCompletedChannelStatus: Failed to update %s files from Channel %s.' % ( len( fileIDs ),
                                                                                                       channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def resetFileChannelStatus( self, channelID, fileIDs ):
    """ set Channel.Status to 'Waiting' given :channelID: and list of :fileIDs:

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param list fileIDs: list of Files.FileID
    """
    time_order = self.__getFineTime()
    req = "UPDATE Channel SET Status = 'Waiting',LastUpdate=UTC_TIMESTAMP(),LastUpdateTimeOrder=%s," \
        "Retries=Retries+1 WHERE FileID IN (%s) AND ChannelID = %s;" % ( time_order,
                                                                         intListToString( fileIDs ),
                                                                         channelID )
    res = self._update( req )
    if not res['OK']:
      err = 'TransferDB.resetFileChannelStatus: Failed to reset %s files from Channel %s.' % ( len( fileIDs ),
                                                                                               channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def getFileChannelAttribute( self, channelID, fileID, attribute ):
    """ select column :attribute: from Channel table given :channelID: and :fileID:

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param int fileID: Channel.FileID
    :param atr attribute: column name
    """
    req = "SELECT %s from Channel WHERE ChannelID = %s and FileID = %s;" % ( attribute, channelID, fileID )
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB.getFileChannelAttribute: Failed to get %s for File %s on Channel %s." % ( attribute,
                                                                                                  fileID,
                                                                                                  channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if not res['Value']:
      err = "TransferDB.getFileChannelAttribute: File %s doesn't exist on Channel %s." % ( fileID, channelID )
      return S_ERROR( err )
    attrValue = res['Value'][0][0]
    return S_OK( attrValue )

  def setFileChannelStatus( self, channelID, fileID, status ):
    """ set Channel.Status to :status:, if :status: is 'Failed', it will also set
    Files.Status to it

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param int fileID: Files.FileID
    :param str status: new value for Channel.Status
    """
    if status == 'Failed':
      req = "UPDATE Files SET Status = 'Failed' WHERE FileID = %d" % fileID
      res = self._update( req )
      if not res['OK']:
        return res
    res = self.setFileChannelAttribute( channelID, fileID, 'Status', status )
    return res

  def setFileChannelAttribute( self, channelID, fileID, attribute, attrValue ):
    """ update :attribute: in Channel table to value :attrValue: given :channelID: and :fileID:

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param int fileID: Files.FileID
    :param str attribute: Channel table column name
    :param mixed attrValue: new value for :attribute:
    """
    if type( fileID ) == ListType:
      fileIDs = fileID
    else:
      fileIDs = [fileID]
    time_order = self.__getFineTime()
    req = "UPDATE Channel SET %s = '%s',LastUpdate=UTC_TIMESTAMP(),LastUpdateTimeOrder=%s " \
        "WHERE ChannelID=%s and FileID IN (%s);" % ( attribute, attrValue, time_order,
                                                      channelID, intListToString( fileIDs ) )
    res = self._update( req )
    if not res['OK']:
      err = 'TransferDB._setFileChannelAttribute: Failed to set %s to %s for %s files on Channel %s.' % ( attribute,
                                                                                                          attrValue,
                                                                                                          len( fileIDs ),
                                                                                                          channelID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def getFilesForChannel( self, channelID, numberOfFiles, status = 'Waiting', sourceSE = None, targetSE = None ):
    """ This method will only return Files for the oldest SourceSE,TargetSE Waiting for a given Channel.
    """
    # req = "SELECT SourceSE,TargetSE FROM Channel WHERE ChannelID = %s AND Status = 'Waiting'
    # ORDER BY Retries, LastUpdateTimeOrder LIMIT 1;" % (channelID)
    if ( sourceSE and not targetSE ) or ( targetSE and not sourceSE ):
      return S_ERROR( 'Both source and target SEs should be supplied' )
    if not sourceSE and not targetSE:
      req = "SELECT c.SourceSE,c.TargetSE FROM Channel as c,Files as f WHERE c.ChannelID=%s AND " \
          "c.Status='%s' AND c.FileID=f.FileID ORDER BY c.Retries,c.LastUpdateTimeOrder LIMIT 1;" % ( channelID, status )
      res = self._query( req )
      if not res['OK']:
        err = "TransferDB.getFilesForChannel: Failed to get files for Channel %s." % channelID
        return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
      if not res['Value']:
        return S_OK()
      sourceSE, targetSE = res['Value'][0]
    req = "SELECT c.FileID,c.SourceSURL,c.TargetSURL,c.FileSize,f.LFN FROM Files AS f, Channel AS c " \
        "WHERE c.ChannelID=%s AND c.FileID=f.FileID AND c.Status ='%s' AND c.SourceSE='%s' " \
        "AND c.TargetSE='%s' ORDER BY c.Retries,c.LastUpdateTimeOrder LIMIT %s;" % \
        ( channelID, status, sourceSE, targetSE, numberOfFiles )
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB.getFilesForChannel: Failed to get files for Channel %s." % channelID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if not res['Value']:
      return S_OK()

    keysTuple = ( "FileID", "SourceSURL", "TargetSURL", "Size", "LFN" )
    resDict = { "SourceSE" : sourceSE,
                "TargetSE" : targetSE,
                "Files" : [ dict( zip( keysTuple, recordTuple ) ) for recordTuple in res["Value"] ] }
    return S_OK( resDict )

  def getChannelQueues( self, status = None ):
    """ get Channel queues given Channel.Status :status:

    if :status: is None, this will pick up all 'Waiting%'-like Channel records

    :return: S_OK( { channelID : { "Files" : nbOfFiles, "Size" : sumOfFileSizes }, ... } )

    :param self: self reference
    :param str status: Channel.Status
    """
    res = self.getChannels()
    if not res['OK']:
      return res
    channels = res['Value']
    channelIDs = channels.keys()
    if status:
      req = "SELECT ChannelID,COUNT(*),SUM(FileSize) FROM Channel WHERE Status = '%s' GROUP BY ChannelID;" % ( status )
    else:
      req = "SELECT ChannelID,COUNT(*),SUM(FileSize) FROM Channel WHERE Status LIKE 'Waiting%' GROUP BY ChannelID;"
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB.getChannelQueues: Failed to get Channel contents for Channels."
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    for channelID, fileCount, sizeCount in res['Value']:
      channels[channelID]['Files'] = int( fileCount )
      channels[channelID]['Size'] = int( sizeCount )
    for channelID in channelIDs:
      if "Files" not in channels[channelID]:
        channels[channelID]['Files'] = 0
        channels[channelID]['Size'] = 0
    return S_OK( channels )


  def getCompletedChannels( self, limit = 100 ):
    """ get list of completed channels

    :param int limit: select limit
    """
    query = "SELECT DISTINCT FileID FROM Channel where Status = 'Done' AND FileID NOT IN ( SELECT FileID from Files ) LIMIT %s;" % limit
    return self._query( query )


  #################################################################################
  # These are the methods for managing the FTSReq table

  def insertFTSReq( self, ftsGUID, ftsServer, channelID ):
    """ insert new FTSReq record

    :return: S_OK( FTSReq.FTSReqID )

    :param self: self reference
    :param str ftsGUID: GUID returned from glite-submit-transfer
    :param str ftsServer: FTS server URI
    :param int channelID: Channel.ChannelID
    """
    self.getIdLock.acquire()
    req = "INSERT INTO FTSReq (FTSGUID,FTSServer,ChannelID,SubmitTime,LastMonitor) " \
        "VALUES ('%s','%s',%s,UTC_TIMESTAMP(),UTC_TIMESTAMP());" % ( ftsGUID, ftsServer, channelID )
    res = self._update( req )
    if not res['OK']:
      self.getIdLock.release()
      err = "TransferDB._insertFTSReq: Failed to insert FTS GUID into FTSReq table."
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    req = "SELECT MAX(FTSReqID) FROM FTSReq;"
    res = self._query( req )
    self.getIdLock.release()
    if not res['OK']:
      err = "TransferDB._insertFTSReq: Failed to get FTSReqID from FTSReq table."
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if not res['Value']:
      err = "TransferDB._insertFTSReq: Request details don't appear in FTSReq table."
      return S_ERROR( err )
    ftsReqID = res['Value'][0][0]
    return S_OK( ftsReqID )

  def setFTSReqStatus( self, ftsReqID, status ):
    """ update FTSReq.Status to :status: given FTSReq.FTSReqID

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    :param str status: new status
    """
    self.getIdLock.acquire()
    req = "UPDATE FTSReq SET Status = '%s' WHERE FTSReqID = %s;" % ( status, ftsReqID )
    res = self._update( req )
    self.getIdLock.release()
    if not res['OK']:
      err = "TransferDB._setFTSReqStatus: Failed to set status to %s for FTSReq %s." % ( status, ftsReqID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def deleteFTSReq( self, ftsReqID ):
    """ delete FTSReq record given FTSReq.FTSReqID

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    """
    self.getIdLock.acquire()
    req = "DELETE FROM FTSReq WHERE FTSReqID = %s;" % ( ftsReqID )
    res = self._update( req )
    self.getIdLock.release()
    if not res['OK']:
      err = "TransferDB._deleteFTSReq: Failed to delete FTSReq %s." % ftsReqID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def getFTSReq( self ):
    """ select 'Submitted' FTSReq

    :param self: self reference
    """
    # req = "SELECT f.FTSReqID,f.FTSGUID,f.FTSServer,f.ChannelID,f.SubmitTime,f.NumberOfFiles,f.TotalSize,c.SourceSite,
    # c.DestinationSite FROM FTSReq as f,Channels as c WHERE f.Status = 'Submitted' and
    # f.ChannelID=c.ChannelID ORDER BY f.LastMonitor;"

    req = "SELECT FTSReqID,FTSGUID,FTSServer,ChannelID,SubmitTime,SourceSE,TargetSE,NumberOfFiles,TotalSize" \
        " FROM FTSReq WHERE Status = 'Submitted' ORDER BY LastMonitor;"
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB._getFTSReq: Failed to get entry from FTSReq table."
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if not res['Value']:
      # It is not an error that there are not requests
      return S_OK()

    keysTuple = ( "FTSReqID", "FTSGuid", "FTSServer", "ChannelID",
                  "SubmitTime", "SourceSE", "TargetSE", "NumberOfFiles", "TotalSize" )
    ftsReqs = [ dict( zip( keysTuple, recordTuple ) ) for recordTuple in res["Value"] ]
    return S_OK( ftsReqs )

  def setFTSReqAttribute( self, ftsReqID, attribute, attrValue ):
    """ set :attribute: column in FTSReq table to :attValue: given :ftsReqID:

    :param self: slf reference
    :param int ftsReqID: FTSReq.FTSReqID
    :param str attribute: FTSReq column name
    :param mixed attrValue: new value
    """
    self.getIdLock.acquire()
    req = "UPDATE FTSReq SET %s = '%s' WHERE FTSReqID = %s;" % ( attribute, attrValue, ftsReqID )
    res = self._update( req )
    self.getIdLock.release()
    if not res['OK']:
      err = "TransferDB._setFTSReqAttribute: Failed to set %s to %s for FTSReq %s." % ( attribute, attrValue, ftsReqID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def setFTSReqLastMonitor( self, ftsReqID ):
    """ update FSTReq.LastMonitor timestamp for given :ftsReqID:

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    """
    req = "UPDATE FTSReq SET LastMonitor = UTC_TIMESTAMP() WHERE FTSReqID = %s;" % ftsReqID
    res = self._update( req )
    if not res['OK']:
      err = "TransferDB._setFTSReqLastMonitor: Failed to update monitoring time for FTSReq %s." % ftsReqID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  #################################################################################
  # These are the methods for managing the FileToFTS table

  def getFTSReqLFNs( self, ftsReqID, channelID = None, sourceSE = None ):
    """ collect LFNs for files in FTSReq

    :warning: if Files records are missing, a new artificial ones would be inserted
    using SubRequests.SubRequestID = 0, this could happen only if original request
    had been removed

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    :param int channelID: Channel.ChannelID
    :param str sourceSE: source SE
    """
    req = "SELECT ftf.FileID,f.LFN from FileToFTS as ftf LEFT JOIN Files as f " \
        "ON (ftf.FileID=f.FileID) WHERE ftf.FTSReqID = %s;" % ftsReqID
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB.getFTSReqLFNs: Failed to get LFNs for FTSReq %s." % ftsReqID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if not res['Value']:
      err = "TransferDB.getFTSReqLFNs: No LFNs found for FTSReq %s." % ftsReqID
      return S_ERROR( err )
    # # list of missing fileIDs
    missingFiles = []
    files = {}
    for fileID, lfn in res['Value']:
      if lfn:
        files[lfn] = fileID
      else:
        error = "TransferDB.getFTSReqLFNs: File %s does not exist in the Files table." % fileID
        gLogger.warn( error )
        missingFiles.append( fileID )

    # # failover  mechnism for removed Requests
    if missingFiles:
      # # no channelID or sourceSE --> return S_ERROR
      if not channelID and not sourceSE:
        return S_ERROR( "TransferDB.getFTSReqLFNs: missing records in Files table: %s" % missingFiles )
      # # create storage element
      sourceSE = StorageElement( sourceSE )
      # # get FileID, SourceSURL pairs for missing FileIDs and channelID used in this FTSReq
      strMissing = intListToString( missingFiles )
      query = "SELECT FileID,SourceSURL FROM Channel WHERE ChannelID=%s and FileID in (%s);" % ( channelID,
                                                                                                 strMissing )
      query = self._query( query )
      if not query["OK"]:
        gLogger.error( "TransferDB.getFSTReqLFNs: unable to select PFNs for missing Files: %s" % query["Message"] )
        return query
      # # guess LFN from StorageElement, prepare query for inserting records, save lfn in files dict
      insertTemplate = "INSERT INTO Files (SubRequestID, FileID, LFN, Status) VALUES (0, %s, %s, 'Scheduled');"
      insertQuery = []
      for fileID, pfn in query["Value"]:
        lfn = sourceSE.getPfnPath( pfn )
        if not lfn["OK"]:
          gLogger.error( "TransferDB.getFTSReqLFNs: %s" % lfn["Message"] )
          return lfn
        lfn = lfn["Value"]
        files[lfn] = fileID
        insertQuery.append( insertTemplate % ( fileID, lfn ) )
      # # insert missing 'fake' records
      if insertQuery:
        ins = self._update( "\n".join( insertQuery ) )
        if not ins["OK"]:
          gLogger.error( "TransferDB.getFTSReqLFNs: unable to insert fake Files for missing LFNs: %s" % ins["Message"] )
          return ins

    # # return files dict
    return S_OK( files )

  def setFTSReqFiles( self, ftsReqID, channelID, fileAttributes ):
    """ insert FileToFTS records for given :ftsReqID: and :channelID:

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    :param int channelID: Channel.ChannelID
    :param list fileAttributes: [ (fileID, fileSize), ... ]
    """
    for fileID, fileSize in fileAttributes:
      req = "INSERT INTO FileToFTS (FTSReqID,FileID,ChannelID,SubmissionTime,FileSize)" \
          " VALUES (%s,%s,%s,UTC_TIMESTAMP(),%s);" % ( ftsReqID, fileID, channelID, fileSize )
      res = self._update( req )
      if not res['OK']:
        err = "TransferDB._setFTSReqFiles: Failed to set File %s for FTSReq %s." % ( fileID, ftsReqID )
        return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return S_OK()

  def getFTSReqFileIDs( self, ftsReqID ):
    """ read FileToFTS.FileID for given :ftsReqID:

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    """
    req = "SELECT FileID FROM FileToFTS WHERE FTSReqID = %s;" % ftsReqID
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB._getFTSReqFileIDs: Failed to get FileIDs for FTSReq %s." % ftsReqID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if not res['Value']:
      err = "TransferDB._getFTSReqLFNs: No FileIDs found for FTSReq %s." % ftsReqID
      return S_ERROR( err )
    fileIDs = [ fileID[0] for fileID in res["Value"] ]
    return S_OK( fileIDs )

  def getSizeOfCompletedFiles( self, ftsReqID, completedFileIDs ):
    """ select size of transferred files in FTSRequest given :ftsReqID: and list of :completedFilesIDs:

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    :param list completedFileIDs: list of Files.FileID
    """
    strCompleted = intListToString( completedFileIDs )
    req = "SELECT SUM(FileSize) FROM FileToFTS where FTSReqID = %s AND FileID IN (%s);" % ( ftsReqID, strCompleted )
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB._getSizeOfCompletedFiles: Failed to get successful transfer size for FTSReq %s." % ftsReqID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return S_OK( res['Value'][0][0] )

  def removeFilesFromFTSReq( self, ftsReqID ):
    """ delete all FileToFST records for given :ftsReqID:

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    """
    req = "DELETE FROM FileToFTS WHERE FTSReqID = %s;" % ftsReqID
    res = self._update( req )
    if not res['OK']:
      err = "TransferDB._removeFilesFromFTSReq: Failed to remove files for FTSReq %s." % ftsReqID
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def setFileToFTSFileAttributes( self, ftsReqID, channelID, fileAttributeTuples ):
    """ update FileToFTS records for given :ftsReqID: and :channeID:

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    :param int channelID: Channel.ChannelID
    :param list fileAttributeTuples: [ ( fileID, status, reason, retries, transferDuration ), ... ]
    """
    for fileID, status, reason, retries, transferTime in fileAttributeTuples:
      req = "UPDATE FileToFTS SET Status ='%s',Duration=%s,Reason='%s',Retries=%s,TerminalTime=UTC_TIMESTAMP() " \
          "WHERE FileID=%s AND FTSReqID=%s AND ChannelID=%s;" % \
          ( status, transferTime, reason, retries, fileID, ftsReqID, channelID )
      res = self._update( req )
      if not res['OK']:
        err = "TransferDB._setFileToFTSFileAttributes: Failed to set file attributes for FTSReq %s." % ftsReqID
        return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def setFileToFTSFileAttribute( self, ftsReqID, fileID, attribute, attrValue ):
    """ update FileToFTS.:attribute: to :attrValue: for given :ftsReqID: and :fileID:

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    :param int fileID: Files.FileID
    :param str attribute: FileToFTS column name
    :param mixed attrValue: new value
    """
    req = "UPDATE FileToFTS SET %s = '%s' WHERE FTSReqID = %s AND FileID = %s;" % ( attribute,
                                                                                    attrValue,
                                                                                    ftsReqID,
                                                                                    fileID )
    res = self._update( req )
    if not res['OK']:
      err = "TransferDB._setFileToFTSFileAttribute: Failed to set %s to %s for File %s and FTSReq %s;" % ( attribute,
                                                                                                           attrValue,
                                                                                                           fileID,
                                                                                                           ftsReqID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def setFileToFTSTerminalTime( self, ftsReqID, fileID ):
    """ update FileToFTS.TerminalTime timestamp for given :ftsReqID: and :fileID:

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    :param int fileID: Files.FileID
    """
    req = "UPDATE FileToFTS SET TerminalTime=UTC_TIMESTAMP() WHERE FTSReqID=%s AND FileID=%s;" % ( ftsReqID, fileID )
    res = self._update( req )
    if not res['OK']:
      err = "TransferDB._setFileToFTSTerminalTime: Failed to set terminal time for File %s and FTSReq %s;" % \
          ( fileID, ftsReqID )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    return res

  def getCountFileToFTS( self, interval = 3600, status = "Failed" ):
    """ get count of distinct FileIDs per Channel for Failed FileToFTS

    :param self: self reference
    :param str status: FileToFTS.Status
    :param int interval: time period in seconds

    :return: S_OK( { FileToFTS.ChannelID : int } )
    """
    channels = self.getChannels()
    if not channels["OK"]:
      return channels
    channels = channels["Value"]
    # # this we're going to return
    channelDict = dict.fromkeys( channels.keys(), 0 )
    # # query
    query = "SELECT ChannelID, COUNT(DISTINCT FileID) FROM FileToFTS WHERE Status='%s' AND " \
        "SubmissionTime > (UTC_TIMESTAMP() - INTERVAL %s SECOND) GROUP BY ChannelID;" % ( status, interval )
    # # query query to query :)
    query = self._query( query )
    if not query["OK"]:
      return S_ERROR( "TransferDB.getCountFailedFTSFiles: " % query["Message"] )
    # # return dict updated by dict created from query tuple :)
    channelDict.update( dict( query["Value"] ) )
    return S_OK( channelDict )

  def getChannelObservedThroughput( self, interval ):
    """ create and return a dict holding summary info about FTS channels
    and related transfers in last :interval: seconds

    :return: S_OK( { channelID : { "Throughput" : float,
                                   "Fileput" : float,
                                   "SuccessfulFiles" : int,
                                   "FailedFiles" : int
                                  }, ... } )

    :param self: self reference
    :param int interval: monitoring interval in seconds
    """
    channels = self.getChannels()
    if not channels["OK"]:
      return channels
    channels = channels['Value']

    # # create empty channelDict
    channelDict = dict.fromkeys( channels.keys(), None )
    # # fill with zeros
    for channelID in channelDict:
      channelDict[channelID] = {}
      channelDict[channelID]["Throughput"] = 0
      channelDict[channelID]["Fileput"] = 0
      channelDict[channelID]["SuccessfulFiles"] = 0
      channelDict[channelID]["FailedFiles"] = 0

    channelTimeDict = dict.fromkeys( channels.keys(), 0 )

    req = "SELECT ChannelID, Status, Count(*), SUM(FileSize), SUM(TimeDiff) FROM " \
          "( SELECT ChannelID, Status,TIME_TO_SEC( TIMEDIFF( TerminalTime, SubmissionTime ) ) " \
          "AS TimeDiff ,FileSize FROM FileToFTS WHERE Status in ('Completed', 'Failed') " \
          "AND SubmissionTime > (UTC_TIMESTAMP() - INTERVAL %s SECOND) ) " \
          "AS T GROUP BY ChannelID, Status;" % interval

    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB.getChannelObservedThroughput: Failed to transfer Statistics.'
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )

    for channelID, status, files, data, totalTime in res['Value']:
      channelTimeDict[channelID] += float( totalTime )
      if status == 'Completed':
        channelDict[channelID]['Throughput'] = float( data )
        channelDict[channelID]['SuccessfulFiles'] = int( files )
      else:
        channelDict[channelID]['FailedFiles'] = int( files )

    for channelID in channelDict.keys():
      if channelTimeDict[channelID]:
        channelDict[channelID]['Throughput'] = channelDict[channelID]['Throughput'] / channelTimeDict[channelID]
        channelDict[channelID]['Fileput'] = channelDict[channelID]['SuccessfulFiles'] / channelTimeDict[channelID]

    return S_OK( channelDict )

    #############################################
    # First get the total time spend transferring files on the channels
    req = "SELECT ChannelID,SUM(TIME_TO_SEC(TIMEDIFF(TerminalTime,SubmissionTime))) FROM FileToFTS " \
        "WHERE Status IN ('Completed','Failed') AND SubmissionTime > (UTC_TIMESTAMP() - INTERVAL %s SECOND) " \
        "GROUP BY ChannelID;" % interval
    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB._getFTSObservedThroughput: Failed to obtain total time transferring.'
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )

    channelTimeDict = dict.fromkeys( channels.keys(), None )
    for channelID, totalTime in res['Value']:
      channelTimeDict[channelID] = float( totalTime )

    #############################################
    # Now get the total size of the data transferred and the number of files that were successful
    req = "SELECT ChannelID,SUM(FileSize),COUNT(*) FROM FileToFTS WHERE Status='Completed' AND " \
        "SubmissionTime > (UTC_TIMESTAMP() - INTERVAL %s SECOND) GROUP BY ChannelID;" % interval
    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB._getFTSObservedThroughput: Failed to obtain total transferred data and files.'
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )

    for channelID, data, files in res['Value']:
      if channelID in channelTimeDict and channelTimeDict[channelID]:
        channelDict[channelID] = { 'Throughput': float( data ) / channelTimeDict[channelID],
                                   'Fileput': float( files ) / channelTimeDict[channelID] }

    #############################################
    # Now get the success rate on the channels
    req = "SELECT ChannelID,SUM(Status='Completed'),SUM(Status='Failed') from FileToFTS WHERE " \
        "SubmissionTime > (UTC_TIMESTAMP() - INTERVAL %s SECOND) GROUP BY ChannelID;" % ( interval )
    res = self._query( req )
    if not res['OK']:
      err = 'TransferDB._getFTSObservedThroughput: Failed to obtain success rate.'
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )

    for channelID, successful, failed in res['Value']:
      channelDict[channelID]['SuccessfulFiles'] = int( successful )
      channelDict[channelID]['FailedFiles'] = int( failed )

    return S_OK( channelDict )

  def getTransferDurations( self, channelID, startTime = None, endTime = None ):
    """ This obtains the duration of the successful transfers on the supplied channel
    """
    req = "SELECT Duration FROM FileToFTS WHERE ChannelID = %s and Duration > 0" % channelID
    if startTime:
      req = "%s AND SubmissionTime > '%s'" % req
    if endTime:
      req = "%s AND SubmissionTime < '%s'" % req
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB.getTransferDurations: Failed to obtain durations from FileToFTS"
      return S_ERROR( err )
    durations = []
    for value in res['Value']:
      durations.append( int( value[0] ) )
    return S_OK( durations )

  #################################################################################
  # These are the methods for managing the FTSReqLogging table

  def addLoggingEvent( self, ftsReqID, event ):
    """ insert new FTSReqLogging :event:

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    :param str event: new event
    """
    req = "INSERT INTO FTSReqLogging (FTSReqID,Event,EventDateTime) VALUES (%s,'%s',UTC_TIMESTAMP());" % ( ftsReqID,
                                                                                                           event )
    res = self._update( req )
    if not res['OK']:
      err = "TransferDB._addLoggingEvent: Failed to add logging event to FTSReq %s" % ftsReqID
      return S_ERROR( err )
    return res

  #################################################################################
  # These are the methods for managing the ReplicationTree table

  def addReplicationTree( self, fileID, tree ):
    """ insert new ReplicationTree record given :fileID: and replicationTree dictionary

    :param self: self refere
    :param int fileID: Files.FileID
    :param dict tree: replicationTree produced by StrategyHandler
    """
    for channelID, repDict in tree.items():
      ancestor = repDict["Ancestor"] if repDict["Ancestor"] else "-"
      strategy = repDict['Strategy']
      req = "INSERT INTO ReplicationTree (FileID, ChannelID, AncestorChannel, Strategy, CreationTime) " \
          " VALUES (%s,%s,'%s','%s',UTC_TIMESTAMP());" % ( fileID, channelID, ancestor, strategy )
      res = self._update( req )
      if not res['OK']:
        err = "TransferDB._addReplicationTree: Failed to add ReplicationTree for file %s" % fileID
        return S_ERROR( err )
    return S_OK()

  #################################################################################
  # These are the methods for managing the FileToCat table

  def addFileRegistration( self, channelID, fileID, lfn, targetSURL, destSE ):
    """ insert new record into FileToCat table

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param int fileID: Files.FileID
    :param str lfn: Files.LFN
    :param str targetSULR: Channel.TargetSURL
    :param str destSE: Channel.TargetSE
    """
    req = "INSERT INTO FileToCat (FileID,ChannelID,LFN,PFN,SE,SubmitTime) " \
        "VALUES (%s,%s,'%s','%s','%s',UTC_TIMESTAMP());" % ( fileID, channelID, lfn, targetSURL, destSE )
    res = self._update( req )
    if not res['OK']:
      err = "TransferDB._addFileRegistration: Failed to add registration entry for file %s" % fileID
      return S_ERROR( err )
    return S_OK()

  def getCompletedReplications( self ):
    """ get SubRequests.Operation, SubRequest.SourceSE, FileToCat.LFN for FileToCat.Status='Waiting'

    :param self: self reference
    """
    req = "SELECT sR.Operation,sR.SourceSE,fc.LFN FROM SubRequests AS sR, Files AS f, FileToCat AS fc " \
        "WHERE fc.Status = 'Waiting' AND fc.FileID=f.FileID AND sR.SubRequestID=f.SubRequestID;"
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB._getCompletedReplications: Failed to get completed replications."
      return S_ERROR( err )
    # # lazy people are using list c'tor
    return S_OK( list( res["Value"] ) )

  def getWaitingRegistrations( self ):
    """ select 'Waiting' records from FileToCat table

    :return: S_OK( [ (fileID, channelID, LFN, PFN, SE), ... ] )

    :param self: self reference
    """
    req = "SELECT FileID, ChannelID, LFN, PFN, SE FROM FileToCat WHERE Status='Waiting';"
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB._getWaitingRegistrations: Failed to get registrations."
      return S_ERROR( err )
    # # less typing, use list constructor
    return S_OK( list( res["Value"] ) )

  def setRegistrationWaiting( self, channelID, fileIDs ):
    """ set FileToCat.Status to 'Waiting' for given :channelID: and list if :fileIDs:

    :param self: self reference
    :param int channelID: Channel.ChannelID
    :param list fileIDs: list of Files.FileID
    """
    req = "UPDATE FileToCat SET Status='Waiting' WHERE ChannelID=%s AND " \
        "Status='Executing' AND FileID IN (%s);" % ( channelID, intListToString( fileIDs ) )
    res = self._update( req )
    if not res['OK']:
      err = "TransferDB._setRegistrationWaiting: Failed to update %s files status." % len( fileIDs )
      return S_ERROR( err )
    return S_OK()

  def setRegistrationDone( self, channelID, fileID ):
    """ set FileToCat.Status to 'Done' for given :channelID: and :fileID:,
        update FileToCat.CompleteTime timestamp

    :param self: self reference
    :param int channelID: Channel.ChanneID
    :param list fileID: Files.FileID or list of Files.FileID
    """
    if type( fileID ) == int:
      fileID = [ fileID ]
    req = "UPDATE FileToCat SET Status='Done',CompleteTime=UTC_TIMESTAMP() " \
        "WHERE FileID IN (%s) AND ChannelID=%s AND Status='Waiting';" % ( intListToString( fileID ), channelID )
    res = self._update( req )
    if not res['OK']:
      err = "TransferDB._setRegistrationDone: Failed to update %s status." % fileID
      return S_ERROR( err )
    return S_OK()

  def getRegisterFailover( self, fileID ):
    """ in FTSMonitorAgent on failed registration
        FileToCat.Status is set to 'Waiting' (was 'Executing')
        got to query those for TA and will try to regiter them there

    :param self: self reference
    :param int fileID: Files.FileID
    """
    query = "SELECT PFN, SE, ChannelID, MAX(SubmitTime) FROM FileToCat WHERE Status='Waiting' AND FileID=%s;" % fileID
    res = self._query( query )
    if not res["OK"]:
      return res
    # # from now on don't care about SubmitTime and empty records
    res = [ rec[:3] for rec in res["Value"] if None not in rec[:3] ]
    # # return list of tuples [ ( PFN, SE, ChannelID ), ... ]
    return  S_OK( res )

  #################################################################################
  # These are the methods used by the monitoring server

  def getFTSJobDetail( self, ftsReqID ):
    """ select detailed information about FTSRequest for given :ftsReqID:

    :return: S_OK( [ ( LFN, FileToFTS.Status, Duration, Reason, Retries, FielSize ), ...  ] )

    :param self: self reference
    :param int ftsReqID: FTSReq.FTSReqID
    """
    req = "SELECT Files.LFN,FileToFTS.Status,Duration,Reason,Retries,FileSize FROM FileToFTS,Files " \
        "WHERE FTSReqID =%s and Files.FileID=FileToFTS.FileID;" % ftsReqID
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB.getFTSJobDetail: Failed to get detailed info for FTSReq %s: %s." % ( ftsReqID, res['Message'] )
      return S_ERROR( err )
    # # lazy people are using list c'tor
    return S_OK( list( res["Value"] ) )

  def getSites( self ):
    """ select distinct SourceSites and DestinationSites from Channels table

    :param self: self reference
    """
    req = "SELECT DISTINCT SourceSite FROM Channels;"
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB.getSites: Failed to get channel SourceSite: %s" % res['Message']
      return S_ERROR( err )
    sourceSites = [ record[0] for record in res["Value"] ]

    req = "SELECT DISTINCT DestinationSite FROM Channels;"
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB.getSites: Failed to get channel DestinationSite: %s" % res['Message']
      return S_ERROR( err )
    destSites = [ record[0] for record in res["Value"] ]
    return S_OK( { 'SourceSites' : sourceSites, 'DestinationSites' : destSites } )


  def getFTSJobs( self ):
    """ get FTS jobs

    :param self: self reference
    """
    req = "SELECT FTSReqID,FTSGUID,FTSServer,SubmitTime,LastMonitor,PercentageComplete," \
        "Status,NumberOfFiles,TotalSize FROM FTSReq;"
    res = self._query( req )
    if not res['OK']:
      err = "TransferDB.getFTSJobs: Failed to get detailed FTS jobs: %s" % res['Message']
      return S_ERROR( err )
    ftsReqs = []
    for ftsReqID, ftsGUID, ftsServer, submitTime, lastMonitor, complete, status, files, size in res['Value']:
      ftsReqs.append( ( ftsReqID, ftsGUID, ftsServer, str( submitTime ),
                        str( lastMonitor ), complete, status, files, size ) )
    return S_OK( ftsReqs )


  def cleanUp( self, gracePeriod = 60, limit = 10 ):
    """ delete completed FTS requests

    it is using txns to be sure we have a proper snapshot of db

    :param self: self reference
    :param int gracePeriod: grace period in days
    :param int limit: selection of FTSReq limit

    :return: S_OK( list( tuple( 'txCmd',  txRes ), ... ) )
    """
    ftsReqs = self._query( "".join( [ "SELECT FTSReqID, ChannelID FROM FTSReq WHERE Status = 'Finished' ",
                                    "AND LastMonitor < DATE_SUB( UTC_DATE(), INTERVAL %s DAY ) LIMIT %s;" % ( gracePeriod,
                                                                                                              limit ) ] ) )
    if not ftsReqs["OK"]:
      return ftsReqs
    ftsReqs = [ item for item in ftsReqs["Value"] if None not in item ]

    delQueries = []

    for ftsReqID, channelID in ftsReqs:
      fileIDs = self._query( "SELECT FileID from FileToFTS WHERE FTSReqID = %s AND ChannelID = %s;" % ( ftsReqID, channelID ) )
      if not fileIDs["OK"]:
        continue
      fileIDs = [ fileID[0] for fileID in fileIDs["Value"] if fileID ]
      for fileID in fileIDs:
        delQueries.append( "DELETE FROM FileToFTS WHERE FileID = %s and FTSReqID = %s;" % ( fileID, ftsReqID ) )
      delQueries.append( "DELETE FROM FTSReqLogging WHERE FTSReqID = %s;" % ftsReqID )
      delQueries.append( "DELETE FROM FTSReq WHERE FTSReqID = %s;" % ftsReqID )

    channels = self._query( "".join( [ "SELECT FileID, ChannelID FROM Channel ",
                                       "WHERE FileID NOT IN ( SELECT FileID FROM Files ) "
                                       "AND FileID NOT IN ( SELECT FileID FROM FileToFTS ) LIMIT %s;" % int( limit ) ] ) )
    if not channels["OK"]:
      return channels
    channels = [ channel for channel in channels["Value"] if None not in channel ]
    for channel in channels:
      delQueries.append( "DELETE FROM Channel WHERE FileID = %s AND ChannelID = %s;" % channel )
      delQueries.append( "DELETE FROM ReplicationTree WHERE FileID = %s AND ChannelID = %s;" % channel )
      delQueries.append( "DELETE FROM FileToCat WHERE FileID = %s and ChannelID = %s;" % channel )


    return self._transaction( sorted( delQueries ) )

  def getAttributesForReqList( self, reqIDList, attrList = None ):
    """ Get attributes for the requests in the req ID list.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[FTSReqID][attribute_name] = attribute_value
    """
    attrList = [] if not attrList else attrList
    attrNames = ''
    attr_tmp_list = [ 'FTSReqID', 'SourceSite', 'DestinationSite' ]
    for attr in attrList:
      if not attr in attr_tmp_list:
        attrNames = '%sFTSReq.%s,' % ( attrNames, attr )
        attr_tmp_list.append( attr )
    attrNames = attrNames.strip( ',' )
    reqList = ",".join( [ str( reqID ) for reqID in reqIDList ] )

    req = 'SELECT FTSReq.FTSReqID,Channels.SourceSite,Channels.DestinationSite,%s FROM FTSReq,Channels ' \
        'WHERE FTSReqID in (%s) AND Channels.ChannelID=FTSReq.ChannelID' % ( attrNames, reqList )
    res = self._query( req )
    if not res['OK']:
      return res
    retDict = {}
    for attrValues in res['Value']:
      reqDict = {}
      for i in range( len( attr_tmp_list ) ):
        try:
          reqDict[attr_tmp_list[i]] = attrValues[i].tostring()
        except:
          reqDict[attr_tmp_list[i]] = str( attrValues[i] )
      retDict[int( reqDict['FTSReqID'] )] = reqDict
    return S_OK( retDict )

  def selectFTSReqs( self, condDict, older = None, newer = None, orderAttribute = None, limit = None ):
    """ Select fts requests matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by FTSReqID if requested, the result is limited to a given
        number of jobs if requested.
    """
    condition = self.__OLDbuildCondition( condDict, older, newer )

    if orderAttribute:
      orderType = None
      orderField = orderAttribute
      if orderAttribute.find( ':' ) != -1:
        orderType = orderAttribute.split( ':' )[1].upper()
        orderField = orderAttribute.split( ':' )[0]
      condition = condition + ' ORDER BY ' + orderField
      if orderType:
        condition = condition + ' ' + orderType

    if limit:
      condition = condition + ' LIMIT ' + str( limit )

    cmd = 'SELECT FTSReqID from FTSReq, Channels ' + condition
    res = self._query( cmd )
    if not res['OK']:
      return res

    if not len( res['Value'] ):
      return S_OK( [] )
    return S_OK( map( self._to_value, res['Value'] ) )

  def __OLDbuildCondition( self, condDict, older = None, newer = None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions

    :TODO: make sure it is not used and delete this
    """
    condition = ''
    conjunction = "WHERE"

    if condDict:
      for attrName, attrValue in condDict.items():
        if attrName in [ 'SourceSites', 'DestinationSites' ]:
          condition = ' %s %s Channels.%s=\'%s\'' % ( condition,
                                                      conjunction,
                                                      str( attrName.rstrip( 's' ) ),
                                                      str( attrValue ) )
        else:
          condition = ' %s %s FTSReq.%s=\'%s\'' % ( condition,
                                                    conjunction,
                                                    str( attrName ),
                                                    str( attrValue ) )
        conjunction = "AND"
      condition += " AND FTSReq.ChannelID = Channels.ChannelID "
    else:
      condition += " WHERE FTSReq.ChannelID = Channels.ChannelID "

    if older:
      condition = ' %s %s LastUpdateTime < \'%s\'' % ( condition,
                                                       conjunction,
                                                       str( older ) )
      conjunction = "AND"

    if newer:
      condition = ' %s %s LastUpdateTime >= \'%s\'' % ( condition,
                                                        conjunction,
                                                        str( newer ) )

    return condition


  #############################################################################
  #
  # These are the methods for monitoring the Reuqests, SubRequests and Files table
  #

  def selectRequests( self, condDict, older = None, newer = None, orderAttribute = None, limit = None ):
    """ Select requests matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by RequestID if requested, the result is limited to a given
        number of requests if requested.
    """
    return self.__selectFromTable( 'Requests', 'RequestID', condDict, older, newer, orderAttribute, limit )

  def selectSubRequests( self, condDict, older = None, newer = None, orderAttribute = None, limit = None ):
    """ Select sub-requests matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by SubRequestID if requested, the result is limited to a given
        number of sub-requests if requested.
    """
    return self.__selectFromTable( 'SubRequests', 'SubRequestID', condDict, older, newer, orderAttribute, limit )

  def selectFiles( self, condDict, older = None, newer = None, orderAttribute = None, limit = None ):
    """ Select files matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by FileID if requested, the result is limited to a given
        number of files if requested.
    """
    return self.__selectFromTable( 'Files', 'FileID', condDict, older, newer, orderAttribute, limit )

  def selectDatasets( self, condDict, older = None, newer = None, orderAttribute = None, limit = None ):
    """ Select datasets matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by DatasetID if requested, the result is limited to a given
        number of datasets if requested.
    """
    return self.__selectFromTable( 'Datasets', 'DatasetID', condDict, older, newer, orderAttribute, limit )

  def getAttributesForRequestList( self, reqIDList, attrList = None ):
    """ Get attributes for the requests in the the reqIDList.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[reqID][attribute_name] = attribute_value
    """
    attrList = [] if not attrList else attrList
    return self.__getAttributesForList( 'Requests', 'RequestID', reqIDList, attrList )

  def getAttributesForSubRequestList( self, subReqIDList, attrList = None ):
    """ Get attributes for the subrequests in the the reqIDList.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[subReqID][attribute_name] = attribute_value
    """
    attrList = [] if not attrList else attrList
    return self.__getAttributesForList( 'SubRequests', 'SubRequestID', subReqIDList, attrList )

  def getAttributesForFilesList( self, fileIDList, attrList = None ):
    """ Get attributes for the files in the the fileIDlist.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[fileID][attribute_name] = attribute_value
    """
    attrList = [] if not attrList else attrList
    return self.__getAttributesForList( 'Files', 'FileID', fileIDList, attrList )

  def getAttributesForDatasetList( self, datasetIDList, attrList = None ):
    """ Get attributes for the datasets in the the datasetIDlist.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[datasetID][attribute_name] = attribute_value
    """
    attrList = [] if not attrList else attrList
    return self.__getAttributesForList( 'Datasets', 'DatasetID', datasetIDList, attrList )

  def __getAttributesForList( self, table, tableID, idList, attrList ):
    """ select :table: columns specified in :attrList: for given :idList:

    :param self: self reference
    :param str table: tabel name
    :param str tableID: primary key in :table:
    :param list idList: list of :table:.:tableID:
    :param list attrList: list of column names from :table:
    """
    res = self.getFields( table, outFields = [tableID] + attrList, condDict = { tableID : idList } )
    if not res['OK']:
      return res
    try:
      retDict = {}
      for retValues in res['Value']:
        rowID = retValues[0]
        reqDict = {}
        reqDict[tableID] = rowID
        attrValues = retValues[1:]
        for i in range( len( attrList ) ):
          try:
            reqDict[attrList[i]] = attrValues[i].tostring()
          except Exception, error:
            reqDict[attrList[i]] = str( attrValues[i] )
        retDict[int( rowID )] = reqDict
      return S_OK( retDict )
    except Exception, error:
      return S_ERROR( 'TransferDB.__getAttributesForList: Failed\n%s' % str( error ) )

  def __selectFromTable( self, table, tableID, condDict, older, newer, orderAttribute, limit ):
    """ select something from table something
    """
    res = self.getFields( table, [tableID], condDict, limit,
                          older = older, newer = newer,
                          timeStamp = 'LastUpdateTime',
                          orderAttribute = orderAttribute )
    if not res['OK']:
      return res
    if not len( res['Value'] ):
      return S_OK( [] )

    return S_OK( map( self._to_value, res['Value'] ) )

  def getDistinctRequestAttributes( self, attribute, condDict = None, older = None, newer = None ):
    """ Get distinct values of the Requests table attribute under specified conditions
    """
    return self.getDistinctAttributeValues( 'Requests', attribute, condDict, older, newer, timeStamp = 'LastUpdateTime' )

  def getDistinctSubRequestAttributes( self, attribute, condDict = None, older = None, newer = None ):
    """ Get distinct values of SubRequests the table attribute under specified conditions
    """
    return self.getDistinctAttributeValues( 'SubRequests', attribute, condDict, older, newer, timeStamp = 'LastUpdateTime' )

  def getDistinctFilesAttributes( self, attribute, condDict = None, older = None, newer = None, timeStamp = None ):
    """ Get distinct values of the Files  table attribute under specified conditions
    """
    return self.getDistinctAttributeValues( 'Files', attribute, condDict = None, older = None, newer = None, timeStamp = None )

  def getDistinctChannelAttributes( self, attribute, condDict = None, older = None, newer = None, timeStamp = 'LastUpdateTime' ):
    """ Get distinct values of the Channel table attribute under specified conditions
    """
    return self.getDistinctAttributeValues( 'Channel', attribute, condDict = None , older = None, newer = None, timeStamp = 'LastUpdate' )

  def getDistinctChannelsAttributes( self, attribute, condDict = None, older = None, newer = None, timeStamp = None ):
    """ Get distinct values of the Channels table attribute under specified conditions
    """
    return self.getDistinctAttributeValues( 'Channels', attribute, condDict, older, newer, timeStamp )
