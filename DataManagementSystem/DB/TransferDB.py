""" RequestDB is a front end to the Request Database.
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.List import randomize,stringListToString,intListToString
import threading,types,string,time,datetime

MAGIC_EPOC_NUMBER = 1270000000

gLogger.initialize('DMS','/Databases/TransferDB/Test')

class TransferDB(DB):

  def __init__(self, systemInstance ='Default', maxQueueSize=10 ):
    DB.__init__(self,'TransferDB','RequestManagement/RequestDB',maxQueueSize)
    self.getIdLock = threading.Lock()

  def __getFineTime(self):
     _date = datetime.datetime.utcnow()
     epoc = time.mktime(_date.timetuple()) - MAGIC_EPOC_NUMBER
     time_order = round(epoc,3)
     return time.time() - MAGIC_EPOC_NUMBER

  #################################################################################
  # These are the methods for managing the Channels table

  def createChannel(self,sourceSE,destSE):
    self.getIdLock.acquire()
    res = self.checkChannelExists(sourceSE,destSE)
    if res['OK']:
      if res['Value']['Exists']:
        self.getIdLock.release()
        str = 'TransferDB._createChannel: Channel %s already exists from %s to %s.' % (res['Value']['ChannelID'],sourceSE,destSE)
        gLogger.debug(str)
        return res
    req = "INSERT INTO Channels (SourceSite,DestinationSite,Status,ChannelName) VALUES ('%s','%s','%s','%s-%s');" % (sourceSE,destSE,'Active',sourceSE,destSE)
    res = self._update(req)
    if not res['OK']:
      self.getIdLock.release()
      err = 'TransferDB._createChannel: Failed to create channel from %s to %s.' % (sourceSE,destSE)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    self.getIdLock.release()
    res = self.checkChannelExists(sourceSE,destSE)
    return res

  def checkChannelExists(self,sourceSE,destSE):
    req = "SELECT ChannelID FROM Channels WHERE SourceSite = '%s' AND DestinationSite = '%s';" % (sourceSE,destSE)
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._checkChannelExists: Failed to retrieve ChannelID for %s to %s.' % (sourceSE,destSE)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    resultDict = {}
    if res['Value']:
      resultDict['Exists'] = True
      resultDict['ChannelID'] = res['Value'][0][0]
    else:
      resultDict['Exists'] = False
    return S_OK(resultDict)

  def getChannelID(self,sourceSE,destSE):
    res = self.checkChannelExists(sourceSE, destSE)
    if res['OK']:
      if res['Value']['Exists']:
        return S_OK(res['Value']['ChannelID'])
      else:
        err = 'TransferDB._getChannelID: Channel from %s to %s does not exist.' % (sourceSE,destSE)
        return S_ERROR(err)
    return res

  def getChannelAttribute(self,channelID,attrName):
    req = "SELECT %s FROM Channels WHERE ChannelID = %s;" % (attrName,channelID)
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._getChannelAttribute: Failed to get %s for Channel %s.' % (attrName,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      err = 'TransferDB._getChannelAttribute: No Channel %s defined.' % (channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return S_OK(res['Value'][0][0])

  def setChannelAttribute(self,channelID,attrName,attrValue):
    req = "UPDATE Channels SET %s = '%s' WHERE ChannelID = %s;" % (attrName,attrValue,channelID)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._setChannelAttribute: Failed to update %s to %s for Channel %s.' % (attrName,attrValue,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def getChannels(self):
    req = "SELECT ChannelID,SourceSite,DestinationSite,Status,ChannelName from Channels;"
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._getChannels: Failed to retrieve channel information.'
      return S_ERROR('%s\n%s' % (err,res['Message']))
    channels = {}
    for channelID,sourceSite,destSite,status,channelName in res['Value']:
      channels[channelID] = {}
      channels[channelID]['Source'] = sourceSite
      channels[channelID]['Destination'] = destSite
      channels[channelID]['Status'] = status
      channels[channelID]['ChannelName'] = channelName
    return S_OK(channels)

  def getChannelsForState(self,status):
    req = "SELECT ChannelID,SourceSite,DestinationSite FROM Channels WHERE Status = '%s';" % status
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._getChannelsInState: Failed to get Channels for Status = %s.' % (status)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      return S_OK()
    channels = {}
    channelIDs = []
    for channelID,sourceSite,destinationSite in res['Value']:
      channels[channelID] = {'SourceSite':sourceSite,'DestinationSite':destinationSite}
      channelIDs.append(channelID)
    resDict = {'ChannelIDs':channelIDs,'Channels':channels}
    return S_OK(resDict)


  #################################################################################
  # These are the methods for managing the Channel table

  def selectChannelForSubmission(self,maxJobsPerChannel):
    res = self.getChannelQueues(status='Waiting')
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK()
    channels = res['Value']
    candidateChannels = {}
    for channelID in channels.keys():
      if channels[channelID]['Status'] == 'Active':
        if channels[channelID]['Files'] > 0:
          candidateChannels[channelID] = channels[channelID]['Files']

    if not len(candidateChannels.keys()) >0:
      return S_OK()
    strChannelIDs = intListToString(candidateChannels.keys())
    req = "SELECT ChannelID,SUM(Status='Submitted') FROM FTSReq WHERE ChannelID IN (%s) GROUP BY ChannelID;" % strChannelIDs
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._selectChannelForSubmission: Failed to count FTSJobs on Channels %s.' % strChannelIDs
      return S_ERROR('%s\n%s' % (err,res['Message']))
    withJobs = {}
    for channelID,numberOfJobs in res['Value']:
      withJobs[channelID] = numberOfJobs

    minJobs = maxJobsPerChannel
    maxFiles = 0
    possibleChannels = []
    for channelID,files in candidateChannels.items():
      if withJobs.has_key(channelID):
        numberOfJobs = withJobs[channelID]
      else:
        numberOfJobs = 0
      if numberOfJobs < maxJobsPerChannel:
        if numberOfJobs < minJobs:
          minJobs = numberOfJobs
          maxFiles = files
          possibleChannels.append(channelID)
        elif numberOfJobs == minJobs:
          if files > maxFiles:
            maxFiles = files
            possibleChannels = []
            possibleChannels.append(channelID)
          elif candidateChannels[channelID] == maxFiles:
            possibleChannels.append(channelID)
    if not possibleChannels:
      return S_OK()

    resDict = {}
    selectedChannel = randomize(possibleChannels)[0]
    resDict = channels[selectedChannel]
    resDict['ChannelID'] = selectedChannel
    return S_OK(resDict)

  def addFileToChannel(self,channelID,fileID,sourceSURL,targetSURL,fileSize,spaceToken,fileStatus='Waiting'):
    res = self.checkFileChannelExists(channelID, fileID)
    if not res['OK']:
      err = 'TransferDB._addFileToChannel: Failed check existance of File %s on Channel %s.' % (fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if res['Value']:
      err = 'TransferDB._addFileToChannel: File %s already exists on Channel %s.' % (fileID,channelID)
      return S_ERROR(err)
    time_order = self.__getFineTime()
    req = "INSERT INTO Channel (ChannelID,FileID,SourceSURL,TargetSURL,SpaceToken,SchedulingTime,SchedulingTimeOrder,LastUpdate,LastUpdateTimeOrder,FileSize,Status) VALUES \
           (%s,%s,'%s','%s','%s',UTC_TIMESTAMP(),%s,UTC_TIMESTAMP(),%s,%s,'%s');"  % (channelID,fileID,sourceSURL,targetSURL,spaceToken,time_order,time_order,fileSize,fileStatus)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._addFileToChannel: Failed to insert File %s to Channel %s.' % (fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def checkFileChannelExists(self,channelID,fileID):
    req = "SELECT FileID FROM Channel WHERE ChannelID = %s and FileID = %s;" % (channelID,fileID)
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._checkFileChannelExists: Failed to check existance of File %s on Channel %s.' % (fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if res['Value']:
      return S_OK(True)
    return S_OK(False)

  def setChannelFilesExecuting(self,channelID,fileIDs):
    strFileIDs = intListToString(fileIDs)
    time_order = self.__getFineTime()
    req = "UPDATE Channel SET Status = 'Executing',  LastUpdate=UTC_TIMESTAMP(),LastUpdateTimeOrder = %s  WHERE FileID IN (%s) AND ChannelID = %s;" % (time_order,strFileIDs,channelID)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._setChannelFilesExecuting: Failed to set file executing.'
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def updateAncestorChannelStatus(self,channelID,fileIDs):
    if not len(fileIDs) >0:
      return S_OK()
    strFileIDs = intListToString(fileIDs)
    req = "UPDATE Channel SET Status = 'Waiting' WHERE FileID IN (%s) AND Status = 'Waiting%s';" % (strFileIDs,channelID)
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._updateAncestorChannelStatus: Failed to update status."
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def removeFilesFromChannel(self,channelID,fileIDs):
    for fileID in fileIDs:
      res = self.removeFileFromChannel(channelID,fileID)
      if not res['OK']:
        return res
    return res

  def removeFileFromChannel(self,channelID,fileID):
    req = "DELETE FROM Channel WHERE ChannelID = %s and FileID = %s;" % (channelID,fileID)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._removeFileFromChannel: Failed to remove File %s from Channel %s.' % (fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def updateCompletedChannelStatus(self,channelID,fileID):
    time_order = self.__getFineTime()
    req = "UPDATE Channel SET Status = 'Done',LastUpdate=UTC_TIMESTAMP(),LastUpdateTimeOrder = %s, CompletionTime=UTC_TIMESTAMP() WHERE FileID=%s AND ChannelID=%s;" % (time_order,fileID,channelID)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._updateCompletedChannelStatus: Failed to update File %s from Channel %s.' % (fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def resetFileChannelStatus(self,channelID,fileID):
    time_order = self.__getFineTime()
    req = "UPDATE Channel SET Status = 'Waiting',LastUpdate=UTC_TIMESTAMP(),LastUpdateTimeOrder = %s,Retries=Retries+1 WHERE FileID=%s AND ChannelID=%s;" % (time_order,fileID,channelID)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._resetFileChannelStatus: Failed to reset File %s from Channel %s.' % (fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def getFileChannelAttribute(self,channelID,fileID,attribute):
    req = "SELECT %s from Channel WHERE ChannelID = %s and FileID = %s;" % (attribute,channelID,fileID)
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB._getFileChannelAttribute: Failed to get %s for File %s on Channel %s." % (attribute,fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      err = "TransferDB._getFileChannelAttribute: File %s doesn't exist on Channel %s." % (fileID,channelID)
      return S_ERROR(err)
    attrValue = res['Value'][0][0]
    return S_OK(attrValue)

  def setFileChannelStatus(self,channelID,fileID,status):
    res = self.setFileChannelAttribute(channelID, fileID, 'Status', status)
    return res

  def setFileChannelAttribute(self,channelID,fileID,attribute,attrValue):
    req = "UPDATE Channel SET %s = '%s' WHERE ChannelID = %s and FileID = %s;" % (attribute,attrValue,channelID,fileID)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._setFileChannelAttribute: Failed to set %s to %s for File %s on Channel %s.' % (attribute,attrValue,fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def getFilesForChannel(self,channelID,numberOfFiles):
    req = "SELECT SpaceToken FROM Channel WHERE ChannelID = %s AND Status = 'Waiting' ORDER BY LastUpdateTimeOrder LIMIT 1;" % (channelID)
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getFilesForChannel: Failed to get files for Channel %s." % channelID
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      return S_OK()
    spaceToken = res['Value'][0][0]
    req = "SELECT FileID,SourceSURL,TargetSURL,FileSize FROM Channel WHERE ChannelID = %s AND Status = 'Waiting' AND SpaceToken = '%s' ORDER BY LastUpdateTimeOrder LIMIT %s;" % (channelID,spaceToken,numberOfFiles)
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getFilesForChannel: Failed to get files for Channel %s." % channelID
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      return S_OK()
    resDict = {'SpaceToken':spaceToken}
    files = []
    for fileID,sourceSURL,targetSURL,size in res['Value']:
      req = "SELECT LFN from Files WHERE FileID = %s;" % fileID
      res = self._query(req)
      if not res['OK']:
        err = "TransferDB.getFilesForChannel: Failed to get LFN for File %s." % fileID
        return S_ERROR('%s\n%s' % (err,res['Message']))
      lfn = res['Value'][0][0]
      files.append({'FileID':fileID,'SourceSURL':sourceSURL,'TargetSURL':targetSURL,'LFN':lfn,'Size':size})
    resDict['Files'] = files
    return S_OK(resDict)

  def getChannelQueues(self,status=None):
    res = self.getChannels()
    if not res['OK']:
      return res
    channels = res['Value']
    channelIDs = channels.keys()
    if status:
      req = "SELECT ChannelID,COUNT(*),SUM(FileSize) FROM Channel WHERE Status = '%s' GROUP BY ChannelID;" % (status)
    else:
      req = "SELECT ChannelID,COUNT(*),SUM(FileSize) FROM Channel WHERE Status LIKE 'Waiting%s' GROUP BY ChannelID;" % ('%')
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getChannelQueues: Failed to get Channel contents for Channels." % strChannelIDs
      return S_ERROR('%s\n%s' % (err,res['Message']))
    channelDict = {}
    for channelID,fileCount,sizeCount in res['Value']:
      channels[channelID]['Files'] = int(fileCount)
      channels[channelID]['Size'] = int(sizeCount)
    for channelID in channelIDs:
      if not channels[channelID].has_key('Files'):
        channels[channelID]['Files'] = 0
        channels[channelID]['Size'] = 0
    return S_OK(channels)

  #################################################################################
  # These are the methods for managing the FTSReq table

  def insertFTSReq(self,ftsGUID,ftsServer,channelID):
    self.getIdLock.acquire()
    req = "INSERT INTO FTSReq (FTSGUID,FTSServer,ChannelID,SubmitTime,LastMonitor) VALUES ('%s','%s',%s,UTC_TIMESTAMP(),UTC_TIMESTAMP());" % (ftsGUID,ftsServer,channelID)
    res = self._update(req)
    if not res['OK']:
      self.getIdLock.release()
      err = "TransferDB._insertFTSReq: Failed to insert FTS GUID into FTSReq table."
      return S_ERROR('%s\n%s' % (err,res['Message']))
    req = "SELECT MAX(FTSReqID) FROM FTSReq;"
    res = self._query(req)
    self.getIdLock.release()
    if not res['OK']:
      err = "TransferDB._insertFTSReq: Failed to get FTSReqID from FTSReq table."
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      err = "TransferDB._insertFTSReq: Request details don't appear in FTSReq table."
      return S_ERROR(err)
    ftsReqID = res['Value'][0][0]
    return S_OK(ftsReqID)

  def setFTSReqStatus(self,ftsReqID,status):
    self.getIdLock.acquire()
    req = "UPDATE FTSReq SET Status = '%s' WHERE FTSReqID = %s;" % (status,ftsReqID)
    res = self._update(req)
    self.getIdLock.release()
    if not res['OK']:
      err = "TransferDB._setFTSReqStatus: Failed to set status to %s for FTSReq %s." % (status,ftsReqID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def deleteFTSReq(self,ftsReqID):
    self.getIdLock.acquire()
    req = "DELETE FROM FTSReq WHERE FTSReqID = %s;" % (ftsReqID)
    res = self._update(req)
    self.getIdLock.release()
    if not res['OK']:
      err = "TransferDB._deleteFTSReq: Failed to delete FTSReq %s." % (ftsReqID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def getFTSReq(self):
    req = "SELECT f.FTSReqID,f.FTSGUID,f.FTSServer,f.ChannelID,f.SubmitTime,f.NumberOfFiles,f.TotalSize,c.SourceSite,c.DestinationSite FROM FTSReq as f,Channels as c WHERE f.Status = 'Submitted' and f.ChannelID=c.ChannelID ORDER BY f.LastMonitor LIMIT 1;"

    # SELECT FTSReqID,FTSGUID,FTSServer,ChannelID,SubmitTime,NumberOfFiles,TotalSize FROM FTSReq WHERE Status = 'Submitted' ORDER BY LastMonitor LIMIT 1;"
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB._getFTSReq: Failed to get entry from FTSReq table."
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      # It is not an error that there are not requests
      return S_OK()
    resDict = {}
    ftsReqID,ftsGUID,ftsServer,channelID,submitTime,numberOfFiles,totalSize,sourceSite,destSite = res['Value'][0]
    resDict['FTSReqID'] = ftsReqID
    resDict['FTSGuid'] = ftsGUID
    resDict['FTSServer'] = ftsServer
    resDict['ChannelID'] = channelID
    resDict['SubmitTime'] = submitTime
    resDict['NumberOfFiles'] = numberOfFiles
    resDict['TotalSize'] = totalSize
    resDict['Source'] = sourceSite
    resDict['Target'] = destSite
    return S_OK(resDict)

  def setFTSReqAttribute(self,ftsReqID,attribute,attrValue):
    self.getIdLock.acquire()
    req = "UPDATE FTSReq SET %s = '%s' WHERE FTSReqID = %s;" % (attribute,attrValue,ftsReqID)
    res = self._update(req)
    self.getIdLock.release()
    if not res['OK']:
      err = "TransferDB._setFTSReqAttribute: Failed to set %s to %s for FTSReq %s." % (attribute,attrValue,ftsReqID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def setFTSReqLastMonitor(self,ftsReqID):
    req = "UPDATE FTSReq SET LastMonitor = UTC_TIMESTAMP() WHERE FTSReqID = %s;" % ftsReqID
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._setFTSReqLastMonitor: Failed to update monitoring time for FTSReq %s." % ftsReqID
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res


  #################################################################################
  # These are the methods for managing the FileToFTS table

  def getFTSReqLFNs(self,ftsReqID):
    req = "SELECT FileID,LFN FROM Files WHERE FileID IN (SELECT FileID from FileToFTS WHERE FTSReqID = %s);" % ftsReqID
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB._getFTSReqLFNs: Failed to get LFNs for FTSReq %s." % ftsReqID
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      err = "TransferDB._getFTSReqLFNs: No LFNs found for FTSReq %s." % ftsReqID
      return S_ERROR(err)
    files = {}
    for fileID,lfn in res['Value']:
      files[lfn] = fileID
    return S_OK(files)

  def setFTSReqFiles(self,ftsReqID,fileIDs,channelID):
    for fileID in fileIDs:
      req = "INSERT INTO FileToFTS (FTSReqID,FileID,ChannelID,SubmissionTime) VALUES (%s,%s,%s,UTC_TIMESTAMP());" % (ftsReqID,fileID,channelID)
      res = self._update(req)
      if not res['OK']:
        err = "TransferDB._setFTSReqFiles: Failed to set File %s for FTSReq %s." % (fileID,ftsReqID)
        return S_ERROR('%s\n%s' % (err,res['Message']))
    return S_OK()

  def getFTSReqFileIDs(self,ftsReqID):
    req = "SELECT FileID FROM FileToFTS WHERE FTSReqID = %s;" % ftsReqID
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB._getFTSReqFileIDs: Failed to get FileIDs for FTSReq %s." % ftsReqID
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      err = "TransferDB._getFTSReqLFNs: No FileIDs found for FTSReq %s." % ftsReqID
      return S_ERROR(err)
    fileIDs = []
    for fileID in res['Value']:
      fileIDs.append(fileID[0])
    return S_OK(fileIDs)

  def getSizeOfCompletedFiles(self,ftsReqID,completedFileIDs):
    req = "SELECT SUM(FileSize) FROM FileToFTS where FTSReqID = %s AND FileID IN (%s);" % (ftsReqID,intListToString(completedFileIDs))
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB._getSizeOfCompletedFiles: Failed to get successful transfer size for FTSReq %s." % ftsReqID
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return S_OK(res['Value'][0][0])

  def removeFilesFromFTSReq(self,ftsReqID):
    req = "DELETE FROM FileToFTS WHERE FTSReqID = %s;" % ftsReqID
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._removeFilesFromFTSReq: Failed to remove files for FTSReq %s." % ftsReqID
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def setFileToFTSFileAttribute(self,ftsReqID,fileID,attribute,attrValue):
    req = "UPDATE FileToFTS SET %s = '%s' WHERE FTSReqID = %s AND FileID = %s;" % (attribute,attrValue,ftsReqID,fileID)
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._setFileToFTSFileAttribute: Failed to set %s to %s for File %s and FTSReq %s;" % (attribute,attrValue,fileID,ftsReqID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def setFileToFTSTerminalTime(self,ftsReqID,fileID):
    req = "UPDATE FileToFTS SET TerminalTime = UTC_TIMESTAMP() WHERE FTSReqID = %s AND FileID = %s;" % (ftsReqID,fileID)
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._setFileToFTSTerminalTime: Failed to set terminal time for File %s and FTSReq %s;" % (fileID,ftsReqID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def getChannelObservedThroughput(self,interval):
    res = self.getChannels()
    if not res['OK']:
      return res
    channels = res['Value']
    channelIDs = channels.keys()

    #############################################
    # First get the total time spend transferring files on the channels
    req = "SELECT ChannelID,SUM(TIME_TO_SEC(TIMEDIFF(TerminalTime,SubmissionTime))) FROM FileToFTS WHERE SubmissionTime > (UTC_TIMESTAMP() - INTERVAL %s SECOND) GROUP BY ChannelID;" % interval
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._getFTSObservedThroughput: Failed to obtain total time transferring.'
      return S_ERROR('%s\n%s' % (err,res['Message']))
    channelTimeDict = {}     
    for channelID,totalTime in res['Value']:
      channelTimeDict[channelID] = float(totalTime)

    #############################################
    # Now get the total size of the data transferred and the number of files that were successful 
    req = "SELECT ChannelID,SUM(FileSize),COUNT(*) FROM FileToFTS WHERE Status = 'Completed' and SubmissionTime > (UTC_TIMESTAMP() - INTERVAL %s SECOND) GROUP BY ChannelID;" % interval
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._getFTSObservedThroughput: Failed to obtain total transferred data and files.'
      return S_ERROR('%s\n%s' % (err,res['Message']))
    channelDict = {}
    for channelID,data,files in res['Value']:
      throughPut = float(data)/channelTimeDict[channelID]
      filePut = float(files)/channelTimeDict[channelID]
      channelDict[channelID] = {'Throughput': throughPut,'Fileput': filePut}

    for channelID in channelIDs:
      if not channelDict.has_key(channelID):
        channelDict[channelID] = {'Throughput': 0,'Fileput': 0}

    #############################################
    # Now get the success rate on the channels
    req = "SELECT ChannelID,SUM(Status='Completed'),SUM(Status='Failed') from FileToFTS WHERE SubmissionTime > (UTC_TIMESTAMP() - INTERVAL %s SECOND) GROUP BY ChannelID;" % (interval)
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._getFTSObservedThroughput: Failed to obtain success rate.'
      return S_ERROR('%s\n%s' % (err,res['Message']))
    for channelID,successful,failed in res['Value']:
      channelDict[channelID]['SuccessfulFiles'] = int(successful)
      channelDict[channelID]['FailedFiles'] = int(failed)
    for channelID in channelIDs:
      if not channelDict[channelID].has_key('SuccessfulFiles'):
        channelDict[channelID]['SuccessfulFiles'] = 0
        channelDict[channelID]['FailedFiles'] = 0

    return S_OK(channelDict)

  def getTransferDurations(self,channelID,startTime=None,endTime=None):
    """ This obtains the duration of the successful transfers on the supplied channel
    """
    req = "SELECT Duration FROM FileToFTS WHERE ChannelID = %s and Duration > 0" % channelID
    if startTime:
      req = "%s AND SubmissionTime > '%s'" % req
    if endTime:
      req = "%s AND SubmissionTime < '%s'" % req
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getTransferDurations: Failed to obtain durations from FileToFTS"
      return S_ERROR(err)
    durations = []
    for value in res['Value']:
      durations.append(int(value[0]))
    return S_OK(durations)

  #################################################################################
  # These are the methods for managing the FTSReqLogging table

  def addLoggingEvent(self,ftsReqID,event):
    req = "INSERT INTO FTSReqLogging (FTSReqID,Event,EventDateTime) VALUES (%s,'%s',UTC_TIMESTAMP());" % (ftsReqID,event)
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._addLoggingEvent: Failed to add logging event to FTSReq %s" % ftsReqID
      return S_ERROR(err)
    return res

  #################################################################################
  # These are the methods for managing the ReplicationTree table

  def addReplicationTree(self,fileID,tree):
    for channelID,dict in tree.items():
       ancestor = dict['Ancestor']
       if not ancestor:
         ancestor = '-'
       strategy = dict['Strategy']
       req = "INSERT INTO ReplicationTree (FileID,ChannelID,AncestorChannel,Strategy,CreationTime) VALUES (%s,%s,'%s','%s',UTC_TIMESTAMP());" % (fileID,channelID,ancestor,strategy)
       res = self._update(req)
       if not res['OK']:
        err = "TransferDB._addReplicationTree: Failed to add ReplicationTree for file %s" % fileID
        return S_ERROR(err)
    return S_OK()

  #################################################################################
  # These are the methods for managing the FileToCat table

  def addFileRegistration(self,channelID,fileID,lfn,targetSURL,destSE):
    req = "INSERT INTO FileToCat (FileID,ChannelID,LFN,PFN,SE,SubmitTime) VALUES (%s,%s,'%s','%s','%s',UTC_TIMESTAMP());" % (fileID,channelID,lfn,targetSURL,destSE)
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._addFileRegistration: Failed to add registration entry for file %s" % fileID
      return S_ERROR(err)
    return S_OK()

  def getCompletedReplications(self):
    req = "SELECT sR.Operation,sR.SourceSE,fc.LFN FROM SubRequests AS sR, Files AS f, FileToCat AS fc WHERE fc.Status = 'Waiting' AND fc.FileID=f.FileID AND sR.SubRequestID=f.SubRequestID;"
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB._getCompletedReplications: Failed to get completed replications."
      return S_ERROR(err)
    tuples = []
    for tuple in res['Value']:
      tuples.append(tuple)
    return S_OK(tuples)

  def getWaitingRegistrations(self):
    req = "SELECT FileID,ChannelID,LFN,PFN,SE FROM FileToCat WHERE Status='Waiting';"
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB._getWaitingRegistrations: Failed to get registrations."
      return S_ERROR(err)
    tuples = []
    for tuple in res['Value']:
      tuples.append(tuple)
    return S_OK(tuples)

  def setRegistrationWaiting(self,channelID,fileID):
    req = "UPDATE FileToCat SET Status='Waiting' WHERE FileID=%s AND ChannelID=%s AND Status='Executing';" % (fileID,channelID)
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._setRegistrationWaiting: Failed to update %s status." % fileID
      return S_ERROR(err)
    return S_OK()

  def setRegistrationDone(self,channelID,fileID):
    req = "UPDATE FileToCat SET Status='Done',CompleteTime=UTC_TIMESTAMP() WHERE FileID=%s AND ChannelID=%s AND Status='Waiting';" % (fileID,channelID)
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._setRegistrationDone: Failed to update %s status." % fileID
      return S_ERROR(err)
    return S_OK()

  #################################################################################
  # These are the methods used by the monitoring server

  def getFTSJobDetail(self,ftsReqID):
    req = "SELECT Files.LFN,FileToFTS.Status,Duration,Reason,Retries,FileSize FROM FileToFTS,Files WHERE FTSReqID =%s and Files.FileID=FileToFTS.FileID;" % ftsReqID
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getFTSJobDetail: Failed to get detailed info for FTSReq %s: %s." %s (ftsReqID,res['Message'])
      return S_ERROR(err)
    files = []
    for tuple in res['Value']:
      files.append(tuple)
    return S_OK(files)

  def getSites(self):
    req = "SELECT DISTINCT SourceSite FROM Channels;"
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getSites: Failed to get channel SourceSite: %s" % res['Message']
      return S_ERROR(err)
    sourceSites = []
    for tuple in res['Value']:
      sourceSites.append(tuple[0])
    req = "SELECT DISTINCT DestinationSite FROM Channels;"
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getSites: Failed to get channel DestinationSite: %s" % res['Message']
      return S_ERROR(err)
    destSites = []
    for tuple in res['Value']:
      destSites.append(tuple[0])
    resDict = {'SourceSites':sourceSites,'DestinationSites':destSites}
    return S_OK(resDict)

  def getFTSJobs(self):
    req = "SELECT FTSReqID,FTSGUID,FTSServer,SubmitTime,LastMonitor,PercentageComplete,Status,NumberOfFiles,TotalSize FROM FTSReq;"
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getFTSJobs: Failed to get detailed FTS jobs: %s." % (res['Message'])
      return S_ERROR(err)
    ftsReqs = []
    for ftsReqID,ftsGUID,ftsServer,submitTime,lastMonitor,complete,status,files,size in res['Value']:
      strSubTime = str(submitTime)
      strLastMonitor = str(lastMonitor)
      tuple = (ftsReqID,ftsGUID,ftsServer,strSubTime,strLastMonitor,complete,status,files,size)
      ftsReqs.append(tuple)
    return S_OK(ftsReqs)

  def getAttributesForReqList(self,reqIDList,attrList=[]):
    """ Get attributes for the requests in the req ID list.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[FTSReqID][attribute_name] = attribute_value
    """
    self.ftsReqAttributeNames = []

    if not attrList:
      attrList = self.ftsReqAttributeNames
    attrNames = ''
    attr_tmp_list = ['FTSReqID','SourceSite','DestinationSite']
    for attr in attrList:
      if not attr in attr_tmp_list:
        attrNames = '%sFTSReq.%s,' % (attrNames,attr)
        attr_tmp_list.append(attr)
    attrNames = attrNames.strip(',')
    reqList = string.join(map(lambda x: str(x),reqIDList),',')

    req = 'SELECT FTSReq.FTSReqID,Channels.SourceSite,Channels.DestinationSite,%s FROM FTSReq,Channels WHERE FTSReqID in (%s) AND Channels.ChannelID=FTSReq.ChannelID' % ( attrNames, reqList )
    print req
    res = self._query( req)
    if not res['OK']:
      return res
    retDict = {}
    for attrValues in res['Value']:
      reqDict = {}
      for i in range(len(attr_tmp_list)):
        try:
          reqDict[attr_tmp_list[i]] = attrValues[i].tostring()
        except:
          reqDict[attr_tmp_list[i]] = str(attrValues[i])
      retDict[int(reqDict['FTSReqID'])] = reqDict
    return S_OK( retDict )

  def selectFTSReqs(self, condDict, older=None, newer=None, orderAttribute=None, limit=None ):
    """ Select fts requests matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by FTSReqID if requested, the result is limited to a given
        number of jobs if requested.
    """
    condition = self.__OLDbuildCondition(condDict, older, newer)

    if orderAttribute:
      orderType = None
      orderField = orderAttribute
      if orderAttribute.find(':') != -1:
        orderType = orderAttribute.split(':')[1].upper()
        orderField = orderAttribute.split(':')[0]
      condition = condition + ' ORDER BY ' + orderField
      if orderType:
        condition = condition + ' ' + orderType

    if limit:
      condition = condition + ' LIMIT ' + str(limit)

    cmd = 'SELECT FTSReqID from FTSReq ' + condition
    res = self._query( cmd )
    if not res['OK']:
      return res

    if not len(res['Value']):
      return S_OK([])
    return S_OK( map( self._to_value, res['Value'] ) )

  def __OLDbuildCondition(self, condDict, older=None, newer=None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions
    """
    condition = ''
    conjunction = "WHERE"

    if condDict != None:
      for attrName, attrValue in condDict.items():
        condition = ' %s %s FTSReq.%s=\'%s\'' % ( condition,
                                           conjunction,
                                           str(attrName),
                                           str(attrValue)  )
        conjunction = "AND"

    if older:
      condition = ' %s %s LastUpdateTime < \'%s\'' % ( condition,
                                                 conjunction,
                                                 str(older) )
      conjunction = "AND"

    if newer:
      condition = ' %s %s LastUpdateTime >= \'%s\'' % ( condition,
                                                 conjunction,
                                                 str(newer) )

    return condition


  #############################################################################
  #
  # These are the methods for monitoring the Reuqests,SubRequests and Files table
  #

  def selectRequests(self, condDict, older=None, newer=None, orderAttribute=None, limit=None ):
    """ Select requests matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by RequestID if requested, the result is limited to a given
        number of requests if requested.
    """
    return self.__selectFromTable('Requests','RequestID',condDict,older,newer,orderAttribute,limit)

  def selectSubRequests(self, condDict, older=None, newer=None, orderAttribute=None, limit=None ):
    """ Select sub-requests matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by SubRequestID if requested, the result is limited to a given
        number of sub-requests if requested.
    """
    return self.__selectFromTable('SubRequests','SubRequestID',condDict,older,newer,orderAttribute,limit)

  def selectFiles(self, condDict, older=None, newer=None, orderAttribute=None, limit=None ):
    """ Select files matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by FileID if requested, the result is limited to a given
        number of files if requested.
    """
    return self.__selectFromTable('Files','FileID',condDict,older,newer,orderAttribute,limit)

  def selectDatasets(self, condDict, older=None, newer=None, orderAttribute=None, limit=None ):
    """ Select datasets matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by DatasetID if requested, the result is limited to a given
        number of datasets if requested.
    """
    return self.__selectFromTable('Datasets','DatasetID',condDict,older,newer,orderAttribute,limit)

  def getAttributesForRequestList(self,reqIDList,attrList=[]):
    """ Get attributes for the requests in the the reqIDList.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[reqID][attribute_name] = attribute_value
    """
    return self.__getAttributesForList('Requests','RequestID',reqIDList,attrList)

  def getAttributesForSubRequestList(self,subReqIDList,attrList=[]):
    """ Get attributes for the subrequests in the the reqIDList.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[subReqID][attribute_name] = attribute_value
    """
    return self.__getAttributesForList('SubRequests','SubRequestID',subReqIDList,attrList)

  def getAttributesForFilesList(self,fileIDList,attrList=[]):
    """ Get attributes for the files in the the fileIDlist.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[fileID][attribute_name] = attribute_value
    """
    return self.__getAttributesForList('Files','FileID',fileIDList,attrList)

  def getAttributesForDatasetList(self,datasetIDList,attrList=[]):
    """ Get attributes for the datasets in the the datasetIDlist.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[datasetID][attribute_name] = attribute_value
    """
    return self.__getAttributesForList('Datasets','DatasetID',datasetIDList,attrList)


  def __getAttributesForList(self,table,tableID,idList,attrList):
    attrNames = string.join(map(lambda x: str(x),attrList ),',')
    attr_tmp_list = attrList
    intIDList = string.join(map(lambda x: str(x),idList),',')
    cmd = "SELECT %s,%s from %s where %s in (%s);" % (tableID,attrNames,table,tableID,intIDList)
    res = self._query( cmd )
    if not res['OK']:
      return res
    try:
      retDict = {}
      for retValues in res['Value']:
        rowID = retValues[0]
        reqDict = {}
        reqDict[tableID ] = rowID
        attrValues = retValues[1:]
        for i in range(len(attr_tmp_list)):
          try:
            reqDict[attr_tmp_list[i]] = attrValues[i].tostring()
          except:
            reqDict[attr_tmp_list[i]] = str(attrValues[i])
        retDict[int(rowID)] = reqDict
      return S_OK( retDict )
    except Exception,x:
      return S_ERROR( 'TransferDB.__getAttributesForList: Failed\n%s'  % str(x) )

  def __selectFromTable(self,table,tableID,condDict,older,newer,orderAttribute,limit):
    condition = self.__buildCondition(condDict, older, newer)

    if orderAttribute:
      orderType = None
      orderField = orderAttribute
      if orderAttribute.find(':') != -1:
        orderType = orderAttribute.split(':')[1].upper()
        orderField = orderAttribute.split(':')[0]
      condition = condition + ' ORDER BY ' + orderField
      if orderType:
        condition = condition + ' ' + orderType

    if limit:
      condition = condition + ' LIMIT ' + str(limit)

    cmd = 'SELECT %s from %s %s' % (tableID,table,condition)
    res = self._query(cmd)
    if not res['OK']:
      return res
    if not len(res['Value']):
      return S_OK([])
    return S_OK( map( self._to_value, res['Value'] ) )

  def __buildCondition(self, condDict, older=None, newer=None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions
    """
    condition = ''
    conjunction = "WHERE"
    if condDict != None:
      for attrName, attrValue in condDict.items():
        condition = ' %s %s %s=\'%s\'' % ( condition,
                                           conjunction,
                                           str(attrName),
                                           str(attrValue)  )
        conjunction = "AND"
    if older:
      condition = ' %s %s LastUpdateTime < \'%s\'' % ( condition,
                                                 conjunction,
                                                 str(older) )
      conjunction = "AND"

    if newer:
      condition = ' %s %s LastUpdateTime >= \'%s\'' % ( condition,
                                                 conjunction,
                                                 str(newer) )
    return condition


  def getDistinctRequestAttributes(self,attribute, condDict = {}, older = None, newer=None):
    return self.__getDistinctTableAttributes('Requests',attribute, condDict,older,newer)

  def getDistinctSubRequestAttributes(self,attribute, condDict = {}, older = None, newer=None):
    return self.__getDistinctTableAttributes('SubRequests',attribute, condDict,older,newer)

  def getDistinctFilesAttributes(self,attribute, condDict = {}, older = None, newer=None):
    return self.__getDistinctTableAttributes('Files',attribute, condDict,older,newer)

  def getDistinctChannelsAttributes(self,attribute,condDict = {}):
    return self.__getDistinctTableAttributes('Channels', attribute, condDict)

  def __getDistinctTableAttributes(self,table,attribute, condDict = {}, older = None, newer=None):
    """ Get distinct values of the table attribute under specified conditions
    """
    cmd = 'SELECT  DISTINCT(%s) FROM %s ORDER BY %s' % (attribute,table,attribute)
    cond = self.__buildCondition( condDict, older=older, newer=newer )
    result = self._query( cmd + cond )
    if not result['OK']:
      return result
    attr_list = [ x[0] for x in result['Value'] ]
    return S_OK(attr_list)


