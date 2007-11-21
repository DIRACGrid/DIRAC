""" RequestDB is a front end to the Request Database.
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.List import randomize,stringListToString,intListToString
import threading,types

gLogger.initialize('DMS','/Databases/TransferDB/Test')

class TransferDB(DB):

  def __init__(self, systemInstance ='Default', maxQueueSize=10 ):
    DB.__init__(self,'TransferDB','RequestManagement/RequestDB',maxQueueSize)
    self.getIdLock = threading.Lock()

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
    req = "INSERT INTO Channels (SourceSite,DestinationSite,Status) VALUES ('%s','%s','%s');" % (sourceSE,destSE,'Active')
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
    req = "SELECT ChannelID,SourceSite,DestinationSite,ActiveJobs,LatestThroughPut from Channels WHERE Status = 'Active';"
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._getChannels: Failed to retrieve channel information.'
      return S_ERROR('%s\n%s' % (err,res['Message']))
    channels = {}
    for channelID,sourceSite,destSite,activeJobs,throughPut in res['Value']:
      channels[channelID] = {}
      channels[channelID]['Source'] = sourceSite
      channels[channelID]['Destination'] = destSite
      channels[channelID]['ActiveJobs'] = activeJobs
      channels[channelID]['Throughput'] = throughPut
    return S_OK(channels)

  def getChannelsForState(self,status):
    req = "SELECT ChannelID,SourceSite,DestinationSite FROM Channels WHERE Status = '%s';" % status
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._getChannelsInState: Failed to get Channels for Status = %s.' % (status)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      return S_OK()
    channels = []
    channelIDs = []
    for channelID,sourceSite,destinationSite in res['Value']:
      channels.append({'ChannelID':channelID,'SourceSite':sourceSite,'DestinationSite':destinationSite})
      channelIDs.append(channelID)
    resDict = {'ChannelIDs':channelIDs,'Channels':channels}
    return S_OK(resDict)


  #################################################################################
  # These are the methods for managing the Channel table

  def selectChannelForSubmission(self,maxJobsPerChannel):
    res = self.getChannelsForState('Active')
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK()
    channelIDs = res['Value']['ChannelIDs']
    strChannelIDs = intListToString(channelIDs)
    req = "SELECT ChannelID,SUM(Status='Submitted') FROM FTSReq WHERE ChannelID IN (%s) GROUP BY ChannelID;" % strChannelIDs
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._selectChannelForSubmission: Failed to count FTSJobs on Channels %s.' % strChannelIDs
      return S_ERROR('%s\n%s' % (err,res['Message']))
    for channelID,numberOfJobs in res['Value']:
      if numberOfJobs >= maxJobsPerChannel:
        channelIDs.remove(channelID)
    if not channelIDs:
      return S_OK()
    #Write a more clever way of doing this by including the number of files waiting
    resDict = {}
    selectedChannel = randomize(channelIDs)[0]
    resDict['ChannelID'] = selectedChannel
    res = self.getChannelAttribute(selectedChannel,'SourceSite')
    if not res['OK']:
      return res
    resDict['SourceSE'] = res['Value']
    res = self.getChannelAttribute(selectedChannel,'DestinationSite')
    if not res['OK']:
      return res
    resDict['TargetSE'] = res['Value']
    return S_OK(resDict)

  def addFileToChannel(self,channelID,fileID,sourceSURL,targetSURL,fileSize,spaceToken,fileStatus='Waiting'):
    res = self.checkFileChannelExists(channelID, fileID)
    if not res['OK']:
      err = 'TransferDB._addFileToChannel: Failed check existance of File %s on Channel %s.' % (fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if res['Value']:
      err = 'TransferDB._addFileToChannel: File %s already exists on Channel %s.' % (fileID,channelID)
      return S_ERROR(err)
    req = "INSERT INTO Channel (ChannelID,FileID,SourceSURL,TargetSURL,SpaceToken,SubmitTime,FileSize,Status) VALUES (%s,%s,'%s','%s','%s',NOW(),%s,'%s');" % (channelID,fileID,sourceSURL,targetSURL,spaceToken,fileSize,fileStatus)
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

  def setFileChannelStatus(self,channelID,fileID,status):
    res = self.setFileChannelAttribute(channelID, fileID, 'Status', status)
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

  def setFileChannelAttribute(self,channelID,fileID,attribute,attrValue):
    req = "UPDATE Channel SET %s = '%s' WHERE ChannelID = %s and FileID = %s;" % (attribute,attrValue,channelID,fileID)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._setFileChannelAttribute: Failed to set %s to %s for File %s on Channel %s.' % (attribute,attrValue,fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def getFilesForChannel(self,channelID,numberOfFiles):
    req = "SELECT SpaceToken FROM Channel WHERE ChannelID = %s AND Status = 'Waiting' ORDER BY SubmitTime LIMIT 1;" % (channelID)
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getFilesForChannel: Failed to get files for Channel %s." % channelID
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      return S_OK()
    spaceToken = res['Value'][0][0]
    req = "SELECT FileID,SourceSURL,TargetSURL,FileSize FROM Channel WHERE ChannelID = %s AND Status = 'Waiting' AND SpaceToken = '%s' ORDER BY SubmitTime LIMIT %s;" % (channelID,spaceToken,numberOfFiles)
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

  def getChannelQueues(self,channelIDs):
    strChannelIDs = intListToString(channelIDs)
    req = "SELECT ChannelID,COUNT(*),SUM(FileSize) FROM Channel WHERE ChannelID IN (%s) AND Status LIKE 'Waiting%s' GROUP BY ChannelID;" % (strChannelIDs,'%')
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB.getChannelQueues: Failed to get Channel contents for Channels." % strChannelIDs
      return S_ERROR('%s\n%s' % (err,res['Message']))
    channelDict = {}
    for channelID,fileCount,sizeCount in res['Value']:
      channelDict[channelID] = {'Files': int(fileCount),'Size': int(sizeCount)}
    for channelID in channelIDs:
      if not channelDict.has_key(channelID):
        channelDict[channelID] = {'Files':0,'Size':0}
    return S_OK(channelDict)  

  def getActiveChannelQueues(self):
    res = self.getChannelsForState('Active')
    if not res['OK']:
      return res
    if not res['Value']:
      return res
    channelIDs = res['Value']['ChannelIDs']
    res = self.getChannelQueues(channelIDs)
    return res

  #################################################################################
  # These are the methods for managing the FTSReq table

  def insertFTSReq(self,ftsGUID,ftsServer,channelID):
    self.getIdLock.acquire()
    req = "INSERT INTO FTSReq (FTSGUID,FTSServer,ChannelID,SubmitTime,LastMonitor) VALUES ('%s','%s',%s,NOW(),NOW());" % (ftsGUID,ftsServer,channelID)
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
    req = "SELECT FTSReqID,FTSGUID,FTSServer FROM FTSReq WHERE Status = 'Submitted' ORDER BY LastMonitor LIMIT 1;"
    res = self._query(req)
    if not res['OK']:
      err = "TransferDB._getFTSReq: Failed to get entry from FTSReq table."
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      # It is not an error that there are not requests
      return S_OK()
    resDict = {}
    ftsReqID,ftsGUID,ftsServer = res['Value'][0]
    resDict['FTSReqID'] = ftsReqID
    resDict['FTSGuid'] = ftsGUID
    resDict['FTSServer'] = ftsServer
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
    req = "UPDATE FTSReq SET LastMonitor = NOW() WHERE FTSReqID = %s;" % ftsReqID
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
      req = "INSERT INTO FileToFTS (FTSReqID,FileID,ChannelID,SubmissionTime) VALUES (%s,%s,%s,NOW());" % (ftsReqID,fileID,channelID)
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
    req = "UPDATE FileToFTS SET TerminalTime = NOW() WHERE FTSReqID = %s AND FileID = %s;" % (ftsReqID,fileID)
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._setFileToFTSTerminalTime: Failed to set terminal time for File %s and FTSReq %s;" % (fileID,ftsReqID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def getActiveChannelObservedThroughput(self,interval):
    res = self.getChannelsForState('Active')
    if not res['OK']:
      return res
    if not res['Value']:
      return res
    channelIDs = res['Value']['ChannelIDs']
    res = self.getFTSObservedThroughput(interval,channelIDs)
    return res     
 
  def getFTSObservedThroughput(self,interval,channelIDs):
    strChannelIDs = intListToString(channelIDs)
    req = "SELECT ChannelID,SUM(FileSize/%s),COUNT(*)/%s from FileToFTS WHERE SubmissionTime > (NOW() - INTERVAL %s SECOND) AND Status = 'Completed' GROUP BY ChannelID;" % (interval,interval,interval)
    res = self._query(req)
    if not res['OK']:
      err = 'TransferDB._getFTSObservedThroughput: Failed to obtain observed throughput.'
      return S_ERROR('%s\n%s' % (err,res['Message']))
    channelDict = {}
    for channelID,throughput,fileput in res['Value']:
      channelDict[channelID] = {'Throughput': float(throughput),'Fileput': float(fileput)}
    for channelID in channelIDs:
      if not channelDict.has_key(channelID):
        channelDict[channelID] = {'Throughput': 0,'Fileput': 0}
    return S_OK(channelDict)
      
  #################################################################################
  # These are the methods for managing the FTSReqLogging table

  def addLoggingEvent(self,ftsReqID,event):
    req = "INSERT INTO FTSReqLogging (FTSReqID,Event,EventDateTime) VALUES (%s,'%s',NOW());" % (ftsReqID,event)
    res = self._update(req)
    if not res['OK']:
      err = "TransferDB._addLoggingEvent: Failed to add logging event to FTSReq %s" % ftsReqID
      return S_ERROR(err)
    return res 
