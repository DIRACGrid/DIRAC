""" RequestDB is a front end to the Request Database.
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
import threading,types

gLogger.initialize('DMS','/Databases/TransferDB/Test')

class TransferDB(DB):

  def __init__(self, systemInstance ='Default', maxQueueSize=10 ):
    DB.__init__(self,'TransferDB','RequestManagement/RequestDB',maxQueueSize)
    self.getIdLock = threading.Lock()

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

  def addFileToChannel(self,channelID,fileID,sourceSURL,targetSURL):
    res = self.checkFileChannelExists(channelID, fileID)
    if not res['OK']:
      err = 'TransferDB._addFileToChannel: Failed check existance of File %s on Channel %s.' % (fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if res['Value']:
      err = 'TransferDB._addFileToChannel: File %s already exists on Channel %s.' % (fileID,channelID)
      return S_ERROR(err)
    req = "INSERT INTO Channel (ChannelID,FileID,SourceSURL,TargetSURL,SubmitTime,Status) VALUES (%s,%s,'%s','%s',NOW(),'Waiting');" % (channelID,fileID,sourceSURL,targetSURL)
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

  def removeFileFromChannel(self,channelID,fileID):
    req = "DELETE FROM Channel WHERE ChannelID = %s and FileID = %s;" % (channelID,fileID)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._removeFileFromChannel: Failed to remove File %s from Channel %s.' % (fileID,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res

  def setFileChannelStatus(self,channelID,fileID,status):
    req = "UPDATE Channel SET Status = '%s' WHERE ChannelID = %s and FileID = %s;" % (status,channelID,fileID)
    res = self._update(req)
    if not res['OK']:
      err = 'TransferDB._setFileChannelStatus: Failed to set File %s status to %s on Channel %s.' % (fileID,status,channelID)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    return res




