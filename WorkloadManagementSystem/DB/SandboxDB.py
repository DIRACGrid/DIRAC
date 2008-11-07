########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/SandboxDB.py,v 1.13 2008/11/07 11:10:07 atsareg Exp $
########################################################################
""" SandboxDB class is a simple storage using MySQL as a container for
    relatively small sandbox files. The file size is limited to 16MB.
    The following methods are provided

    addLoggingRecord()
    getJobLoggingInfo()
    getWMSTimeStamps()
"""

__RCSID__ = "$Id: SandboxDB.py,v 1.13 2008/11/07 11:10:07 atsareg Exp $"

import re, os, sys, threading
import time, datetime
from types import *

from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
import DIRAC.Core.Utilities.Time as Time

#############################################################################
class SandboxDB(DB):

  def __init__( self, sandbox_type, maxQueueSize=10 ):
    """ Standard Constructor
    """

    DB.__init__(self,sandbox_type,'WorkloadManagement/SandboxDB',maxQueueSize)

    self.maxSize = gConfig.getValue( self.cs_path+'/MaxSandboxSize', 16 )
    self.maxPartitionSize = gConfig.getValue( self.cs_path+'/MaxPartitionSize', 2 )
    self.maxPartitionSize *= 1024*1024*1024 # in GBs

    self.maxPartitionSize = 15000000

    self.lock = threading.Lock()

#############################################################################
  def storeFile(self,jobID,filename,fileString,sandbox):
    """ Store input sandbox ASCII file for jobID with the name filename which
        is given with its string body
    """

    result = self.__getWorkingPartition(sandbox)
    if not result['OK']:
      return result
    pTable = result['Value']

    fileSize = len(fileString)
    if fileSize > self.maxSize*1024*1024:
      return S_ERROR('File size too large %.2f MB for file %s' % \
                     (fileSize/1024./1024.,filename))

    # Check that the file does not exist already
    req = "SELECT FileName,FileLink,Partition FROM %s WHERE JobID=%d AND FileName='%s'" % \
          (sandbox,int(jobID),filename)
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) > 0:
      fileLink = result['Value'][0][1]
      partTable = result['Value'][0][2]
      fJobID = 0
      if fileLink and fileLink.find('part') == 0:
        fJobID = fileLink[5:].split('/')[2] 
      # Remove the already existing file - overwrite
      gLogger.warn('Overwriting file %s for job %d' % (filename,int(jobID)))
      req = "DELETE FROM %s WHERE JobID=%d AND FileName='%s'" % \
            (sandbox,int(jobID),filename)
      result = self._update(req)
      if not result['OK']:
        return result
      if partTable and fJobID == jobID:
        req = "DELETE FROM %s WHERE JobID=%d AND FileName='%s'" % \
              (partTable,int(jobID),filename)
        result = self._update(req)
        if not result['OK']:
          return result

    #inFields = ['JobID','FileName','FileBody','FileLink','UploadDate']
    #inValues = [jobID,filename,'','part:/%s/%d/%s' % (pTable,jobID,filename),Time.dateTime()]
    req = "INSERT INTO %s (JobID,FileName,FileSize,Partition,UploadDate) VALUES (%d,'%s',%d,'%s',UTC_TIMESTAMP())" % \
          (sandbox,jobID,filename,fileSize,pTable)
    #result = self._insert(sandbox,inFields,inValues)
    result = self._update(req)
    if not result['OK']:
      return result

    inFields = ['JobID','FileName','FileBody','FileSize']
    inValues = [jobID,filename,fileString,len(fileString)]
    result = self._insert(pTable,inFields,inValues)
    if not result['OK']:
      return result

    result = self.__updatePartitionSize(sandbox,pTable)
    if not result['OK']:
      return S_ERROR('Failed to update partition size for %s' % pTable)

    return S_OK()

  def __updatePartitionSize(self,sandbox,partition):
    """ Update the size information for the given partition
    """

    result = self.__getTableSize(partition)
    if not result['OK']:
      return S_ERROR('Can not get the %s table size' % partition)
    tableSize = result['Value']
    result = self.__getTableContentsSize(partition)
    if not result['OK']:
      return S_ERROR('Can not get the %s partition data size' % partition)
    dataSize = result['Value']

    partID = partition.split('_')[1]
    req = "UPDATE %sPartitions SET DataSize=%d, TableSize=%d, LastUpdate=UTC_TIMESTAMP() WHERE PartID=%d" % (sandbox,int(dataSize),int(tableSize),int(partID))
    result = self._update(req)
    return result 

  def __getTableSize(self,table):
    """ Get the table size in bytes
    """

    req = "SHOW TABLE STATUS LIKE '%s'" % table
    result = self._query(req)
    if not result['OK']:
      return result

    if not result['Value']:
      return S_ERROR('No result returned from the database')

    size = int(result['Value'][0][6])
    return S_OK(size)

  def __getTableContentsSize(self,table):
    """ Get the table size in bytes
    """

    req = "SELECT SUM(FileSize) FROM %s" % table
    result = self._query(req)
    if not result['OK']:
      return result

    size = int(result['Value'][0][0])
    return S_OK(size)

  def __getCurrentPartition(self,sandbox):
    """ Get the current sandbox partition number
    """

    req = "SELECT PartID,DataSize,TableSize FROM %sPartitions ORDER BY TableSize ASC LIMIT 1" % sandbox
    result = self._query(req)
    if not result['OK']:
      return result

    partID = 0
    dataSize = 0
    tableSize = 0
    if result['Value']:
      partID = int(result['Value'][0][0])
      dataSize = int(result['Value'][0][1])
      tableSize = int(result['Value'][0][2])
    result = S_OK(partID)
    result['DataSize'] = dataSize
    result['TableSize'] = tableSize
    return result

  def __getWorkingPartition(self,sandbox):
    """ Get the working partition
    """

    result = self.__getCurrentPartition(sandbox)
    if not result['OK']:
      return result

    sprefix = "IS"
    if sandbox == "OutputSandbox":
      sprefix = "OS"

    partID = result['Value']
    tableSize = result['TableSize']
    if partID == 0:
      result = self.__createPartition(sandbox)
      if not result['OK']:
        return result
      partID = result['Value']
      return S_OK('%s_%d' % (sprefix,partID))

    if tableSize > self.maxPartitionSize:
      result = self.__createPartition(sandbox)
      if not result['OK']:
        return result
      partID = result['Value']

    return S_OK('%s_%d' % (sprefix,partID))

  def __createPartition(self,sandbox):
    """ Create new snadbox partition
    """

    sprefix = "IS"
    if sandbox == "OutputSandbox":
      sprefix = "OS"
    self.lock.acquire()
    req = "INSERT INTO %sPartitions (CreationDate,LastUpdate) VALUES (UTC_TIMESTAMP(),UTC_TIMESTAMP())" % sandbox
    result = self._getConnection()
    if result['OK']:
      connection = result['Value']
    else:
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    res = self._update(req,connection)
    if not res['OK']:
      self.lock.release()
      return res
    req = "SELECT LAST_INSERT_ID();"
    res = self._query(req,connection)
    self.lock.release()
    partID = int(res['Value'][0][0])

    req = """CREATE TABLE %s_%d(
    JobID INTEGER NOT NULL,
    FileName VARCHAR(255) NOT NULL,
    FileBody LONGBLOB NOT NULL,
    FileSize INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (JobID,FileName)
) ENGINE=MyISAM MAX_ROWS=150000 AVG_ROW_LENGTH=150000;
""" % (sprefix,partID)

    result = self._update(req)
    if not result['OK']:
      return S_ERROR('Failed to create new Sandbox partition')

    return S_OK(partID)

#############################################################################
  def getSandboxFile(self,jobID,filename,sandbox):
    """ Store input sandbox ASCII file for jobID with the name filename which
        is given with its string body
    """

    req = "SELECT FileBody,FileLink,Partition FROM %s WHERE JobID=%d AND FileName='%s'" % \
          (sandbox, int(jobID), filename)

    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      return S_ERROR('Sandbox file not found')

    body = result['Value'][0][0]
    partition = result['Value'][0][2]
    fileLink = result['Value'][0][1]
    fJobID = jobID
    fname = filename
    if fileLink:
      if fileLink.find('part') == 0:
        dummy,partition,fJobID,fname = fileLink[5:].split('/') 
    if body and not partition:
      req = "UPDATE %s SET RetrieveDate=UTC_TIMESTAMP() WHERE JobID=%d AND FileName='%s'" % \
            (sandbox, int(jobID), filename)
      result = self._update(req)
      return S_OK(body)

    if partition:
      req = "SELECT FileBody FROM %s WHERE JobID=%d AND FileName='%s'" % \
            (partition, int(fJobID), fname)
      result = self._query(req)
      if not result['OK']:
        return result
      if len(result['Value']) == 0:
        return S_ERROR('Sandbox file not found')
      body = result['Value'][0][0]
      req = "UPDATE %s SET RetrieveDate=UTC_TIMESTAMP() WHERE JobID=%d AND FileName='%s'" % \
            (sandbox, int(jobID), filename)
      result = self._update(req)
      return S_OK(body)
    else:
      return S_ERROR('Sandbox file not found')

#############################################################################
  def getFileNames(self,jobID,sandbox):
    """ Get file names for a given job in a given sandbox
    """

    req = "SELECT FileName FROM %s WHERE JobID=%d" % (sandbox,int(jobID))
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      return S_ERROR('No files found for job %d' % int(jobID))

    fileList = [ x[0] for x in result['Value']]
    return S_OK(fileList)

#############################################################################
  def getSandboxStats(self,sandbox):
    """ Get sandbox statistics
    """

    req = "SELECT SUM(DataSize),SUM(TableSize) FROM %sPartitions" % sandbox
    result = self._query(req)
    if not result['OK']:
      return result
    dataSize = int(result['Value'][0][0])
    tableSize = int(result['Value'][0][1])

    req = "SELECT COUNT(PartID) FROM %sPartitions" % sandbox
    result = self._query(req)
    if not result['OK']:
      return result
    nParts = result['Value'][0][0]

    req = "SELECT COUNT(JobID) FROM %s WHERE FileSize > 0" % sandbox
    result = self._query(req)
    if not result['OK']:
      return result
    nFiles = result['Value'][0][0]
    req = "SELECT COUNT(JobID) FROM %s" % sandbox
    result = self._query(req)
    if not result['OK']:
      return result
    nEntries = result['Value'][0][0]
    resultDict = {}
    resultDict['DataSize'] = dataSize
    resultDict['OnDiskSize'] = tableSize
    resultDict['NumberOfFiles'] = nFiles
    resultDict['NumberOfLinks'] = nEntries - nFiles
    resultDict['NumberOfPartitions'] = nParts
    resultDict['MaxPartitionSize'] = self.maxPartitionSize

    print resultDict

    return S_OK(resultDict)

#############################################################################
  def removeJob(self,jobID,sandbox):
    """ Remove all the files belonging to the given job
    """

    req = "SELECT FileName,FileLink,Partition FROM %s WHERE JobID=%d" % (sandbox,int(jobID))
    result = self._query(req)
    if not result['OK']:
      return result

    if not result['Value']:
      return S_OK()

    for fname,flink,partition in result['Value']:
      if partition:
        req = "DELETE FROM %s WHERE JobID=%d" % (partition,int(jobID))
        result = self._update(req)
        if not result['OK']:
          gLogger.warn('Failed to remove files for job %d' % jobID)
          return result 
      if flink and link.find('part') == 0:
        dummy,pTable,jID,fname = flink[5:].split('/')
        if jID == jobID:
          req = "DELETE FROM %s WHERE JobID=%d" % (partition,int(jobID))
          result = self._update(req)
          if not result['OK']:
            gLogger.warn('Failed to remove files for job %d' % jobID)
            return result

    req = "DELETE FROM %s WHERE JobID=%d" % (sandbox,int(jobID))
    result = self._update(req)
    if not result['OK']:
      gLogger.warn('Failed to remove files for job %d' % jobID)
      return result

    gLogger.info('Removed files for job %d' % jobID)
    return S_OK()
