########################################################################
# $Id$
########################################################################
""" DIRAC Transformation DB

    Transformation database is used to collect and serve the necessary information
    in order to automate the task of job preparation for high level transformations.
    This class is typically used as a base class for more specific data processing
    databases
"""

__RCSID__ = "$Id$"

from DIRAC                                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                import DB
from DIRAC.DataManagementSystem.Client.ReplicaManager                  import ReplicaManager
from DIRAC.Core.DISET.RPCClient                                        import RPCClient
from DIRAC.Core.Utilities.List                                         import stringListToString, intListToString
from DIRAC.Core.Utilities.SiteSEMapping                                import getSEsForSite, getSitesForSE

from types import *
import re,time,string,threading

MAX_ERROR_COUNT = 3

#############################################################################

class TransformationDB(DB):

  def __init__(self, dbname, dbconfig, maxQueueSize=10 ):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """
    DB.__init__(self,dbname, dbconfig, maxQueueSize)
    self.lock = threading.Lock()
    self.dbname = dbname
    res = self.__updateFilters()
    if not res['OK']:
      gLogger.fatal("Failed to create filters")
    self.TRANSPARAMS = [  'TransformationID',
                          'TransformationName',
                          'Description',
                          'LongDescription',
                          'CreationDate',
                          'LastUpdate'
                          'AuthorDN',
                          'AuthorGroup',
                          'Type',
                          'Plugin',
                          'AgentType',
                          'Status',
                          'FileMask',
                          'TransformationGroup',
                          'GroupSize',
                          'InheritedFrom',
                          'Body',
                          'MaxNumberOfJobs',
                          'EventsPerJob']
    self.mutable = [      'TransformationName',
                          'Description',
                          'LongDescription',
                          'AgentType',
                          'Status',
                          'MaxNumberOfJobs']

  def getName(self):
    """  Get the database name
    """
    return self.dbname

  ###########################################################################
  #
  # These methods manipulate the Transformations table
  #

  def addTransformation(self, name, description, longDescription,authorDN, authorGroup, type, plugin,agentType,fileMask,
                        transformationGroup = 'General',
                        groupSize           = 1,
                        inheritedFrom       = 0,
                        body                = '', 
                        maxJobs             = 0,
                        eventsPerJob        = 0,
                        addFiles            = True):
    """ Add new transformation definition including its input streams
    """
    connection = self.__getConnection(connection)
    res  = self.__getTransformationID(transName,connection)
    if res['OK']:
      return S_ERROR("Transformation with name %s already exists with TransformationID = %d" % (transName,res['Value']))
    elif res['Message'] != "Transformation does not exist":
      return res
    self.lock.acquire()
    req = "INSERT INTO Transformations (TransformationName,Description,LongDescription, \
                                        CreationDate,LastUpdate,AuthorDN,AuthorGroup,Type,Plugin,AgentType,\
                                        FileMask,Status,TransformationGroup,GroupSize,\
                                        InheritedFrom,Body,MaxNumberOfJobs,EventsPerJob)\
                                VALUES ('%s','%s','%s',\
                                        UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s','%s','%s','%s','%s',\
                                        '%s','New','%s',%d,\
                                        %d,'%s',%d,%d);" % \
                                      (name, description, longDescription,
                                       authorDN, authorGroup, type, plugin, agentType,
                                       fileMask,transformationGroup,groupSize,
                                       inheritedFrom,body,maxJobs,eventsPerJob)
    res = self._update(req,connection)
    if not res['OK']:
      self.lock.release()
      return res
    transID = res['Value']['lastRowId']
    self.lock.release()
    # If the transformation has an input data specification
    if fileMask or inheritedFrom:
      self.filters.append((transID,re.compile(fileMask)))
      # Add already existing files to this transformation if any
      res = self.__createTransformationTable(transID,connection)
      if not res['OK']:
        self.deleteTransformation(transID,connection)
        return res
      
    if inheritedFrom:
      res  = self.__getTransformationID(inheritedFrom,connection)
      if not res['OK']:
        gLogger.error("Failed to get ID for parent transformation",res['Message'])
        self.deleteTransformation(transID,connection)
        return res
      originalID = res['Value']
      res = self.setTransformationStatus(originalID,'Stopped')
      if not res['OK']:
        gLogger.error("Failed to update parent transformation status",res['Message'])
        self.deleteTransformation(transID,connection)
        return res
      message = 'Status changed to "Stopped" due to creation of the Derived Production (%d)' % transID
      self.updateTransformationLogging(originalID,message,authorDN,connection)
      res = self.__getTransformationFiles(originalID,connection)
      if not res['OK']:
        self.deleteTransformation(transID,connection)
        return res
      req = "INSERT INTO T_%d (Status,JobID,FileID,TargetSE,UsedSE,LastUpdate) VALUES" % transID
      for fileID,status,taskID,targetSE,usedSE in result['Value']:
        if taskID:
          taskID = str(int(originalID)).zfill(8)+'_'+str(int(taskID)).zfill(8)
        req = "%s ('%s','%s',%d,'%s','%s',UTC_TIMESTAMP())," % (req,status,taskID,fileID,targetSE,usedSE)
      req = req.rstrip(",")
      res = self._update(req,connection)
      if not res['OK']:
        self.deleteTransformation(transID,connection)
        return res
    if addFiles and fileMask:
      self.__addExistingFiles(transID,connection)
    return S_OK(transID)

  def getTransformations(self,condDict={},older=None, newer=None, timeStamp='CreationDate', orderAttribute=None, limit=None, extraParams=False,connection=False):
    """ Get parameters of all the Transformations with support for the web standard structure
    """
    connection = self.__getConnection(connection)
    req = "SELECT TransformationID,TransformationName,Description,LongDescription,CreationDate,LastUpdate,\
          AuthorDN,AuthorGroup,Type,Plugin,AgentType,Status,FileMask,TransformationGroup,GroupSize,\
          InheritedFrom,Body,MaxNumberOfJobs,EventsPerJob FROM Transformations %s" % self.buildCondition(condDict, older, newer, timeStamp)
    if orderAttribute:
      orderType = None
      orderField = orderAttribute
      if orderAttribute.find(':') != -1:
        orderField = orderAttribute.split(':')[0]
        orderType = orderAttribute.split(':')[1].upper()
      req = "%s ORDER BY %s" % (req,orderField)
      if orderType:
        req = "%s %s" % (req,orderType)
    if limit:
      req = "%s LIMIT %d" % (req,limit)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = []
      for item in raw:
        if type(item) not in [IntType,LongType]:
          rList.append(str(item))
        else:
          rList.append(item)
      webList.append(rList)
      transDict = {}
      transDict['TransID'] = row[0]
      transDict['Name'] = row[1]
      transDict['Description'] = row[2]
      transDict['LongDescription'] = row[3]
      transDict['CreationDate'] = row[4]
      transDict['LastUpdate'] = row[5]
      transDict['AuthorDN'] = row[6]
      transDict['AuthorGroup'] = row[7]
      transDict['Type'] = row[8]
      transDict['Plugin'] = row[9]
      transDict['AgentType'] = row[10]
      transDict['Status'] = row[11]
      transDict['FileMask'] = row[12]
      transDict['TransformationGroup'] = row[13]
      transDict['GroupSize'] = row[14]
      transDict['InheritedFrom'] = row[15]
      transDict['Body'] = row[16]
      transDict['MaxNumberOfJobs'] = row[17]
      transDict['EventsPerJob'] = row[18]
      if extraParams:
        req = "SELECT ParameterName,ParameterValue FROM AdditionalParameters WHERE TransformationID = %d" % transDict['TransID']
        res = self._query(req,connection)
        if not res['OK']:
          return res
        for parameterName,parameterValue in res['Value']:
          transDict[parameterName] = parameterValue
      resultList.append(transDict) 
    result = S_OK(resultList)
    result['Records'] = webList
    result['ParameterNames'] = self.TRANSPARAMS
    return S_OK(transDict)
  
  def getTransformation(self,transName,extraParams=False,connection=False):
    """Get Transformation definition and parameters of Transformation identified by TransformationID
    """
    res = self.getTransformations(condDict = {'TransformationName':transName},extraParams=extraParams,connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Transformation %s did not exist" % transName)
    return S_OK(res['Value'].values()[0])

  def getTransformationParameters(self,transName,parameters,connection=False):
    """ Get the requested parameters for a supplied transformation """
    if type(parameters) in StringTypes:
      parameters = [parameters]
    extraParams = False
    for param in parameters:
      if not param in self.TRANSPARAMS:
        extraParams = True
    res = self.getTransformation(transName,extraParams=extraParams,connection=connection)
    if not res['OK']:
      return res
    transParams = res['Value']
    paramDict = {}
    for reqParam in parameters:
      if not reqParam in transParams.keys():
        return S_ERROR("Parameter %s not defined for transformation" % reqParam) 
      paramDict[reqParam] = transParams[reqParam]
    if len(paramDict) == 1:
      return S_OK(paramDict[reqParam])  
    return S_OK(paramDict)
  
  def getTransformationWithStatus(self,status,connection=False):
    """ Gets a list of the transformations with the supplied status """
    req = "SELECT TransformationID FROM Transformations WHERE Status = '%s';" % status
    res = self._query(req)
    if not res['OK']:
      return res
    transIDs = []  
    for tuple in res['Value']:
      transIDs.append(tuple[0])
    return S_OK(transIDs)

  def getDistinctAttributeValues(self, attribute, selectDict, connection=False):
    """ Get distinct values of the given transformation attribute """
    if not attribute in self.TRANSPARAMS:
      return S_ERROR('Can not serve values for attribute %s' % attribute) 
    return self.getDistinctAttributeValues('Transformations',attribute,condDict=selectDict,connection=connection)

  def __updateTransformationParameter(self,transID,paramName,paramValue,connection=False):
    if not (paramName in self.mutable):
      return S_ERROR("Can not update the '%s' transformation parameter" % paramName)
    req = "UPDATE Transformations SET %s='%s', LastUpdate=UTC_TIMESTAMP() WHERE TransformationID=%d" % (paramName,paramValue,transID)
    return self._update(req,connection)

  def __getTransformationID(self,transName,connection=False):
    """ Method returns ID of transformation with the name=<name> """
    try:
      return S_OK(long(transName))
    except:
      pass
    if type(transName) not in StringTypes:
      return S_ERROR("Transformation should ID or name")
    cmd = "SELECT TransformationID from Transformations WHERE TransformationName='%s';" % transName
    res = self._query(cmd,connection)
    if not res['OK']:
      gLogger.error("Failed to obtain transformation ID for transformation","%s:%s" % (name,res['Message']))
      return res
    elif not res['Value']:
      gLogger.verbose("Transformation with name %s does not exists" % (transName))
      return S_ERROR("Transformation does not exist")
    return S_OK(res['Value'][0][0])

  def __deleteTransformation(self,transID,connection=False):
    req = "DELETE FROM Transformations WHERE TransformationID=%d;" % transID
    return self._update(req,connection)
  
  def __updateFilters(self,connection=False):
    """ Get filters for all defined input streams in all the transformations.
        If transID argument is given, get filters only for this transformation.
    """
    resultList = []
    # Define the general filter first
    setup = gConfig.getValue('/DIRAC/Setup','')
    value = gConfig.getValue('/Operations/InputDataFilter/%s/%sFilter' % (setup,self.database_name),'')
    if value:
      refilter = re.compile(value)
      resultList.append((0,refilter))
    # Per transformation filters
    req = "SELECT TransformationID,FileMask FROM Transformations;"
    res = self._query(req,connection)
    if not res['OK']:
      return res
    for transID,mask in res['Value']:
      if mask:
        refilter = re.compile(mask)
        resultList.append((transID,refilter))
    self.filters = resultList
    return S_OK(resultList)

  def __filterFile(self,lfn,filters=None):
    """Pass the input file through a supplied filter or those currently active """
    result = []
    if filters:
      for transID,refilter in filters:
        if refilter.search(lfn):
          result.append(transID)
    else:
      for transID,refilter in self.filters:
        if refilter.search(lfn):
          result.append(transID)
    return result
  
  def getTransformationLastUpdate(self,transName,connection=False):
    #TODO: Eventually remove this once the ProductionCleaningAgent is updated
    return self.getTransformationParameters(transName,'LastUpdate',connection)

  def setTransformationStatus(self,transName,status,connection=False):
    #TODO: Update where this is used
    return self.setTransformationParameter(transName,'Status',paramValue,connection=connection)

  def setTransformationAgentType(self,transName,status,connection=False):
    #TODO: Update where this is used
    return self.setTransformationParameter(transName,'AgentType',paramValue,connection=connection)

  ###########################################################################
  #
  # These methods manipulate the AdditionalParameters tables
  #
  def setTransformationParameter(self,transName,paramName,paramValue,connection=False):
    """ Add a parameter for the supplied transformations """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    if (paramName in self.TRANSPARAMS):
      return self.__updateTransformationParameter(transID,paramName,paramValue,connection)
    return self.__addAdditionalTransformationParameter(transID, paramName, paramValue, connection)

  def __addAdditionalTransformationParameter(self,transID,paramName,paramValue,connection=False):
    req = "DELETE FROM AdditionalParameters WHERE TransformationID=%d AND ParameterName='%s'" % (transID,paramName)
    res = self._update(req,connection)
    if not res['OK']:
      return res
    req = "INSERT INTO AdditionalParameters (TransformationID,ParameterName,ParameterValue) VALUES (%s,'%s','%s');" % (transID,paramName,paramValue)
    return self._update(req,connection)

  def __deleteTransformationParameters(self,transID,connection=False):
    """ Remove the parameters associated to a transformation """
    req = "DELETE FROM AdditionalParameters WHERE TransformationID=%d;" % transID
    return self._update(req,connection)

  ###########################################################################
  #
  # These methods manipulate the T_* tables
  #

  def addFilesToTransformation(self,transName,lfns,connection=False):
    """ Add a list of LFNs to the transformation directly """
    if not lfnList:
      return S_ERROR('Zero length LFN list')
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    fileIDs = self.__getFileIDsForLfns(lfns, connection)
    failed = {}
    successful = {}
    for lfn in lfns:
      if lfn not in fileIDs.values():
        failed[lfn] = 'File not found in the Transformation Database'
    res = self.__addFilesToTransformation(transID, fileIDs.keys(), connection)  
    if not res['OK']:
      return res
    for fileID in fileIDs.keys():
      successful[lfn] = "Present"
      if not fileID in res['Value']:
        successful[lfn] = "Added"
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)
  
  def getTransformationFiles(self,transName,condDict={},older=None, newer=None, timeStamp='LastUpdate', orderAttribute=None, limit=None,connection=False):
    """ Get files for the supplied transformations with support for the web standard structure
    """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    self.TRANSFILEPARAMS = ['FileID','Status','JobID','TargetSE','UsedSE','ErrorCount','LastUpdate']
    req = "SELECT %s FROM T_%d" % (intListToString(self.TRANSFILEPARAMS),transID)
    if condDict or older or newer:
      req = "%s %s" % self.buildCondition(condDict, older, newer, timeStamp)
    if orderAttribute:
      orderType = None
      orderField = orderAttribute
      if orderAttribute.find(':') != -1:
        orderField = orderAttribute.split(':')[0]
        orderType = orderAttribute.split(':')[1].upper()
      req = "%s ORDER BY %s" % (req,orderField)
      if orderType:
        req = "%s %s" % (req,orderType)
    if limit:
      req = "%s LIMIT %d" % (req,limit)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    transFiles = res['Value']
    fileIDs = [int(row[0]) for row in transFiles]
    res = self.__getLfnsForFileIDs(fileIDs,connection=connection)
    if not res['OK']:
      return res
    fileIDLfns = res['Value'][1]
    webList = []
    resultList = []
    for row in transFiles:
      fileID,status,taskID,targetSE,usedSE,errorCount,lastUpdate = row
      lfn = fileIDLfns[row[0]]
      # Prepare the structure for the web
      rList = [lfn]
      for item in raw:
        if type(item) not in [IntType,LongType]:
          rList.append(str(item))
        else:
          rList.append(item)
      webList.append(rList)
      # Now prepare the standard dictionary
      fDict = {}
      fDict['LFN'] = lfn
      fDict['FileID'] = fileID
      fDict['Status'] = status
      fDict['JobID'] = taskID
      fDict['TargetSE'] = targetSE
      fDict['UsedSE'] = usedSE
      fDict['ErrorCount'] = errorCount
      fDict['LastUpdate'] = lastUpdate
      resultList.append(fileDict)
    result = S_OK(resultList)
    result['LFNs'] = fileIDLfns.values()
    result['Records'] = webList
    result['ParameterNames'] = ['LFN'].extend(self.TRANSFILEPARAMS)
    return result

  def setFileStatusForTransformation(self,transName,status,lfns,force=False,connection=False):
    """ Set file status for the given transformation """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    fileIDs = self.__getFileIDsForLfns(lfns,connection=connection)
    if not fileIDs:
      return S_ERROR('Files not found in the Transformation Database')
    successful = {}
    failed = {}
    for lfn in lfns:
      if lfn not in fileIDs.values():
        failed[lfn] = 'File not found in the Transformation Database'
    res = self.getTransformationFiles(transID,condDict={'FileID':fileIDs.keys()},connection=connection)
    if not res['OK']:
      return res
    transFiles = res['Value']
    for fileDict in transFiles:
      currentStatus = fileDict['Status']
      errorCount = fileDict['ErrorCount']
      lfn = fileDict['LFN']
      fileID = fileDict['FileID']
      if (currentStatus.lower() == "processed") and (status.lower() != "processed"):
        failed[lfn] = 'Can not change Processed status'
      elif (currentStatus == status):
        successful[lfn] = 'Status not changed'
      elif (status.lower() == 'unused') and (errorCount >= MAX_ERROR_COUNT) and (not force):
        failed[lfn] = 'Max number of resets reached'
        req = "UPDATE T_%d SET Status='MaxReset', LastUpdate=UTC_TIMESTAMP() WHERE FileID=%d;" % (transID,fileID)
      elif (status.lower() == 'unused'):
        req = "UPDATE T_%d SET Status='%s', LastUpdate=UTC_TIMESTAMP(),ErrorCount=ErrorCount+1 WHERE FileID=%d;" % (transID,status,fileID)
      else:
        req = "UPDATE T_%d SET Status='%s', LastUpdate=UTC_TIMESTAMP() WHERE FileID=%d;" % (transID,status,fileID)
      if not req:
        continue
      res = self._update(req,connection)
      if failed.has_key(lfn) or successful.has_key(lfn):
        continue
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = 'Status updated to %s' % status
    return S_OK({"Successful":successful,"Failed":failed})
  

  def getTransformationStats(self,transName,connection=False):
    """ Get number of files in Transformation Table for each status """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.getCounters('T_%s' % transID,['Status'],{})
    if not res['OK']:
      return res
    statusDict = {}
    total=0
    for attrDict,count in res['Value']:
      status = attrDict['Status']
      statusDict[status]=count
      total += count
    statusDict['Total']=total
    return S_OK(statusDict)
  
  def __createTransformationTable(self,transID,connection=False):
    """ Add a new Transformation table for a given transformation """
    req = "DROP TABLE IF EXISTS T_%d;" % transID
    self._update(req,connection)
    req = """CREATE TABLE T_%d(
FileID INTEGER NOT NULL,
Status VARCHAR(32) DEFAULT "Unused",
INDEX (Status),
ErrorCount INT(4) NOT NULL DEFAULT 0,
JobID VARCHAR(32),
TargetSE VARCHAR(32) DEFAULT "Unknown",
UsedSE VARCHAR(32) DEFAULT "Unknown",
LastUpdate DATETIME,
PRIMARY KEY (FileID)
)""" % str(transID)
    res = self._update(req,connection)
    if not res['OK']:
      return S_ERROR("TransformationDB.__addTransformationTable: Failed to add new transformation table",res['Message'])
    return S_OK()

  def __addFilesToTransformation(self,transID,fileIDs,connection=False):
    #TODO: Must add the files mapping to the FileTranformations table
    req = "SELECT FileID from T_%d WHERE FileID IN (%s);" % intListToString(fileIDs)
    res = self._update(req,connection)
    if not res['OK']:
      return res
    for tuple in res['Value']:
      fileIDs.remove(tuple[0])
    if not fileIDs:
      return S_OK()
    req = "INSERT INTO T_%d (FileID,LastUpdate) VALUES" % transID
    for fileID in fileIDs:
      req = "%s (%d,UTC_TIMESTAMP())," % (req,fileID)
    req = req.rstrip(',')
    res = self._update(req,connection)
    if not res['OK']:
      return res
    return S_OK(fileIDs)

  def __addExistingFiles(self,transID,connection=connection):
    """ Add files that already exist in the DataFiles table to the transformation specified by the transID """
    for tID,filter in self.filters:
      if tid == transID:
        filters = [(tid,filter)]
        break
    if not filters:
      return S_ERROR('No filters defined for transformation %d' % transID)
    fileIDs = self.__getFileIDsForLfns(lfns=[],connection=connection)
    passFilter = []
    for fileID,lfn in fileIDs.items():
      if self.__filterFile(lfn,filters):
        passFilter.append(fileID)
    return self.__addFilesToTransformation(transID, passFilter, connection)
  
  def __assignTransformationFile(self,transID,taskID,se,fileIDs,connection=False):
    """ Make necessary updates to the T_* table for the newly created task """
    req = "UPDATE T_%d SET JobID='%d',UsedSE='%s',Status='Assigned',LastUpdate=UTC_TIMESTAMP() WHERE FileID IN (%s);" % (transID,taskID,se,intListToString(fileIDs.keys()))
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to assign file to task",res['Message'])
    return res
  
  def __setTransformationFileStatus(self,transID,fileIDs,status,connection=False):
    req = "UPDATE T_%d SET Status = '%s' WHERE FileID IN (%s);" % (transID,status,intListToString(fileIDs))
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to update file status",res['Message'])                                                                
    return res

  def __resetTransformationFile(self,transID,taskID,connection=False):	
    req = "UPDATE T_%d SET JobID=NULL, UsedSE='Unknown', Status='Unused' WHERE JobID=%d;" % (transID,taskID)
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to reset transformation file",res['Message'])
    return res

  def __getTransformationFiles(self,transID,connection=False):
    req = "SELECT FileID,Status,JobID,TargetSE,UsedSE from T_%d WHERE Status != 'Unused';" % (originalProdID)
    res = self._query(req,connection)
    if not res['OK']:
      gLogger.error("Failed to get transformation files",res['Message'])                                                                
    return res

  def __deleteTransformationFiles(self,transID,connection=False):
    """ Remove the files associated to a transformation """  
    req = "DROP TABLE IF EXISTS T_%d;" % transID
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to delete transformation files",res['Message'])                                                                
    return res
  
  ###########################################################################
  # To clean-up

  def addLFNsToTransformation(self,lfnList,transName):
    #TODO Update where this is used
    return self.addFilesToTransformation(transName,lfnList)
  
  def getTransformationLFNs(self,transName,status='Unused'):
    #TODO: Change this where it is used
    """  Get input LFNs for the given transformation, only files with a given transformation status."""
    res = self.getTransformationFiles(transName,condDict={'Status':status})
    if not res['OK']:
      return res
    return S_OK(res['LFNs'])
  
  def getFilesForTransformation(self,transName,jobOrdered=False):
    #TODO Change this where it is used
    """ Get files and their status for the given transformation """
    ordering = 'LFN'
    if jobOrdered:
      ordering = 'JobID'
    res = self.getTransformationFiles(transName,orderAttribute=ordering)
    if not res['OK']:
      return res
    return S_OK(res['Value'])
  
  ###########################################################################
  #
  # These methods manipulate the Jobs table
  #

  def getTransformationTaskStats(self,transName='',connection=False):
    """ Returns dictionary with number of jobs per status for the given production.
    """
    connection = self.__getConnection(connection)
    if transName:
      res  = self.__getTransformationID(transName,connection)
      if not res['OK']:
        gLogger.error("Failed to get ID for transformation",res['Message'])
        return res
      res = self.getCounters('Jobs',['WmsStatus'],{'TransformationID':res['Value']},connection=connection)
    else:
      res = self.getCounters('Jobs',['WmsStatus','TransformationID'],{},connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR('No records found')
    statusDict = {}
    for attrDict,count in res['Value']:
      status = attrDict['WmsStatus']
      statusDict[status] = count
    return S_OK(statusDict)
  
  def reserveTask(self,transName,taskID,connection=False):
    """ Reserve the taskID from transformation for submission """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']    
    res = self.__checkUpdate("Jobs","WmsStatus","Reserved",{"TransformationID":transID,"JobID":taskID},connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR('Failed to set Reserved status for job %d - already Reserved' % int(jobID) )
    # The job is reserved, update the time stamp
    res = self.setJobStatus(transID,taskID,'Reserved',connection=connection)
    if not res['OK']:
      return S_ERROR('Failed to set Reserved status for job %d - failed to update the time stamp' % int(taskID))
    return S_OK()

  def setTaskStatusAndWmsID(self,transName,taskID,status,taskWmsID,connection=False):
    """ Set status and JobWmsID for job with jobID in production with transformationID
    """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "UPDATE Jobs SET WmsStatus='%s', JobWmsID='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE TransformationID=%d AND JobID=%d;" % (status,taskWmsID,transID,taskID)
    return self._update(req,connection)

  def setTaskStatus(self,transName,taskID,status,connection=False):
    """ Set status for job with jobID in production with transformationID """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    if type(taskID) != ListType:
      taskIDList = [taskID]
    else:
      taskIDList = list(taskID)
    taskString = ','.join([ str(x) for x in taskIDList ])
    req = "UPDATE Jobs SET WmsStatus='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE TransformationID=%d AND JobID in (%s)" % (status,transID,taskString)
    return self._update(req,connection)

  def getTaskInfo(self,transName,taskID,connection=False):
    """ returns dictionary with information for given Task of given transformation ID """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "SELECT JobID,JobWmsID,WmsStatus,TargetSE,CreationTime,LastUpdateTime FROM Jobs WHERE TransformationID=%d AND JobID='%d';" % (transID,taskID)
    res = self._query(req,connection)
    # lets create dictionary
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Failed to find job with JobID=%s/TransformationID=%s in the Jobs table with message: %s" % (taskID, transID, res['Message']))
    taskDict = {}
    taskDict['JobID']=res['Value'][0][0]
    taskDict['JobWmsID']=res['Value'][0][1]
    taskDict['WmsStatus']=res['Value'][0][2]
    taskDict['TargetSE']=res['Value'][0][3]
    taskDict['CreationTime']=res['Value'][0][4]
    taskDict['LastUpdateTime']=res['Value'][0][5]
    taskDict['InputVector']=''
    res = self.getTaskInputVector(transID,taskID)
    if res['OK']:
      if res['Value'].has_key(taskID):
        taskDict['InputVector']=res['Value'][taskID]    
    return S_OK(taskDict)
  
  def getTasksForSubmission(self,transName,statusList=[],numJobs=1,site='',older=None,newer=None,connection=False):
    """ Select tasks with the given status (and site) for submission """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.selectTransformationTasks(transID,statusList,numJobs,site,older,newer,connection)
    if not res['OK']:
      return res
    tasks = res['Value']
    taskList = [ int(x[0]) for x in tasks ]
    res = self.getTaskInputVector(transID,taskList,connection)
    if not res:
      return res
    inputVectorDict = res['Value']
    # Prepare Site->SE resolution mapping
    selSEs = []
    if site:
      res = getSEsForSite(site)
      if not res['OK']:
        return res
      selSEs = res['Value']
    # Now prepare the tasks
    resultDict = {}
    for row in tasks:
      if len(resultDict) >= numTasks:
        break
      taskID = int(row[0])
      se = row[2]
      status = row[3]
      inputVector = ''
      if inputVectorDict.has_key(taskID):
        inputVector = inputVectorDict[taskID]
      if not site:
        if inputVector:
          res = getSitesForSE(se)
          if not res['OK']:
            continue
          sites = res['Value']
          site = 'Multiple'
          if len(sites) == 1:
            site = sites[0]
        else:
          site = 'ANY'      
        resultDict[taskID] = {'InputData':inputVector,'TargetSE':se,'Status':status,'Site':site}
      elif site and (se in selSEs):
        resultDict[taskID] = {'InputData':inputVector,'TargetSE':se,'Status':status,'Site':site}   
      else:
        gLogger.warn("Can not find corresponding site for se",se)
    return S_OK(resultDict)

  def selectWMSTasks(self,transName,statusList = [],newer = 0):
    """ Select tasks IDs for the given production having one of the specified
        statuses and optionally with last status update older than "newer" number
        of minutes
    """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "SELECT JobID, JobWmsID, WmsStatus FROM Jobs"
    condList = ['TransformationID=%d' % int(transID)]
    if statusList:
      statusString = ','.join(["'"+x+"'" for x in statusList])
      condList.append("WmsStatus IN (%s)" % statusString)
    if newer:
      condList.append("LastUpdateTime < DATE_SUB(UTC_TIMESTAMP(),INTERVAL %d MINUTE)" % newer)
    if condList:
      condString = " AND ".join(condList)
      req += " WHERE %s" % condString
    result = self._query(req)
    if not result['OK']:
      return result
    resultDict = {}
    if result['Value']:
      for row in result['Value']:
        if row[1] and int(row[1]) != 0:
          resultDict[int(row[1])] = (row[0],row[2])
    return S_OK(resultDict)

  def selectTransformationTasks(self,transName,statusList=[],numJobs=1,site='',older=None,newer=None,connection=False):
    """ Select tasks with the given status from the given transformation """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    condDict = {"TransformationID":transID}
    if statusList:
      condDict["WmsStatus"] = statusList
    req = "SELECT JobID,CreationTime,TargetSE,WmsStatus FROM Jobs %s" % self.buildCondition(condDict, older=older, newer=newer, timeStamp='LastUpdateTime')
    if not site:
      req += " LIMIT %d" % numJobs 
    return self._query(req,connection)

  def deleteTasks(self,transName,jobIDbottom, jobIDtop,connection=False):
    """ Delete tasks with taskID range in transformation
        TODO: merge with removeTransformationTasks
    """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "DELETE FROM Jobs WHERE TransformationID=%d AND JobID>=%d AND JobID<=%d;" % (productionID,jobIDbottom, jobIDtop)
    return self._update(req,connection)
  
  def __deleteTransformationTasks(self,transID,connection=False):
    """ Delete all the tasks from the Jobs table for transformation with TransformationID """
    req = "DELETE FROM Jobs WHERE TransformationID=%d" % transID
    return self._update(req,connection)
  
  ###########################################################################
  #
  # These methods manipulate the JobInputs table
  #

  def getTaskInputVector(self,transName,taskID,connection=False):
    """ Get input vector for the given task """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    if type(taskID) != ListType:
      taskIDList = [taskID]
    else:
      taskIDList = list(taskID)
    taskString = ','.join(["'"+str(x)+"'" for x in taskIDList])      
    req = "SELECT JobID,InputVector FROM JobInputs WHERE JobID in (%s) AND TransformationID='%d';" % (taskString,transID)
    res = self._query(req)
    inputVectorDict = {}
    if res['OK'] and res['Value']:
      for row in res['Value']:
        inputVectorDict[row[0]] = row[1] 
    return S_OK(inputVectorDict)

  def __insertTaskInputs(self,transID,taskID,lfns,connection=False):      
    vector= str.join(';',lfns)
    fields = ['TransformationID','JobID','InputVector']
    values = [transID,taskID,inputVector]
    res = self._insert('JobInputs',fields,values,connection)
    if not res['OK']:
      gLogger.error("Failed to add input vector to task %d" % taskID)
    return res

  def __deleteTransformationTaskInputs(self,transID,taskID=0,connection=False):
    """ Delete all the tasks inputs from the JobsInputs table for transformation with TransformationID """
    req = "DELETE FROM JobInputs WHERE TransformationID=%d" % transID
    if taskID:
      req = "%s AND JobID=%d" % (req,taskID)
    return self._update(req,connection)

  ###########################################################################
  #
  # These methods manipulate the FileTransformations table
  #

  def __insertFileTransformations(self,transID,fileIDs,connection=False):
   req = "INSERT INTO FileTransformations (FileID,TransformationID) VALUES"
   for fileID in fileIDs:
     req = "%s (%d, %d)," % (req,fileID,transID)
   req = req.rstrip(',')
   res = self._update(req,connection)
   if not res['OK']:
     gLogger.error("Failed to insert rows to FileTransformations",res['Message'])   
   return res
 
   def __getFileTransformations(self,fileIDs,connection=False):
    """ Get the file transformation associations """
    req = "SELECT FileID,TransformationID FROM FileTransformations WHERE FileID IN (%s);" % intListToString(fileIDs)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    fileTrans = {}
    for fileID,transID in res['Value']:
      if not fileTrans.has_key(fileID):
        fileTrans[fileID] = []
      fileTrans[fileID].append(transID)
    return S_OK(fileTrans)
  
  def __getFileTransformationFiles(self,fileIDs,connection=False):
    """ Get the file transformation associations """
    req = "SELECT FileID,TransformationID FROM FileTransformations WHERE FileID IN (%s);" % intListToString(fileIDs)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    fileTrans = {}
    for fileID,transID in res['Value']:
      if not fileTrans.has_key(transID):
        fileTrans[transID] = []
      fileTrans[transID].append(fileID)
    return S_OK(fileTrans)

  def __deleteFileTransformations(self,transID,taskID=0,connection=False):
    """ Delete all file associated to the supplied TransformationID """
    req = "DELETE FROM FileTransformations WHERE TransformationID=%d" % transID
    if taskID:
      req = "%s AND JobID=%d" % (req,taskID)
    return self._update(req,connection)

  ###########################################################################
  #
  # These methods manipulate the TransformationLog table
  #

  def updateTransformationLogging(self,transName,message,authorDN,connection=False):
    """ Update the Transformation log table with any modifications
    """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "INSERT INTO TransformationLog (TransformationID,Message,Author,MessageDate) VALUES (%s,'%s','%s',UTC_TIMESTAMP());" % (transID,message,authorDN)
    return self._update(req,connection)
  
  def getTransformationLogging(self,transName,connection=False):
    """ Get logging info from the TransformationLog table """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "SELECT TransformationID, Message, Author, MessageDate FROM TransformationLog WHERE TransformationID=%s ORDER BY MessageDate;" % (transID)
    res = self._query(req)
    if not res['OK']:
      return res
    transList = []
    for transID, message, authorDN, messageDate in res['Value']:
      transDict = {}
      transDict['TransID'] = transID
      transDict['Message'] = message
      transDict['AuthorDN'] = authorDN
      transDict['MessageDate'] = messageDate
      transList.append(transdict)
    return S_OK(transList)
  
  def __deleteTransformationLog(self,transID,connection=False):
    """ Remove the entries in the transformation log for a transformation """
    req = "DELETE FROM TransformationLog WHERE TransformationID=%d;" % transID
    return self._update(req,connection)

  ###########################################################################
  #
  # These methods manipulate the DataFiles table
  #

  def __getFileIDsForLfns(self,lfns=[],connection=False):
    """ Get file IDs for the given list of lfns """
    req = "SELECT LFN,FileID FROM DataFiles"
    if lfns:
      req = "%s WHERE LFN in (%s);" % (req,stringListToString(lfns))
    res = self._query(req,connection)
    if not res['OK']:
      return res
    fids = {}
    lfns = {}
    for lfn,fileID in res['Value']:
      fids[fileID] = lfn
      lfns[lfn] = fileID
    return S_OK((fids,lfns))

  def __getLfnsForFileIDs(self,fileIDs,connection=False):
    """ Get lfns for the given list of fileIDs """
    req = "SELECT LFN,FileID FROM DataFiles WHERE LFN in (%s);" % stringListToString(lfns)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    fids = {}
    lfns = {}
    for lfn,fileID in res['Value']:
      fids[lfn] = fileID
      lfns[fileID] = lfn
    return S_OK((fids,lfns))
  
  def __addDataFiles(self,lfns,connection=False):
    """ Add a file to the DataFiles table and retrieve the FileIDs """  
    fileIDs = self.__getFileIDsForLfns(lfns,connection=connection)
    lfnFileIDs = {}
    for fileID,lfn in fileIDs.items():
      lfnFileIDs[lfn] = fileID
    for lfn in lfns:
      if not lfn in lfnFileIDs.keys():
        req = "INSERT INTO DataFiles (LFN,Status) VALUES ('%s','New');" % lfn
        res = self._update(req,connection)
        if not res['OK']:
          return res
        lfnFileIDs[lfn] = res['Value']['lastRowId']
    return S_OK(lfnFileIDs)
  
  def __setDataFileStatus(self,fileIDs,status,connection=False):
    """ Set the status of the supplied files """
    req = "UPDATE DataFiles SET Status = '%s' WHERE FileID IN (%s);" % (status,intListToString(fileIDs))
    return self._update(req,connection)

  def __addFileTuples(self,fileTuples,connection=False):
    """ Add files and replicas """
    lfns = [x[0] for x in fileTuples ]
    res = self.__addDataFiles(lfns,connection=connection)
    if not res['OK']:
      return res
    lfnFileIDs = res['Value']
    toRemove = []
    for lfn,pfn,se in fileTuples:
      fileID =  lfnFileIDs[lfn]
      res = self.__addReplica(fileID,se,pfn,connection=connection)
      if not res['OK']:
        lfnFileIDs.pop(lfn)
    return S_OK(lfnFileIDs)

  ###########################################################################
  #
  # These methods manipulate the Replicas table
  #
	
  def __addReplica(self,fileID,se,pfn,connection=False):
    """ Add a SE,PFN for the given fileID in the Replicas table.
    """
    req = "SELECT FileID FROM Replicas WHERE FileID=%s AND SE='%s';" % (fileID,se)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    elif res['Value']:
      return S_OK()
    req = "INSERT INTO Replicas (FileID,SE,PFN) VALUES (%s,'%s','%s');" % (fileID,se,pfn)
    res = self._update(req,connection)
    if not res['OK']:
      return res
    return S_OK()
  
  def __getFileReplicas(self,fileIDs,allStatus=False,connection=False):
    fileReplicas = {}
    req = "SELECT FileID,SE,PFN,Status FROM Replicas WHERE FileID IN (%s);" % intListToString(fileIDs)
    res = self._query(req)
    if not res['OK']:
      return res
    for fileID,se,pfn,status in res['Value']:
      if (allStatus) or (status.lower() != 'problematic'):
        if not fileReplicas.has_key(fileID):
          fileReplicas[fileID] = {}
        fileReplicas[fileID][se] = pfn
    return S_OK(fileReplicas)
  
  def __deleteFileReplicas(self,fileIDs,se='',connection=False):
    req = "DELETE FROM Replicas WHERE FileID IN (%s)" % intListToString(fileIDs)
    if se:
      req = "%s AND SE = '%s';" % (req,se)
    return self._update(req,connection)

  def __updateReplicaStatus(self,fileIDs,status,se='',connection=False):
    req = "UPDATE Replicas SET Status='%s' WHERE FileID IN (%s)" % (status,intListToString(fileIDs))
    if se and (se.lower() != 'any'):
      req = "%s AND SE = '%s'" % (req,se)
    return self._update(req,connection)
  
  def __getReplicaStatus(self,fileIDs,connection=False):
    req = "SELECT FileID,SE,Status FROM Replicas WHERE FileID IN (%s);" % intListToString(fileIDs)
    return self._query(req)
  
  def __updateReplicaSE(self,fileIDs,oldSE,newSE,connection=False):
    # First check whether there are existing replicas at this SE (to avoid primary key restrictions)
    req = "SELECT FileID,SE FROM Replicas WHERE FileIDs IN (%s) AND SE = '%s';" % (intListToString(fileIDs),newSE)
    res = self._query(req,connection)
    if not res['OK']:
      return res
    for fileID,se in res['Value']:
      fileIDs.remove(fileID)
    req = "UPDATE Replicas SET SE='%s' WHERE FileID IN (%s) AND SE = '%s';" % (newSE,intListToString(fileIDs),oldSE)
    return self._update(req,connection)

  ###########################################################################
  #
  # These methods manipulate multiple tables
  #

  def addTaskForTransformation(self,transID,lfns=[],se='Unknown',connection=False):
    """ Create a new task with the supplied files for a transformation.
    """
    connection = self.__getConnection(connection)
    # Be sure the all the supplied LFNs are known to the databse
    if lfns:
      fileIDs = self.__getFileIDsForLfns(lfns,connection)
      if not fileIDs:
        gLogger.error("All files not found in the transformation database")
        return S_ERROR("All files not found in the transformation database")
      allFound = True
      for lfn in lfns:
        if lfn not in fileIDs.values():
          gLogger.error("Supplied file does not exist in the transformation database",lfn)
          allFound = False
      if not allFound:
        return S_ERROR("Not all file found in the transformation database")
    # Get the transformation ID if we have a transformation name
    res  = self.__getTransformationID(transID,connection)
    if not res['OK']:
      gLogger.error("Failed to get ID for transformation",res['Message'])
      return res
    transID = res['Value']
    # Insert the task into the jobs table and retrieve the taskID
    self.lock.acquire()
    req = "INSERT INTO Jobs (TransformationID, WmsStatus, JobWmsID, TargetSE, CreationTime, LastUpdateTime) VALUES\
     (%s,'%s','%d','%s', UTC_TIMESTAMP(), UTC_TIMESTAMP());" % (transID,'Created', 0, se)
    res = self._update(req,connection)
    if not res['OK']:
      self.lock.release()
      gLogger.error("Failed to publish task for transformation", result['Message'])
      return res
    res = self._query("SELECT LAST_INSERT_ID();", connection)
    self.lock.release()
    if not res['OK']:
      return res
    taskID = int(res['Value'][0][0])
    gLogger.verbose("Published task %d for transformation %d." % (taskID,transID))
    # If we have input data then update their status, and taskID in the transformation table
    if lfns:
      res = self.__insertTaskInputs(transID,taskID,lfns,connection)
      if not res['OK']:
        self.__removeTransformationTask(transID,taskID,connection)
        return res
      res = self.__insertFileTransformations(transID,fileIDs,connection)
      if not res['OK']:
        self.__removeTransformationTask(transID,taskID,connection)
        return res
      res = self.__assignTransformationFile(transID, taskID, se, fileIDs, connection)
      if not res['OK']:
        self.__removeTransformationTask(transID,taskID,connection)
        return res
    return S_OK(taskID)

  def extendTransformation(self, transName, nTasks, authorDN, connection=False):
    """ Extend SIMULATION type transformation by nTasks number of tasks
    """
    connection = self.__getConnection(connection)
    res  = self.getTransformation(transName,connection)
    if not res['OK']:
      gLogger.error("Failed to get transformation details",res['Message'])
      return res
    transType = res['Value']['Type']
    if transType.lower() not in ['simulation','mcsimulation']:
      return S_ERROR('Can not extend non-SIMULATION type production')
    taskIDs = []
    for task in range(nTasks):
      res = self.addTaskForTransformation(transID,connection=connection)
      if not res['OK']:
        return res
      taskIDs.append(result['Value'])
    # Add information to the transformation logging
    message = 'Transformation extended by %d tasks' % nTasks
    self.updateTransformationLogging(transName,message,authorDN,connection=connection)  
    return S_OK(taskIDs)
  
  def cleanTransformation(self,transName,connection=False):
    """ Clean the transformation specified by name or id """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__deleteTransformationFiles(transID,connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformationTasks(transID,connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformationTaskInputs(transID,connection)
    if not res['OK']:
      return res
    res = self.__deleteFileTransformations(transID,connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformationParameters(transID,connection)
    if not res['OK']:
      return res
    return S_OK(transID)
    
  def deleteTransformation(self,transName,connection=False):
    """ Remove the transformation specified by name or id """
    res = self.__getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.cleanTransformation(transID,connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformationLog(transID,connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformation(transID,connection)
    if not res['OK']:
      return res
    res = self.__updateFilters()
    if not res['OK']:
      return res
    return S_OK()

  def __removeTransformationTask(self,transID,taskID,connection=False):
    res = self.__deleteTransformationTaskInputs(transID,taskID,connection)
    if not res['OK']:
      return res
    res = self.__deleteFileTransformations(transID,taskID,connection)
    if not res['OK']:
      return res
    return self.__resetTransformationFile(transID,taskID,connection)

  def __checkUpdate(self,table,param,paramValue,selectDict = {},connection=False):
    """ Check whether the update will perform an update """
    req = "UPDATE %s SET %s = '%s'" % (table,param,paramValue)
    if selectDict:
      req = "%s %s" % (req,buildCondition(selectDict))
    return self._update(req,connection)

  def __getConnection(self,connection):
    if connection:
      return connection
    res = self._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn("Failed to get MySQL connection",res['Message'])
    return connection

  def __getConnectionTransID(self,connection,transName):
    connection = self.__getConnection(connection)
    res  = self.__getTransformationID(transName,connection)
    if not res['OK']:
      gLogger.error("Failed to get ID for transformation",res['Message'])
      return res
    transID = res['Value']
    resDict = {'Connection':connection,'TransformationID':transID}
    return S_OK(resDict)

####################################################################################
#
#  This part should correspond to the DIRAC Standard File Catalog interface
#
####################################################################################

  def exists(self,lfns,connection=False):
    """ Check the presence of the lfn in the TransformationDB DataFiles table
    """
    gLogger.info("TransformationDB.exists: Attempting to determine existence of %s files." % len(lfns))
    fileIDs = self.__getFileIDsForLfns(lfns,connection)
    failed = {}
    successful = {}
    for lfn in lfns:
      if not lfn in fileIDs.values():
        successful[lfn] = False
      else:
        successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def addReplica(self,replicaTuples,force=False):
    """ Add new replica to the TransformationDB for an existing lfn.
    """
    gLogger.info("TransformationDB.addReplica: Attempting to add %s replicas." % len(replicaTuples))
    fileTuples = []
    for lfn,pfn,se,master in replicaTuples:
      fileTuples.append((lfn,pfn,0,se,'IGNORED-GUID','IGNORED-CHECKSUM'))
    return self.addFile(fileTuples,force)

  def addFile(self,fileTuples,force=False,connection=False):
    """  Add a new file to the TransformationDB together with its first replica.
    """
    gLogger.info("TransformationDB.addFile: Attempting to add %s files." % len(fileTuples))
    successful = {}
    failed = {}
    # Determine which files pass the filters and are to be added to transformations 
    transFiles = {}
    filesToAdd = []
    for lfn,pfn,size,se,guid,checksum in fileTuples:
      fileTrans = self.__filterFile(lfn)
      if not (fileTrans or force):
        successful[lfn] = True
      else:
        filesToAdd.append((lfn,pfn,se))
        for trans in fileTrans:
          if not transFiles.has_key(trans):
            transFiles[trans] = []
          transFiles[trans].append(lfn)
    # Add the files to the DataFiles and Replicas tables
    if filesToAdd:
      connection = self.__getConnection(connection)
      res = self.__addFileTuples(filesToAdd,connection=connection)
      if not res['OK']:
        return res
      lfnFileIDs = res['Value']
      for lfn,pfn,se in filesToAdd:
        if lfnFileIDs.has_key(lfn):
          successful[lfn] = True
        else:
          failed[lfn] = True
      # Add the files to the transformations
      for transID,lfns in transFiles.items():
        fileIDs = []
        for lfn in lfns:
          if lfn.has_key(lfn):
            fileIDs.append(lfnFileIDs[lfn])  
        if fileIDs:
          res = self.__addFilesToTransformation(transID,fileIDs,connection=connection)
          if not res['OK']:
            gLogger.error("Failed to add files to transformation","%s %s" % (transID,res['Message']))
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def getReplicas(self,lfns,allStatus=False,connection=False):
    """ Get replicas for the files specified by the lfn list """
    gLogger.info("TransformationDB.getReplicas: Attempting to get replicas for %s files." % len(lfns))
    connection = self.__getConnection(connection)
    fileIDs = self.__getFileIDsForLfns(lfns,connection=connection)
    failed = {}
    successful = {}
    for lfn in lfns:
      if not lfn in fileIDs.values():
        failed[lfn] = 'File does not exist'
    if fileIDs:
      res = self.__getFileReplicas(fileIDs.keys(),allStatus=allStatus,connection=connection)
      if not res['OK']:
        return res
      for fileID in fileIDs.keys():
        # To catch the case where a file has no replicas
        replicas = {}
        if fileID in res['Value'].keys():
          replicas = res['Value'][fileID]
        successful[fileIDs[fileID]] = replicas
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeReplica(self,replicaTuples):
    """ Remove replica pfn of lfn. """
    gLogger.info("TransformationDB.removeReplica: Attempting to remove %s replicas." % len(replicaTuples))
    successful = {}
    failed = {}
    lfns = []
    for lfn,pfn,se in replicaTuples:
      lfns.append(lfn)
    connection = self.__getConnection(connection)
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res
    fileIDs,lfnFilesIDs = res['Value']
    for lfn in lfns:
      if not lfnFilesIDs.has_key(lfn):
        successful[lfn] = 'File did not exist'
    seFiles = {}
    if fileIDs:
      for lfn,pfn,se in replicaTuples:
        if not seFiles.has_key(se):
          seFiles[se] = []
        seFiles[se].append(lfnFilesIDs[lfn])
    for se,files in seFiles.items():
      res = self.__deleteFileReplicas(files,se=se,connection=connection)
      if not res['OK']:
        for fileID in files:
          failed[fileIDs[fileID]] = res['Message']
      else:
        for fileID in files:
          successful[fileIDs[fileID]] = True
    res = self.__getFileReplicas(fileIDs.keys(),allStatus=True,connection=connection)
    if not res['OK']:
      gLogger.warn("Failed to remove single replica files")
    else:
      noReplicas = []
      fileReplicas = res['Value']
      for fileID in fileIDs.keys():
        if not fileID in fileReplicas.keys():
          noReplicas.append(fileIDs[fileID])
      self.removeFile(noReplicas)
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeFile(self,lfns,connection=False):
    """ Remove file specified by lfn from the ProcessingDB
    """
    gLogger.info("TransformationDB.removeFile: Attempting to remove %s files." % len(lfns))
    failed = {}
    successful = {}
    connection = self.__getConnection(connection)
    res = self.__getFileIDsForLfns(lfns, connection)
    if not res['OK']:
      return res
    fileIDs,lfnFilesIDs = res['Value']
    for lfn in lfns:
      if not lfnFilesIDs.has_key(lfn):
        successful[lfn] = 'File did not exist'
    res = self.__getFileTransformationFiles(fileIDs.keys(),connection=connection)
    if not res['OK']:
      return res
    for transID,fileIDs in res['Value'].items():
      res = self.__setTransformationFileStatus(transID, fileIDs, 'Deleted', connection=connection)
      if not res['OK']:
        for fileID in fileIDs:
          failed[fileIDs[fileID]] = res['Message']
    if fileIDs:
      res = self.__deleteFileReplicas(fileIDs.keys(),connection=connection)
      if not res['OK']:
        return S_ERROR("TransformationDB.removeFile: Failed to remove file replicas.")
      res = self.__setDataFileStatus(fileIDs.keys(),'Deleted',connection=connection)
      if not res['OK']:
        return S_ERROR("TransformationDB.removeFile: Failed to remove files.")
    for lfn in lfnFilesIDs.keys():
      if not failed.has_key(lfn):
        successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)
  
  def setReplicaStatus(self,replicaTuples):
    """Set status for the supplied replica tuples
    """
    gLogger.info("TransformationDB.setReplicaStatus: Attempting to set statuses for %s replicas." % len(replicaTuples))
    successful = {}
    failed = {}
    lfns = []
    for lfn,pfn,se,status in replicaTuples:
      lfns.append(lfn)
    connection = self.__getConnection(connection)
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res
    fileIDs,lfnFilesIDs = res['Value']
    for lfn in lfns:
      if not lfnFilesIDs.has_key(lfn):
        successful[lfn] = True # In the case that the file does not exist then return ok
    seFiles = {}
    if fileIDs:
      for lfn,pfn,se,status in replicaTuples:
        if not seFiles.has_key(se):
          seFiles[se] = {}
        if not seFiles[se].has_key(status):
          seFiles[se][status] = []
        seFiles[se][status].append(lfnFilesIDs[lfn])
    for se,statusDict in seFiles.items():
      for status,files in statusDict.items():
        res = self.__updateReplicaStatus(files,status,se=se,connection=connection)
        if not res['OK']:
          for fileID in files:
            failed[fileIDs[fileID]] = res['Message']
        else:
          for fileID in files:
            successful[fileIDs[fileID]] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def getReplicaStatus(self,replicaTuples):
    """ Get the status for the supplied file replicas """
    gLogger.info("TransformationDB.getReplicaStatus: Attempting to get statuses of file replicas.")
    failed = {}
    successful = {}
    lfns = []
    for lfn,pfn,se in replicaTuples:
      lfns.append(lfn)
    connection = self.__getConnection(connection)
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res
    fileIDs,lfnFilesIDs = res['Value']
    for lfn in lfns:
      if not lfnFilesIDs.has_key(lfn):
        failed[lfn] = 'File did not exist'
    res = self.__getReplicaStatus(fileIDs.keys(), connection=connection)
    if not res['OK']:
      return res
    for fileID,se,status in res['Value']:
      lfn = fileIDs[fileID]
      if not successful.has_key(lfn):
        successful[lfn] = {}
      successful[lfn][se] = status
    for lfn in fileIDs.values():
      if not successful.has_key(lfn):
        failed[lfn] = "TransformationDB.getReplicaStatus: No replicas found."
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def setReplicaHost(self,replicaTuples):
    gLogger.info("TransformationDB.setReplicaHost: Attempting to set SE for %s replicas." % len(replicaTuples))
    successful = {}
    failed = {}
    lfns = []
    for lfn,pfn,oldSE,newSE in replicaTuples:
      lfns.append(lfn)
    connection = self.__getConnection(connection)
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res
    fileIDs,lfnFilesIDs = res['Value']
    for lfn in lfns:
      if not lfnFilesIDs.has_key(lfn):
        successful[lfn] = 'File did not exist'
    if fileIDs:
      for lfn,pfn,oldSE,newSE in replicaTuples:
        if not seFiles.has_key(oldSE):
          seFiles[oldSE] = {}
        if not seFiles[oldSE].has_key(newSE):
          seFiles[oldSE][newSE] = []
        seFiles[oldSE][newSE].append(lfnFilesIDs[lfn])
    for oldSE,seDict in seFiles.items():
      for newSE,files in seDict.items():
        res = self.__updateReplicaSE(files,oldSE,newSE,connection=connection)
        if not res['OK']:
          for fileID in files:
            failed[fileIDs[fileID]] = res['Message']
        else:
          for fileID in files:
            successful[fileIDs[fileID]] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def addDirectory(self,path,force=False):
    """ Adds all the files stored in a given directory in file catalog """
    gLogger.info("TransformationDB.addDirectory: Attempting to populate %s." % path)
    res = self.__getReplicaManager()
    if not res['OK']:
      return res
    rm = res['Value']
    start = time.time()
    res = rm.getCatalogDirectoryReplicas(path,True)
    if not res['OK']:
      gLogger.error("TransformationDB.addDirectory: Failed to get replicas. %s" % res['Message'])
      return res
    gLogger.info("TransformationDB.addDirectory: Obtained %s replicas in %s seconds." % (path,time.time()-start))
    fileTuples = []
    for lfn,replicaDict in res['Value'].items():
      for se,pfn in replicaDict.items():
        fileTuples.append((lfn,pfn,0,se,'IGNORED-GUID','IGNORED-CHECKSUM'))
    if fileTuples:
      res = self.addFile(fileTuples,force=force)
      if not res['OK']:
        return res
      if not res['Value']['Successful']:
        return S_ERROR("Failed to add any files to database")
    return S_OK()

  def __getReplicaManager(self):
    """Gets the RM client instance
    """
    try:
      return S_OK(ReplicaManager())
    except Exception,x:
      errStr = "TransformationDB.__getReplicaManager: Failed to create ReplicaManager"
      gLogger.exception(errStr, lException=x)
      return S_ERROR(errStr)
  
####################################################################################
#
#  This part contains the getFileSummary code
#
#  TODO: Find a way to remove this
#
####################################################################################

  def getFileSummary(self,lfns,transName=''):
    """ Get file status summary in all the transformations
    """
    if transName:
      result = self.getTransformation(transName)
      if not result['OK']:
        return result
      transList = [result['Value']]
    else:
      result = self.getAllTransformations()
      if not result['OK']:
        return S_ERROR('Can not get transformations')
      transList = result['Value']

    resultDict = {}
    fileIDs = self.__getFileIDsForLfns(lfns)
    if not fileIDs:
      return S_ERROR('Files not found in the Transformation Database')

    failedDict = {}
    for lfn in lfns:
      if lfn not in fileIDs.values():
        failedDict[lfn] = True

    fileIDString = ','.join([ str(x) for x in fileIDs.keys() ])

    for transDict in transList:
      transID = transDict['TransID']
      transStatus = transDict['Status']

      req = "SELECT FileID,Status,TargetSE,UsedSE,JobID,ErrorCount,LastUpdate FROM T_%s \
             WHERE FileID in ( %s ) " % (transID,fileIDString)
      result = self._query(req)
      if not result['OK']:
        continue
      if not result['Value']:
        continue

      fileJobIDs = []

      for fileID,status,se,usedSE,jobID,errorCount,lastUpdate in result['Value']:
        lfn = fileIDs[fileID]
        if not resultDict.has_key(fileIDs[fileID]):
          resultDict[lfn] = {}
        if not resultDict[lfn].has_key(transID):
          resultDict[lfn][transID] = {}
        resultDict[lfn][transID]['FileStatus'] = status
        resultDict[lfn][transID]['TargetSE'] = se
        resultDict[lfn][transID]['UsedSE'] = usedSE
        resultDict[lfn][transID]['TransformationStatus'] = transStatus
        if jobID:
          resultDict[lfn][transID]['JobID'] = jobID
          fileJobIDs.append(jobID)
        else:
          resultDict[lfn][transID]['JobID'] = 'No JobID assigned'
        resultDict[lfn][transID]['JobStatus'] = 'Unknown'
        resultDict[lfn][transID]['FileID'] = fileID
        resultDict[lfn][transID]['ErrorCount'] = errorCount
        resultDict[lfn][transID]['LastUpdate'] = str(lastUpdate)

      if fileJobIDs:
        fileJobIDString = ','.join([ str(x) for x in fileJobIDs ])
        req = "SELECT T.FileID,J.WmsStatus from Jobs_%s as J, T_%s as T WHERE J.JobID in ( %s ) AND J.JobID=T.JobID" % (transID,transID,fileJobIDString)
        result = self._query(req)
        if not result['OK']:
          continue
        if not result['Value']:
          continue
        for fileID,jobStatus in result['Value']:
          # If the file was not requested then just ignore it
          if fileID in fileIDs.keys():
            lfn = fileIDs[fileID]
            resultDict[lfn][transID]['JobStatus'] = jobStatus
    return S_OK({'Successful':resultDict,'Failed':failedDict})
