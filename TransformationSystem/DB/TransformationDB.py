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
from DIRAC.DataManagementSystem.Client.ReplicaManager                  import CatalogDirectory
from DIRAC.Core.DISET.RPCClient                                        import RPCClient
from DIRAC.Core.Security.Misc                                          import getProxyInfo
from DIRAC.Core.Utilities.List                                         import stringListToString, intListToString
from DIRAC.Core.Utilities.SiteSEMapping                                import getSEsForSite, getSitesForSE
from DIRAC.Core.Utilities.Shifter                                      import setupShifterProxyInEnv
from DIRAC.Core.Utilities.Subprocess                                   import pythonCall

from types import *
import re,time,string,threading,copy

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
                          'LastUpdate',
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

    self.TRANSFILEPARAMS = ['TransformationID',
                            'FileID',
                            'Status',
                            'JobID',
                            'TargetSE',
                            'UsedSE',
                            'ErrorCount',
                            'LastUpdate',
                            'InsertedTime']

    self.TASKSPARAMS = [  'JobID',
                          'TransformationID',
                          'WmsStatus',
                          'JobWmsID',
                          'TargetSE',
                          'CreationTime',
                          'LastUpdateTime']

  def getName(self):
    """  Get the database name
    """
    return self.dbname

  ###########################################################################
  #
  # These methods manipulate the Transformations table
  #

  def addTransformation(self, transName, description, longDescription,authorDN, authorGroup, transType, plugin,agentType,fileMask,
                        transformationGroup = 'General',
                        groupSize           = 1,
                        inheritedFrom       = 0,
                        body                = '', 
                        maxJobs             = 0,
                        eventsPerJob        = 0,
                        addFiles            = True,
                        connection          = False):
    """ Add new transformation definition including its input streams
    """
    connection = self.__getConnection(connection)
    res  = self._getTransformationID(transName,connection=connection)
    if res['OK']:
      return S_ERROR("Transformation with name %s already exists with TransformationID = %d" % (transName,res['Value']))
    elif res['Message'] != "Transformation does not exist":
      return res
    self.lock.acquire()
    res = self._escapeString(body)
    if not res['OK']:
      return S_ERROR("Failed to parse the transformation body")
    body = res['Value']
    req = "INSERT INTO Transformations (TransformationName,Description,LongDescription, \
                                        CreationDate,LastUpdate,AuthorDN,AuthorGroup,Type,Plugin,AgentType,\
                                        FileMask,Status,TransformationGroup,GroupSize,\
                                        InheritedFrom,Body,MaxNumberOfJobs,EventsPerJob)\
                                VALUES ('%s','%s','%s',\
                                        UTC_TIMESTAMP(),UTC_TIMESTAMP(),'%s','%s','%s','%s','%s',\
                                        '%s','New','%s',%d,\
                                        %d,%s,%d,%d);" % \
                                      (transName, description, longDescription,
                                       authorDN, authorGroup, transType, plugin, agentType,
                                       fileMask,transformationGroup,groupSize,
                                       inheritedFrom,body,maxJobs,eventsPerJob)
    res = self._update(req,connection)
    if not res['OK']:
      self.lock.release()
      return res
    transID = res['lastRowId']
    self.lock.release()
    # If the transformation has an input data specification
    if fileMask:
      self.filters.append((transID,re.compile(fileMask)))
      
    if inheritedFrom:
      res  = self._getTransformationID(inheritedFrom,connection=connection)
      if not res['OK']:
        gLogger.error("Failed to get ID for parent transformation",res['Message'])
        self.deleteTransformation(transID,connection=connection)
        return res
      originalID = res['Value']
      res = self.setTransformationStatus(originalID,'Stopped',author=authorDN,connection=connection)
      if not res['OK']:
        gLogger.error("Failed to update parent transformation status",res['Message'])
        self.deleteTransformation(transID,connection=connection)
        return res
      message = 'Status changed to "Stopped" due to creation of the derived transformation (%d)' % transID
      self.__updateTransformationLogging(originalID,message,authorDN,connection=connection)
      res = self.__getTransformationFiles(originalID,connection=connection)
      if not res['OK']:
        self.deleteTransformation(transID,connection=connection)
        return res
      if res['Value']:
        res = self.__insertExistingTransformationFiles(transID, res['Value'])
        if not res['OK']:
          self.deleteTransformation(transID,connection=connection)
          return res
    if addFiles and fileMask:
      self.__addExistingFiles(transID,connection=connection)
    message = "Created transformation %d" % transID
    self.__updateTransformationLogging(transID,message,authorDN,connection=connection)
    return S_OK(transID)

  def getTransformations(self,condDict={},older=None, newer=None, timeStamp='CreationDate', orderAttribute=None, limit=None, extraParams=False,connection=False):
    """ Get parameters of all the Transformations with support for the web standard structure """
    connection = self.__getConnection(connection)
    req = "SELECT %s FROM Transformations %s" % (intListToString(self.TRANSPARAMS),self.buildCondition(condDict, older, newer, timeStamp,orderAttribute,limit))
    res = self._query(req,connection)
    if not res['OK']:
      return res
    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = []
      transDict = {}
      count = 0
      for item in row:
        transDict[self.TRANSPARAMS[count]] = item
        count += 1
        if type(item) not in [IntType,LongType]:
          rList.append(str(item))
        else:
          rList.append(item)
      webList.append(rList)
      if extraParams:
        res = self.__getAdditionalParameters(transDict['TransformationID'],connection=connection)
        if not res['OK']:
          return res
        transDict.update(res['Value'])
      resultList.append(transDict) 
    result = S_OK(resultList)
    result['Records'] = webList
    result['ParameterNames'] = copy.copy(self.TRANSPARAMS)
    return result

  def getTransformation(self,transName,extraParams=False,connection=False):
    """Get Transformation definition and parameters of Transformation identified by TransformationID
    """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.getTransformations(condDict = {'TransformationID':transID},extraParams=extraParams,connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Transformation %s did not exist" % transName)
    return S_OK(res['Value'][0])

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

  def getDistinctAttributeValue(self, attribute, selectDict, connection=False):
    """ Get distinct values of the given transformation attribute """
    if not attribute in self.TRANSPARAMS:
      return S_ERROR('Can not serve values for attribute %s' % attribute) 
    return self.getDistinctAttributeValues('Transformations',attribute,condDict=selectDict,connection=connection)

  def __updateTransformationParameter(self,transID,paramName,paramValue,connection=False):
    if not (paramName in self.mutable):
      return S_ERROR("Can not update the '%s' transformation parameter" % paramName)
    req = "UPDATE Transformations SET %s='%s', LastUpdate=UTC_TIMESTAMP() WHERE TransformationID=%d" % (paramName,paramValue,transID)
    return self._update(req,connection)

  def _getTransformationID(self,transName,connection=False):
    """ Method returns ID of transformation with the name=<name> """
    try:
      transName = long(transName)
      cmd = "SELECT TransformationID from Transformations WHERE TransformationID=%d;" % transName
    except:
      if type(transName) not in StringTypes:
        return S_ERROR("Transformation should ID or name")
      cmd = "SELECT TransformationID from Transformations WHERE TransformationName='%s';" % transName
    res = self._query(cmd,connection)
    if not res['OK']:
      gLogger.error("Failed to obtain transformation ID for transformation","%s:%s" % (transName,res['Message']))
      return res
    elif not res['Value']:
      gLogger.verbose("Transformation %s does not exist" % (transName))
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
  
  def setTransformationStatus(self,transName,status,author='',connection=False):
    #TODO: Update where used
    return self.setTransformationParameter(transName,'Status',status,author=author,connection=connection)

  ###########################################################################
  #
  # These methods manipulate the AdditionalParameters tables
  #
  def setTransformationParameter(self,transName,paramName,paramValue,author='',connection=False):
    """ Add a parameter for the supplied transformations """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    message = ''
    if paramName in self.TRANSPARAMS:
      res = self.__updateTransformationParameter(transID,paramName,paramValue,connection=connection)
      if res['OK'] and (paramName != 'Body'):
        message = '%s updated to %s' % (paramName,paramValue)
    else:
      res = self.__addAdditionalTransformationParameter(transID, paramName, paramValue, connection=connection)
      if res['OK']:
        message = 'Added additional parameter %s' % paramName
    if message:
      self.__updateTransformationLogging(transID,message,author,connection=connection)      
    return res      

  def deleteTransformationParameter(self,transName,paramName,author='',connection=False):
    """ Delete a parameter from the additional parameters table """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    message = ''
    if paramName in self.TRANSPARAMS: 
      return S_ERROR("Can not delete core transformation parameter")
    res = self.__deleteTransformationParameters(transID,parameters=[paramName],connection=connection)
    if not res['OK']:
      return res
    self.__updateTransformationLogging(transID,'Removed additional parameter %s' % paramName,author,connection=connection)
    return res

  def __addAdditionalTransformationParameter(self,transID,paramName,paramValue,connection=False):
    req = "DELETE FROM AdditionalParameters WHERE TransformationID=%d AND ParameterName='%s'" % (transID,paramName)
    res = self._update(req,connection)
    if not res['OK']:
      return res
    res = self._escapeString(paramValue)
    if not res['OK']:
      return S_ERROR("Failed to parse parameter value")
    paramValue = res['Value']
    paramType = 'StringType'
    if type(paramValue) in [IntType,LongType]:
      paramType = 'IntType'
    req = "INSERT INTO AdditionalParameters (TransformationID,ParameterName,ParameterValue,ParameterType) VALUES (%s,'%s',%s,'%s');" % (transID,paramName,paramValue,paramType)
    return self._update(req,connection)

  def __getAdditionalParameters(self,transID,connection=False):
     req = "SELECT ParameterName,ParameterValue,ParameterType FROM AdditionalParameters WHERE TransformationID = %d" % transID
     res = self._query(req,connection)
     if not res['OK']:
       return res
     paramDict = {}
     for parameterName,parameterValue,parameterType in res['Value']:
       parameterType = eval(parameterType)
       if parameterType in [IntType,LongType]:
         parameterValue = int(parameterValue)
       paramDict[parameterName] = parameterValue
     return S_OK(paramDict)    

  def __deleteTransformationParameters(self,transID,parameters=[],connection=False):
    """ Remove the parameters associated to a transformation """
    req = "DELETE FROM AdditionalParameters WHERE TransformationID=%d" % transID
    if parameters:
      req = "%s AND ParameterName IN (%s);" % (req,stringListToString(parameters))
    return self._update(req,connection)

  ###########################################################################
  #
  # These methods manipulate the TransformationFiles table
  #

  def addFilesToTransformation(self,transName,lfns,connection=False):
    """ Add a list of LFNs to the transformation directly """
    if not lfns:
      return S_ERROR('Zero length LFN list')
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res
    fileIDs,lfnFilesIDs = res['Value']
    failed = {}
    successful = {}
    missing = []
    for lfn in lfns:
      if lfn not in fileIDs.values():
        missing.append((lfn,'Unknown','Unknown'))
    if missing:
      res = self.__addFileTuples(missing,connection=connection)
      if not res['OK']:
        return res
      for lfn,fileID in res['Value'].items():
        fileIDs[fileID] = lfn 
    # must update the fileIDs
    if fileIDs:
      res = self.__addFilesToTransformation(transID, fileIDs.keys(),connection=connection)  
      if not res['OK']:
        return res
      for fileID in fileIDs.keys():
        lfn = fileIDs[fileID]
        successful[lfn] = "Present"
        if fileID in res['Value']:
          successful[lfn] = "Added"
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)
  
  def getTransformationFiles(self,condDict={},older=None, newer=None, timeStamp='LastUpdate', orderAttribute=None, limit=None,connection=False):
    """ Get files for the supplied transformations with support for the web standard structure """
    connection = self.__getConnection(connection)
    req = "SELECT %s FROM TransformationFiles" % (intListToString(self.TRANSFILEPARAMS))
    if condDict or older or newer:
      req = "%s %s" % (req,self.buildCondition(condDict, older, newer, timeStamp,orderAttribute,limit))
    res = self._query(req,connection)
    if not res['OK']:
      return res
    transFiles = res['Value']
    fileIDs = [int(row[1]) for row in transFiles]
    webList = []
    resultList = []
    fileIDLfns = {}
    if fileIDs:
      res = self.__getLfnsForFileIDs(fileIDs,connection=connection)
      if not res['OK']:
        return res
      fileIDLfns = res['Value'][1]
      for row in transFiles:
        lfn = fileIDLfns[row[1]]
        # Prepare the structure for the web
        rList = [lfn]
        fDict = {}
        fDict['LFN'] = lfn
        count = 0
        for item in row:
          fDict[self.TRANSFILEPARAMS[count]] = item
          count += 1
          if type(item) not in [IntType,LongType]:
            rList.append(str(item))
          else:
            rList.append(item)
        webList.append(rList)
        resultList.append(fDict)
    result = S_OK(resultList)
    result['LFNs'] = fileIDLfns.values()
    result['Records'] = webList
    result['ParameterNames'] = ['LFN'] + self.TRANSFILEPARAMS
    return result

  def getTransformationFileInfo(self,transID,lfns,connection=False):
    """ Get the file status for given transformation files """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res
    fileIDs,lfnFilesIDs = res['Value']
    return self.getTransformationFiles(condDict={'TransformationID':transID,'FileID':fileIDs.keys()},connection=connection)

  def getFileSummary(self,lfns,connection=False):
    """ Get file status summary in all the transformations """
    connection = self.__getConnection(connection)
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res   
    fileIDs,lfnFilesIDs = res['Value']
    failedDict = {}
    for lfn in lfns:
      if lfn not in fileIDs.values():
        failedDict[lfn] = 'Did not exist in the Transformation database'
    condDict = {'FileID':fileIDs.keys()}
    res = self.getTransformationFiles(self,condDict=condDict,connection=connection)
    if not res['OK']:
      return res
    resDict = {}
    for fileDict in res['Value']:
      lfn = fileDict['LFN']
      transID = fileDict['TransformationID']
      if not resDict.has_key(lfn):
        resDict[lfn] = {}
      if not resDict[lfn].has_key(transID):
        resDict[lfn][transID] = {}
      resDict[lfn][transID] = fileDict
    return S_OK({'Successful':resDict,'Failed':failedDict})
  
  #TODO use the getTransformationFileInfo method 
  def setFileStatusForTransformation(self,transName,status,lfns,force=False,connection=False):
    """ Set file status for the given transformation """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res   
    fileIDs,lfnFilesIDs = res['Value']
    successful = {}
    failed = {}
    for lfn in lfns:
      if lfn not in fileIDs.values():
        failed[lfn] = 'File not found in the Transformation Database'
    res = self.getTransformationFiles(condDict={'TransformationID':transID,'FileID':fileIDs.keys()},connection=connection)
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
        req = ''
      elif (currentStatus == status):
        successful[lfn] = 'Status not changed'
        req = ''
      elif (status.lower() == 'unused') and (errorCount >= MAX_ERROR_COUNT) and (not force):
        failed[lfn] = 'Max number of resets reached'
        req = "UPDATE TransformationFiles SET Status='MaxReset', LastUpdate=UTC_TIMESTAMP() WHERE TransformationID=%d AND FileID=%d;" % (fileID,transID)
      elif (status.lower() == 'unused'):
        req = "UPDATE TransformationFiles SET Status='%s', LastUpdate=UTC_TIMESTAMP(),ErrorCount=ErrorCount+1 WHERE TransformationID=%d AND FileID=%d;" % (status,transID,fileID)
      else:
        req = "UPDATE TransformationFiles SET Status='%s', LastUpdate=UTC_TIMESTAMP() WHERE TransformationID=%d AND FileID=%d;" % (status,transID,fileID)
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
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.getCounters('TransformationFiles',['TransformationID','Status'],{'TransformationID':transID})
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
  
  def __addFilesToTransformation(self,transID,fileIDs,connection=False):
    req = "SELECT FileID from TransformationFiles WHERE TransformationID = %d AND FileID IN (%s);" % (transID,intListToString(fileIDs))
    res = self._query(req,connection)
    if not res['OK']:
      return res
    for tuple in res['Value']:
      fileIDs.remove(tuple[0])
    if not fileIDs:
      return S_OK([])
    req = "INSERT INTO TransformationFiles (TransformationID,FileID,LastUpdate,InsertedTime) VALUES"
    for fileID in fileIDs:
      req = "%s (%d,%d,UTC_TIMESTAMP(),UTC_TIMESTAMP())," % (req,transID,fileID)
    req = req.rstrip(',')
    res = self._update(req,connection)
    if not res['OK']:
      return res
    return S_OK(fileIDs)

  def __addExistingFiles(self,transID,connection=False):
    """ Add files that already exist in the DataFiles table to the transformation specified by the transID """
    for tID,filter in self.filters:
      if tID == transID:
        filters = [(tID,filter)]
        break
    if not filters:
      return S_ERROR('No filters defined for transformation %d' % transID)
    res = self.__getFileIDsForLfns(lfns=[],connection=connection)
    if not res['OK']:
      return res   
    fileIDs,lfnFilesIDs = res['Value']
    passFilter = []
    for fileID,lfn in fileIDs.items():
      if self.__filterFile(lfn,filters):
        passFilter.append(fileID)
    return self.__addFilesToTransformation(transID, passFilter,connection=connection)
  
  def __insertExistingTransformationFiles(self,transID,fileTuples):
    req = "INSERT INTO TransformationFiles (TransformationID,Status,JobID,FileID,TargetSE,UsedSE,LastUpdate) VALUES"
    for fileID,status,taskID,targetSE,usedSE in res['Value']:
      if taskID:
        taskID = str(int(originalID)).zfill(8)+'_'+str(int(taskID)).zfill(8)
      req = "%s (%d,'%s','%s',%d,'%s','%s',UTC_TIMESTAMP())," % (req,transID,status,taskID,fileID,targetSE,usedSE)
    req = req.rstrip(",")
    return self._update(req,connection)

  def __assignTransformationFile(self,transID,taskID,se,fileIDs,connection=False):
    """ Make necessary updates to the TransformationFiles table for the newly created task """
    req = "UPDATE TransformationFiles SET JobID='%d',UsedSE='%s',Status='Assigned',LastUpdate=UTC_TIMESTAMP() WHERE TransformationID = %d AND FileID IN (%s);" % (taskID,se,transID,intListToString(fileIDs.keys()))
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to assign file to task",res['Message'])
    return res
  
  def __setTransformationFileStatus(self,fileIDs,status,connection=False):
    req = "UPDATE TransformationFiles SET Status = '%s' WHERE FileID IN (%s);" % (status,intListToString(fileIDs))
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to update file status",res['Message'])                                                                
    return res

  def __resetTransformationFile(self,transID,taskID,connection=False):	
    req = "UPDATE TransformationFiles SET JobID=NULL, UsedSE='Unknown', Status='Unused' WHERE TransformationID = %d AND JobID=%d;" % (transID,taskID)
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to reset transformation file",res['Message'])
    return res

  #TODO USE THE GENERAL METHOD
  def __getTransformationFiles(self,transID,connection=False):
    req = "SELECT FileID,Status,JobID,TargetSE,UsedSE from TransformationFiles WHERE TransformationID = %d AND Status != 'Unused';" % (transID)
    res = self._query(req,connection)
    if not res['OK']:
      gLogger.error("Failed to get transformation files",res['Message'])                                                                
    return res

  def __deleteTransformationFiles(self,transID,connection=False):
    """ Remove the files associated to a transformation """  
    req = "DELETE FROM TransformationFiles WHERE TransformationID = %d;" % transID
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to delete transformation files",res['Message'])                                                                
    return res
  
  ###########################################################################
  #
  # These methods manipulate the Jobs table
  #
  
  def getTransformationTasks(self,condDict={},older=None, newer=None, timeStamp='CreationTime', orderAttribute=None, limit=None, inputVector=False, connection=False):
    connection = self.__getConnection(connection)
    req = "SELECT %s FROM Jobs %s" % (intListToString(self.TASKSPARAMS),self.buildCondition(condDict, older, newer, timeStamp,orderAttribute,limit))
    res = self._query(req,connection)
    if not res['OK']:
      return res
    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = []
      taskDict = {}
      count = 0
      for item in row:
        taskDict[self.TASKSPARAMS[count]] = item
        count += 1
        if type(item) not in [IntType,LongType]:
          rList.append(str(item))
        else:
          rList.append(item)
      webList.append(rList)
      if inputVector:
        taskDict['InputVector'] = ''
        taskID = taskDict['JobID']
        transID = taskDict['TransformationID']
        res = self.getTaskInputVector(transID,taskID)
        if res['OK']:
          if res['Value'].has_key(taskID):
            taskDict['InputVector']=res['Value'][taskID]    
      resultList.append(taskDict) 
    result = S_OK(resultList)
    result['Records'] = webList
    result['ParameterNames'] = self.TASKSPARAMS
    return result

  def getTasksForSubmission(self,transName,numTasks=1,site='',statusList=['Created'],older=None,newer=None,connection=False):
    """ Select tasks with the given status (and site) for submission """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    condDict = {"TransformationID":transID}
    if statusList:
      condDict["WmsStatus"] = statusList
    if site:
      numTasks=0
    res = self.getTransformationTasks(condDict=condDict,older=older, newer=newer, timeStamp='CreationTime', orderAttribute=None, limit=numTasks,inputVector=True,connection=connection)
    if not res['OK']:
      return res
    tasks = res['Value']
    # Prepare Site->SE resolution mapping
    selSEs = []
    if site:
      res = getSEsForSite(site)
      if not res['OK']:
        return res
      selSEs = res['Value']
    # Now prepare the tasks
    resultDict = {}
    for taskDict in tasks:
      if len(resultDict) >= numTasks:
        break
      taskID = taskDict['JobID']
      se = taskDict['TargetSE']
      status = taskDict['WmsStatus']
      inputVector = taskDict['InputVector']
      transID = taskDict['TransformationID']
      if not site:
        if inputVector:
          res = getSitesForSE(se,'LCG')
          if not res['OK']:
            continue
          usedSite = res['Value']
          if len(usedSite) == 1:
            usedSite = usedSite[0]
        else:
          usedSite = 'ANY'      
        resultDict[taskID] = {'InputData':inputVector,'TargetSE':se,'Status':status,'Site':usedSite,'TransformationID':transID}
      elif site and (se in selSEs):
        resultDict[taskID] = {'InputData':inputVector,'TargetSE':se,'Status':status,'Site':site,'TransformationID':transID}   
      else:
        gLogger.warn("Can not find corresponding site for se",se)
    return S_OK(resultDict)

  def deleteTasks(self,transName,taskIDbottom, taskIDtop,author='',connection=False):
    """ Delete tasks with taskID range in transformation """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    for taskID in range(taskIDbottom,taskIDtop+1):
      res = self.__removeTransformationTask(transID, taskID, connection=connection)
      if not res['OK']:
        return res
    message = "Deleted tasks from %d to %d" % (taskIDbottom,taskIDtop)
    self.__updateTransformationLogging(transID,message,author,connection=connection)      
    return res

  def reserveTask(self,transName,taskID,connection=False):
    """ Reserve the taskID from transformation for submission """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']    
    res = self.__checkUpdate("Jobs","WmsStatus","Reserved",{"TransformationID":transID,"JobID":taskID},connection=connection)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR('Failed to set Reserved status for job %d - already Reserved' % int(taskID) )
    # The job is reserved, update the time stamp
    res = self.setTaskStatus(transID,taskID,'Reserved',connection=connection)
    if not res['OK']:
      return S_ERROR('Failed to set Reserved status for job %d - failed to update the time stamp' % int(taskID))
    return S_OK()

  def setTaskStatusAndWmsID(self,transName,taskID,status,taskWmsID,connection=False):
    """ Set status and JobWmsID for job with jobID in production with transformationID
    """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__setTaskParameterValue(transID,taskID,'WmsStatus',status,connection=connection)
    if not res['OK']:
      return res
    return self.__setTaskParameterValue(transID, taskID, 'JobWmsID', taskWmsID, connection=connection)

  def setTaskStatus(self,transName,taskID,status,connection=False):
    """ Set status for job with jobID in production with transformationID """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    if type(taskID) != ListType:
      taskIDList = [taskID]
    else:
      taskIDList = list(taskID)
    for taskID in taskIDList:
      res = self.__setTaskParameterValue(transID, taskID, 'WmsStatus', status, connection=connection)
      if not res['OK']:
        return res
    return S_OK()  

  def getTransformationTaskStats(self,transName='',connection=False):
    """ Returns dictionary with number of jobs per status for the given production.
    """
    connection = self.__getConnection(connection)
    if transName:
      res  = self._getTransformationID(transName,connection=connection)
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

  def __setTaskParameterValue(self,transID,taskID,paramName,paramValue,connection=False):
    req = "UPDATE Jobs SET %s='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE TransformationID=%d AND JobID=%d;" % (paramName,paramValue,transID,taskID)
    return self._update(req,connection)

  def __deleteTransformationTasks(self,transID,connection=False):
    """ Delete all the tasks from the Jobs table for transformation with TransformationID """
    req = "DELETE FROM Jobs WHERE TransformationID=%d" % transID
    return self._update(req,connection)

  def __deleteTransformationTask(self,transID,taskID,connection=False):
    """ Delete the task from the Jobs table for transformation with TransformationID """
    req = "DELETE FROM Jobs WHERE TransformationID=%d AND JobID=%d" % (transID,taskID)
    return self._update(req,connection)
  
  ###########################################################################
  #
  # These methods manipulate the JobInputs table
  #

  def getTaskInputVector(self,transName,taskID,connection=False):
    """ Get input vector for the given task """
    res = self._getConnectionTransID(connection,transName)
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
    values = [transID,taskID,vector]
    res = self._insert('JobInputs',fields,values,connection)
    if not res['OK']:
      gLogger.error("Failed to add input vector to task %d" % taskID)
    return res

  def __deleteTransformationTaskInputs(self,transID,taskID=0,connection=False):
    """ Delete all the tasks inputs from the JobsInputs table for transformation with TransformationID """
    req = "DELETE FROM JobInputs WHERE TransformationID=%d" % transID
    if taskID:
      req = "%s AND JobID=%d" % (req,int(taskID))
    return self._update(req,connection)

  ###########################################################################
  #
  # These methods manipulate the TransformationLog table
  #

  def __updateTransformationLogging(self,transName,message,authorDN,connection=False):
    """ Update the Transformation log table with any modifications
    """
    if not authorDN:
      res = getProxyInfo(False,False)
      if res['OK']:
        authorDN = res['Value']['identity']
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "INSERT INTO TransformationLog (TransformationID,Message,Author,MessageDate) VALUES (%s,'%s','%s',UTC_TIMESTAMP());" % (transID,message,authorDN)
    return self._update(req,connection)
  
  def getTransformationLogging(self,transName,connection=False):
    """ Get logging info from the TransformationLog table """
    res = self._getConnectionTransID(connection,transName)
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
      transDict['TransformationID'] = transID
      transDict['Message'] = message
      transDict['AuthorDN'] = authorDN
      transDict['MessageDate'] = messageDate
      transList.append(transDict)
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
    req = "SELECT LFN,FileID FROM DataFiles WHERE FileID in (%s);" % stringListToString(fileIDs)
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
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res   
    fileIDs,lfnFileIDs = res['Value']
    for lfn in lfns:
      if not lfn in lfnFileIDs.keys():
        req = "INSERT INTO DataFiles (LFN,Status) VALUES ('%s','New');" % lfn
        res = self._update(req,connection)
        if not res['OK']:
          return res
        lfnFileIDs[lfn] = res['lastRowId']
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
    #TODO IF LFNS SUPPLIED ARE NOT UNUSED IT SHOULD NOT ALLOW A TASK TO BE CREATED
    connection = self.__getConnection(connection)
    # Be sure the all the supplied LFNs are known to the databse
    if lfns:
      res = self.__getFileIDsForLfns(lfns,connection=connection)
      if not res['OK']:
        return res   
      fileIDs,lfnFilesIDs = res['Value']
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
    res  = self._getTransformationID(transID,connection=connection)
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
      gLogger.error("Failed to publish task for transformation", res['Message'])
      return res
    res = self._query("SELECT LAST_INSERT_ID();", connection)
    self.lock.release()
    if not res['OK']:
      return res
    taskID = int(res['Value'][0][0])
    gLogger.verbose("Published task %d for transformation %d." % (taskID,transID))
    # If we have input data then update their status, and taskID in the transformation table
    if lfns:
      res = self.__insertTaskInputs(transID,taskID,lfns,connection=connection)
      if not res['OK']:
        self.__removeTransformationTask(transID,taskID,connection=connection)
        return res
      res = self.__assignTransformationFile(transID, taskID, se, fileIDs, connection=connection)
      if not res['OK']:
        self.__removeTransformationTask(transID,taskID,connection=connection)
        return res
    return S_OK(taskID)

  def extendTransformation(self, transName, nTasks, author='', connection=False):
    """ Extend SIMULATION type transformation by nTasks number of tasks
    """
    connection = self.__getConnection(connection)
    res  = self.getTransformation(transName,connection=connection)
    if not res['OK']:
      gLogger.error("Failed to get transformation details",res['Message'])
      return res
    transType = res['Value']['Type']
    transID = res['Value']['TransformationID']
    if transType.lower() not in ['simulation','mcsimulation']:
      return S_ERROR('Can not extend non-SIMULATION type production')
    taskIDs = []
    for task in range(nTasks):
      res = self.addTaskForTransformation(transID,connection=connection)
      if not res['OK']:
        return res
      taskIDs.append(res['Value'])
    # Add information to the transformation logging
    message = 'Transformation extended by %d tasks' % nTasks
    self.__updateTransformationLogging(transName,message,author,connection=connection)  
    return S_OK(taskIDs)
  
  def cleanTransformation(self,transName,author='',connection=False):
    """ Clean the transformation specified by name or id """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__deleteTransformationFiles(transID,connection=connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformationTasks(transID,connection=connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformationTaskInputs(transID,connection=connection)
    if not res['OK']:
      return res
    res = self.setTransformationParameter(transID,'Status','Cleaned',author=author,connection=connection)
    if not res['OK']:
      return res
    message = "Transformation Cleaned"
    self.__updateTransformationLogging(transID,message,author,connection=connection)
    return S_OK(transID)
    
  def deleteTransformation(self,transName,author='',connection=False):
    """ Remove the transformation specified by name or id """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.cleanTransformation(transID,author=author,connection=connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformationLog(transID,connection=connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformationParameters(transID,connection=connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformation(transID,connection=connection)
    if not res['OK']:
      return res
    res = self.__updateFilters()
    if not res['OK']:
      return res
    return S_OK()

  def __removeTransformationTask(self,transID,taskID,connection=False):
    res = self.__deleteTransformationTaskInputs(transID,taskID,connection=connection)
    if not res['OK']:
      return res
    res = self.__deleteTransformationTask(transID,taskID,connection=connection)
    if not res['OK']:
      return res
    return self.__resetTransformationFile(transID,taskID,connection=connection)

  def __checkUpdate(self,table,param,paramValue,selectDict = {},connection=False):
    """ Check whether the update will perform an update """
    req = "UPDATE %s SET %s = '%s'" % (table,param,paramValue)
    if selectDict:
      req = "%s %s" % (req,self.buildCondition(selectDict))
    return self._update(req,connection)

  def __getConnection(self,connection):
    if connection:
      return connection
    res = self._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn("Failed to get MySQL connection",res['Message'])
    return connection

  def _getConnectionTransID(self,connection,transName):
    connection = self.__getConnection(connection)
    res  = self._getTransformationID(transName,connection=connection)
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
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res   
    fileIDs,lfnFilesIDs = res['Value']
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
      #TODO: THIS SHOULD BE TESTED WITH A TRANSFORMATION WITH A FILTER
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
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res   
    fileIDs,lfnFilesIDs = res['Value']
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

  def removeReplica(self,replicaTuples,connection=False):
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
      if noReplicas:
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
    if not lfns:
      return S_ERROR("No LFNs supplied")
    res = self.__getFileIDsForLfns(lfns, connection=connection)
    if not res['OK']:
      return res
    fileIDs,lfnFilesIDs = res['Value']
    for lfn in lfns:
      if not lfnFilesIDs.has_key(lfn):
        successful[lfn] = 'File did not exist'
    if fileIDs:
      res = self.__setTransformationFileStatus(fileIDs.keys(), 'Deleted', connection=connection)
      if not res['OK']:
        return res
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
  
  def setReplicaStatus(self,replicaTuples,connection=False):
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

  def getReplicaStatus(self,replicaTuples,connection=False):
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

  def setReplicaHost(self,replicaTuples,connection=False):
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
    seFiles = {}
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
    res = pythonCall(0,self.__addDirectory,path,force)
    if not res['OK']:
      gLogger.error("Failed to invoke addDirectory with shifter proxy")
      return res
    return res['Value']

  def __addDirectory(self,path,force):
    res = setupShifterProxyInEnv("ProductionManager")    
    if not res['OK']:
      return S_OK("Failed to setup shifter proxy")
    catalog = CatalogDirectory()
    start = time.time()
    res = catalog.getCatalogDirectoryReplicas(path,singleFile=True)
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
    return S_OK(len(res['Value']['Successful']))
