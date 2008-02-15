########################################################################
# $Id: TransformationDB.py,v 1.34 2008/02/15 22:49:21 gkuznets Exp $
########################################################################
""" DIRAC Transformation DB

    Transformation database is used to collect and serve the necessary information
    in order to automate the task of job preparation for high level transformations.
    This class is typically used as a base class for more specific data processing
    databases
"""

__RCSID__ = "$Id: "

import re,time,types

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogClient import LcgFileCatalogClient
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase import FileCatalogueBase
from DIRAC.Core.Utilities.List import stringListToString, intListToString
import threading

#############################################################################

class TransformationDB(DB):

  def __init__(self, dbname, dbconfig, maxQueueSize=10 ):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """

    DB.__init__(self,dbname, dbconfig, maxQueueSize)
    self.lock = threading.Lock()
    self.dbname = dbname
    self.filters = self.__getFilters()
    self.catalog = None


  def getTransformationID(self, name):
    """ Method returns ID of transformation with the name=<name>
        it checks type of the argument, and if it is string returns transformationID
        if not we assume that prodName is actually prodID

        Returns transformation ID if exists otherwise 0
        WARNING!! returned value is long !!
    """
    if isinstance(name, str):
      cmd = "SELECT TransformationID from Transformations WHERE TransformationName='%s';" % name
      result = self._query(cmd)
      if not result['OK']:
        gLogger.error("Failed to check if Transformation with name %s exists %s" % (name, result['Message']))
        return 0L # we do not terminate execution here but log error
      elif result['Value'] == ():
        gLogger.verbose("Transformation with name %s do not exists" % (name))
        return 0L # we do not terminate execution here
      return result['Value'][0][0]
    return name # it is actually number

  def transformationExists(self, transID):
    """ Method returns TRUE if transformation with the ID=<id> exists
    """
    cmd = "SELECT COUNT(*) from Transformations WHERE TransformationID='%s';" % transID
    result = self._query(cmd)
    if not result['OK']:
      gLogger.error("Failed to check if Transformation with ID %d exists %s" % (transID, result['Message']))
      return False
    elif result['Value'][0][0] > 0:
        return True
    return False

####################################################################################
#
#  This part contains the transformation manipulation methods
#
####################################################################################

  def getName(self):
    """  Get the database name
    """
    return self.dbname

  def addTransformation(self, name, description, longDescription, authorDN, authorGroup, type, plugin, agentType,fileMask):
    """ Add new transformation definition including its input streams
    """
    self.lock.acquire()
    req = "INSERT INTO Transformations (TransformationName,Description,LongDescription,\
    CreationDate,AuthorDN,AuthorGroup,Type,Plugin,AgentType,FileMask,Status) VALUES\
    ('%s','%s','%s',NOW(),'%s','%s','%s','%s','%s','%s','New');" % (name, description, longDescription,authorDN, authorGroup, type, plugin, agentType,fileMask)
    res = self._update(req)
    if not res['OK']:
      self.lock.release()
      return res
    req = "SELECT LAST_INSERT_ID();"
    res = self._query(req)
    self.lock.release()
    if not res['OK']:
      return res
    transID = int(res['Value'][0][0])
    self.filters.append((transID,re.compile(fileMask)))
    result = self.__addTransformationTable(transID)
    # Add already existing files to this transformation if any
    result = self.__addExistingFiles(transID)
    return S_OK(transID)

  def updateTransformationLogging(self,transName,message,authorDN):
    """ Update the Transformation log table with any modifications (we know who you are!!)
    """
    transID = self.getTransformationID(transName)
    req = "INSERT INTO TransformationLog (TransformationID,Message,Author,MessageDate) \
    VALUES (%s,'%s','%s',NOW());" % (transID,message,authorDN)
    res = self._update(req)
    return res

  def setTransformationStatus(self,transName,status):
    """ Set the status of the transformation specified by transID
    """
    transID = self.getTransformationID(transName)
    req = "UPDATE Transformations SET Status='%s' WHERE TransformationID=%s;" % (status,transID)
    res = self._update(req)
    return res

  def getTransformationStats(self,transName):
    """ Get the statistics of Transformation by supplied transformation name.
    """
    transID = self.getTransformationID(transName)
    req = "SELECT FileID,Status from T_%s;" % transID
    res = self._query(req)
    if not res['OK']:
      return res
    total = 0
    resDict = {}
    for fileID,status in res['Value']:
      total += 1
      if not resDict.has_key(status):
        resDict[status] = 0
      resDict[status] += 1
    resDict['total'] = total
    return S_OK(resDict)

  def getTransformation(self,transName):
    """Get Transformation definition
       Get the parameters of Transformation idendified by production ID
       KGG This code must be corrected - better version is commented
    """
#    transID = self.getTransformationID(transName)
#    if transID > 0:
#      req = "SELECT TransformationID,TransformationName,Description,LongDescription,CreationDate,\
#             AuthorDN,AuthorGroup,Type,Plugin,AgentType,Status,FileMask FROM Transformations WHERE TransformationID=%d;"%transID
#      res = self._query(req)
#      if not res['OK']:
#        return res
#      tr=result['Value']
#      transdict = {}
#      transdict['TransID'] = tr[0]
#      transdict['Name'] = tr[1]
#      transdict['Description'] = tr[2]
#      transdict['LongDescription'] = tr[3]
#      transdict['CreationDate'] = tr[4]
#      transdict['AuthorDN'] = tr[5]
#      transdict['AuthorGroup'] = tr[6]
#      transdict['Type'] = tr[7]
#      transdict['Plugin'] = tr[8]
#      transdict['AgentType'] = tr[9]
#      transdict['Status'] = tr[10]
#      transdict['FileMask'] = tr[11]
#      req = "SELECT ParameterName,ParameterValue FROM TransformationParameters WHERE TransformationID = %s;" % transID
#      res = self._query(req)
#      if res['OK']:
#        if res['Value']:
#          transdict['Additional'] = {}
#          for parameterName,parameterValue in res['Value']:
#            transdict['Additional'][parameterName] = parameterValue
#      return S_OK(transDict)
#    return S_ERROR('Transformation with id =%d not found'%transID)

    res = self.getAllTransformations()
    if not res['OK']:
      return res
    for transDict in res['Value']:
      if transDict['Name'] == transName:
        return S_OK(transDict)
    return S_ERROR('Transformation not found')

  def getAllTransformations(self):
    """ Get parameters of all the Transformations
    """
    translist = []
    req = "SELECT TransformationID,TransformationName,Description,LongDescription,CreationDate,\
    AuthorDN,AuthorGroup,Type,Plugin,AgentType,Status,FileMask FROM Transformations;"
    res = self._query(req)
    if not res['OK']:
      return res
    for transID,transName,description,longDesc,createDate,authorDN,authorGroup,type,plugin,agentType,status,mask in res['Value']:
      transdict = {}
      transdict['TransID'] = transID
      transdict['Name'] = transName
      transdict['Description'] = description
      transdict['LongDescription'] = longDesc
      transdict['CreationDate'] = createDate
      transdict['AuthorDN'] = authorDN
      transdict['AuthorGroup'] = authorGroup
      transdict['Type'] = type
      transdict['Plugin'] = plugin
      transdict['AgentType'] = agentType
      transdict['Status'] = status
      transdict['FileMask'] = mask
      req = "SELECT ParameterName,ParameterValue FROM TransformationParameters WHERE TransformationID = %s;" % transID
      res = self._query(req)
      if res['OK']:
        if res['Value']:
          transdict['Additional'] = {}
          for parameterName,parameterValue in res['Value']:
            transdict['Additional'][parameterName] = parameterValue
      translist.append(transdict)
    return S_OK(translist)

  def setTransformationMask(self,transName,fileMask):
    """ Modify the input stream definition for the given transformation
        identified by production
    """
    transID = self.getTransformationID(transName)
    req = "UPDATE Transformations SET FileMask='%s' WHERE TransformationID=%s" % (fileMask,transID)
    res = self._update(req)
    return res

  def changeTransformationName(self,transName,newName):
    """ Change the transformation name
    """
    transID = self.getTransformationID(transName)
    req = "UPDATE Transformations SET TransformationName='%s' WHERE TransformationID=%s;" % (newName,transID)
    res = self._update(req)
    return res

  def getInputData(self,transName,status):
    """ Get input data for the given transformation, only files
        with a given status which is defined for the file replicas.
    """
    
    print 'TransformationDB 1:',transName,status
    
    transID = self.getTransformationID(transName)
    req = "SELECT FileID from T_%s WHERE Status='Unused';" % (transID)
    res = self._query(req)
    if not res['OK']:
      return res
    if not res['Value']:
      return res
    ids = [ str(x[0]) for x in res['Value'] ]
    if not ids:
      return S_OK([])

    if status:
      req = "SELECT LFN,SE FROM Replicas,DataFiles WHERE Replicas.Status = '%s' AND \
      Replicas.FileID=DataFiles.FileID AND Replicas.FileID in (%s);" % (status,intListToString(ids))
    else:
      req = "SELECT LFN,SE FROM Replicas,DataFiles WHERE Replicas.Status = 'AprioriGood' AND \
      Replicas.FileID=DataFiles.FileID AND Replicas.FileID in (%s);" % intListToString(ids)
    res = self._query(req)
    if not res['OK']:
      return res
    replicaList = []
    for lfn,se in res['Value']:
      replicaList.append((lfn,se))
    return S_OK(replicaList)

  def getFilesForTransformation(self,transName,jobOrdered=False):
    """ Get files and their status for the given transformation
    """
    transID = self.getTransformationID(transName)
    req = "SELECT d.LFN,t.Status,t.JobID,t.TargetSE FROM DataFiles AS d,T_%s AS t WHERE t.FileID=d.FileID" % transID
    if jobOrdered:
      req = "%s ORDER by t.JobID;" % req
    else:
      req = "%s ORDER by LFN;" % req
    res = self._query(req)
    if not res['OK']:
      return res
    flist = []
    for lfn,status,jobid,usedse in res['Value']:
      print lfn,status,jobid,usedse
      fdict = {}
      fdict['LFN'] = lfn
      fdict['Status'] = status
      if jobid is None: jobid = 'No JobID assigned'
      fdict['JobID'] = jobid
      fdict['TargetSE'] = usedse
      flist.append(fdict)
    return S_OK(flist)

  def setFileSEForTransformation(self,transName,se,lfns):
    """ Set file SE for the given transformation identified by transID
        for files in the list of lfns
    """
    transID = self.getTransformationID(transName)
    fileIDs = self.__getFileIDsForLfns(lfns).keys()
    if not fileIDs:
      return S_ERROR('TransformationDB.setFileSEForTransformation: No files found.')
    else:
      req = "UPDATE T_%s SET UsedSE='%s' WHERE FileID IN (%s);" % (transID,se,intListToString(fileIDs))
      return self._update(req)

  def setFileTargetSEForTransformation(self,transName,se,lfns):
    """ Set file Target SE for the given transformation identified by transID
        for files in the list of lfns
    """
    transID = self.getTransformationID(transName)
    fileIDs = self.__getFileIDsForLfns(lfns).keys()
    if not fileIDs:
      return S_ERROR('TransformationDB.setFileSEForTransformation: No files found.')
    else:
      req = "UPDATE T_%s SET TargetSE='%s' WHERE FileID IN (%s);" % (transID,se,intListToString(fileIDs))
      return self._update(req)

  def setFileStatusForTransformation(self,transName,status,lfns):
    """ Set file status for the given transformation identified by transID
        for the given stream for files in the list of lfns
    """
    transID = self.getTransformationID(transName)
    fileIDs = self.__getFileIDsForLfns(lfns).keys()
    if not fileIDs:
      return S_ERROR('TransformationDB.setFileStatusForTransformation: No files found.')
    else:
      req = "UPDATE T_%s SET Status='%s' WHERE FileID IN (%s);" % (transID,status,intListToString(fileIDs))
      return self._update(req)

  def setFileJobID(self,transName,jobID,lfns):
    """ Set file job ID for the given transformation identified by transID
        for the given stream for files in the list of lfns
    """
    transID = self.getTransformationID(transName)
    fileIDs = self.__getFileIDsForLfns(lfns).keys()
    if not fileIDs:
      return S_ERROR('TransformationDB.setFileStatusForTransformation: No files found.')
    else:
      req = "UPDATE T_%s SET JobID='%s' WHERE FileID IN (%s);" % (transID,jobID,intListToString(fileIDs))
      return self._update(req)


  def deleteTransformation(self, transName):
    """ Remove the transformation specified by name or id
    """
    transID = self.getTransformationID(transName)
    if self.transformationExists(transID) > 0:
      req = "DELETE FROM Transformations WHERE TransformationID=%s;" % transID
      res = self._update(req)
      if not res['OK']:
        return res
      req = "DROP TABLE IF EXISTS T_%s;" % transID
      res = self._update(req)
      if not res['OK']:
        return res
      self.filters = self.__getFilters()
      return S_OK()
    else:
      return S_ERROR("No Transformation with the id '%s' in the TransformationDB" % transID)


####################################################################################
#
#  This part should correspond to the internal methods required for tranformation manipulation
#
####################################################################################

  def __addExistingFiles(self,transID):
    """ Add files that already exist in the DataFiles table to the
        transformation specified by the transID
    """
    # Add already existing files to this transformation if any
    filters = self.__getFilters(transID)
    req = "SELECT LFN,FileID FROM DataFiles;"
    res = self._query(req)
    if not res['OK']:
      return res
    for lfn,fileID in res['Value']:
      resultFilter = self.__filterFile(lfn,filters)
      if resultFilter:
        result = self.__addFileToTransformation(fileID,resultFilter)
    return S_OK()

  def __addTransformationTable(self,transID):
    """ Add a new Transformation table for a given transformation
    """
    req = """CREATE TABLE T_%s(
FileID INTEGER NOT NULL,
Status VARCHAR(32) DEFAULT "Unused",
ErrorCount INT(4) NOT NULL DEFAULT 0,
JobID VARCHAR(32),
TargetSE VARCHAR(32) DEFAULT "Unknown",
UsedSE VARCHAR(32) DEFAULT "Unknown",
PRIMARY KEY (FileID)
)""" % str(transID)
    res = self._update(req)
    if not res['OK']:
      return S_ERROR("TransformationDB.__addTransformationTable: Failed to add new transformation table",res['Message'])
    return S_OK()

  def __getFilters(self,transID=None):
    """ Get filters for all defined input streams in all the transformations.
        If transID argument is given, get filters only for this transformation.
    """
    resultList = []
    req = "SELECT TransformationID,FileMask FROM Transformations"
    result = self._query(req)
    if not result['OK']:
      return result
    for transID,mask in result['Value']:
      refilter = re.compile(mask)
      resultList.append((transID,refilter))

    return resultList


  def __addFileToTransformation(self,fileID,resultFilter):
    """Add file to transformations

       Add file to all the transformations which require this kind of files.
       resultFilter is a list of pairs transID,StreamName which needs this file
    """

    if resultFilter:
      for transID in resultFilter:
        req = "SELECT * FROM T_%s WHERE FileID=%s;" % (transID,fileID)
        res = self._query(req)
        if not res['OK']:
          return res
        if not res['Value']:
          req = "INSERT INTO T_%s (FileID) VALUES (%s);"  % (transID,fileID)
          res = self._update(req)
          if not res['OK']:
            return res
          gLogger.info("TransformationDB.__addFileToTransformation: File %s added to transformation %s." % (fileID,transID))
        else:
          gLogger.info("TransformationDB.__addFileToTransformation: File %s already present in transformation %s." % (fileID,transID))
    return S_OK()

  def __getFileIDsForLfns(self,lfns):
    """ Get file IDs for the given list of lfns
    """
    fids = {}
    req = "SELECT LFN,FileID FROM DataFiles WHERE LFN in (%s);" % stringListToString(lfns)
    res = self._query(req)
    if not res['OK']:
      return res
    for lfn,fileID in res['Value']:
      fids[fileID] = lfn
    return fids

  def __filterFile(self,lfn,filters=None):
    """Pass the input file through a filter

       Apply input file filters of the currently active transformations to the
       given lfn and select appropriate transformations if any. If 'filters'
       argument is given, use this one instead of the global filter list.
       Filter list is composed of triplet tuples transID,StreamName,refilter
       where refilter is a compiled RE object to check the lfn against.
    """
    result = []
    # If the list of filters is given use it, otherwise use the complete list
    if filters:
      for transID,refilter in filters:
        if refilter.search(lfn):
          result.append(transID)
    else:
      for transID,refilter in self.filters:
        if refilter.search(lfn):
          result.append(transID)
    return result

####################################################################################
#
#  This part should correspond to the DIRAC Standard File Catalog interface
#
####################################################################################

  def addDirectory(self,path,force=False):
    """ Adds all the files stored in a given directory in the LFC catalog.
    """
    gLogger.info("TransformationDB.addDirectory: Attempting to populate %s." % path)
    if self.catalog is None:
      res = self.__getLFCClient()
      if not res['OK']:
        return res
    start = time.time()
    res = self.catalog.getDirectoryReplicas(path)
    end = time.time()
    if not res['OK']:
      gLogger.error("TransformationDB.addDirectory: Failed to get replicas." % res['Message'])
      return res
    elif not res['Value']['Successful'].has_key(path):
      gLogger.error("TransformationDB.addDirectory: Failed to get replicas." % res['Message'])
      return res
    else:
      gLogger.info("TransformationDB.addDirectory: Obtained %s replicas in %s seconds." % (path,end-start))
      replicas = res['Value'][path]
      fileCount = 0
      filesAdded = 0
      replicaCount = 0
      replicasAdded = 0
      replicasFailed = 0
      replicasForced = 0
      for lfn,replicaDict in replicas.items():
        fileCount += 1
        addFile = False
        for se,pfn in replicaDict.items():
          replicaCount += 1
          replicaTuples = [(lfn,pfn,se,'IGNORED-MASTER')]
          res = self.addReplica(replicaTuples,force)
          if not res['OK']:
            replicasFailed += 1
          elif not res['Value']['Successful'].has_key(lfn):
            replicasFailed += 1
          else:
            addFile = True
            if res['Value']['Successful'][lfn]['AddedToCatalog']:
              replicasAdded += 1
            if res['Value']['Successful'][lfn]['Forced']:
              replicasForced += 1
        if addFile:
          filesAdded += 1
      infoStr = "Found %s files and %s replicas\n" % (fileCount,replicaCount)
      infoStr = "%sAdded %s files.\n" % (infoStr,filesAdded)
      infoStr = "%sAdded %s replicas.\n" % (infoStr,replicasAdded)
      infoStr = "%sFailed to add %s replicas.\n" % (infoStr,replicasFailed)
      infoStr = "%sForced %s replicas." % (infoStr,replicasForced)
      gLogger.info(infoStr)
      return S_OK(infoStr)
      
  def updateTransformation(self,transName):
    """ Update the transformation w.r.t files registered already
    """    
    
    transID = self.getTransformationID(transName)
    result = self.__addExistingFiles(transID)
    return result

  def __getLFCClient(self):
    """Gets the LFC client instance
    """
    try:
      self.catalog = LcgFileCatalogClient()
      self.catalog.setAuthenticationID('/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev')
      return S_OK()
    except Exception,x:
      errStr = "TransformationDB.__getLFCClient: Failed to create LcgFileCatalogClient"
      gLogger.exception(errStr, str(x))
      return S_ERROR(errStr)

  def exists(self,lfns):
    """ Check the presence of the lfn in the TransformationDB DataFiles table
    """
    gLogger.info("TransformationDB.exists: Attempting to determine existence of %s files." % len(lfns))
    fileIDs = self.__getFileIDsForLfns(lfns)
    failed = {}
    successful = {}
    for lfn in lfns:
      if not lfn in fileIDs.values():
        successful[lfn] = False
      else:
        successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeFile(self,lfns):
    """ Remove file specified by lfn from the ProcessingDB
    """
    gLogger.info("TransformationDB.removeFile: Attempting to remove %s files." % len(lfns))
    res = self.getAllTransformations()
    if res['OK']:
      for transformation in res['Value']:
        transName = transformation['Name']
        res = self.setFileStatusForTransformation(transName,'deleted',lfns)
    fileIDs = self.__getFileIDsForLfns(lfns)
    failed = {}
    successful = {}
    for lfn in lfns:
      if not lfn in fileIDs.values():
        successful[lfn] = True
    if len(fileIDs.keys()) > 0:
      req = "DELETE Replicas FROM Replicas WHERE FileID IN (%s);" % intListToString(fileIDs.keys())
      print req
      res = self._update(req)
      if not res['OK']:
        return S_ERROR("TransformationDB.removeFile: Failed to remove file replicas.")
      req = "DELETE FROM DataFiles WHERE FileID IN (%s);" % intListToString(fileIDs.keys())
      print req
      res = self._update(req)
      if not res['OK']:
        return S_ERROR("TransformationDB.removeFile: Failed to remove files.")
    for lfn in fileIDs.values():
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
    print fileTuples
    res = self.addFile(fileTuples,force)
    return res


  def addFile(self,fileTuples,force=False):
    """  Add a new file to the TransformationDB together with its first replica.
    """
    gLogger.info("TransformationDB.addFile: Attempting to add %s files." % len(fileTuples))
    successful = {}
    failed = {}
    for lfn,pfn,size,se,guid,checksum in fileTuples:

      passFilter = False
      forced = False
      retained = False
      lFilters = self.__filterFile(lfn)
      if lFilters:
        passFilter = True
        retained = True
      elif force:
        forced = True
        retained = True

      addedToCatalog = False
      addedToTransformation = False

      if retained:
        res = self.__addFile(lfn,pfn,se)
        if not res['OK']:
          failed[lfn] = "TransformationDB.addFile: Failed to add file. %s" % res['Message']
        else:
          addedToCatalog = True
          fileID = res['Value']['FileID']
          fileExists = res['Value']['LFNExist']
          replicaExists = res['Value']['ReplicaExist']
          if lFilters:
            res = self.__addFileToTransformation(fileID,lFilters)
            if res['OK']:
              addedToTransformation = True
          successful[lfn] = {'PassFilter':passFilter,'Retained':retained,'Forced':forced,'AddedToCatalog':addedToCatalog,'AddedToTransformation':addedToTransformation,'FileExists':fileExists,'ReplicaExists':replicaExists}
      else:
        successful[lfn] = {'PassFilter':passFilter,'Retained':retained,'Forced':forced,'AddedToCatalog':addedToCatalog,'AddedToTransformation':addedToTransformation}
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def __addFile(self,lfn,pfn,se):
    """ Add file without checking for filters
    """
    lfn_exist = 0
    fileIDs = self.__getFileIDsForLfns([lfn])
    if lfn in fileIDs.values():
      lfn_exist = 1
      fileID = fileIDs.keys()[0]
    else:
      self.lock.acquire()
      req = "INSERT INTO DataFiles (LFN,Status) VALUES ('%s','New');" % lfn
      res = self._update(req)
      if not res['OK']:
        self.lock.release()
        return S_ERROR("TransformationDB.__addFile: %s" % res['Message'])
      req = " SELECT LAST_INSERT_ID();"
      res = self._query(req)
      self.lock.release()
      if not res['OK']:
        return S_ERROR("TransformationDB.__addFile: %s" % res['Message'])
      fileID = res['Value'][0][0]

    replica_exist = 0
    res = self.__addReplica(fileID,se,pfn)
    if not res['OK']:
      self.removeFile([lfn])
      return S_ERROR("TransformationDB.__addFile: %s" % res['Message'])
    elif not res['Value']:
      replica_exist = 1

    resDict = {'FileID':fileID,'LFNExist':lfn_exist,'ReplicaExist':replica_exist}
    return S_OK(resDict)

  def __addReplica(self,fileID,se,pfn):
    """ Add a SE,PFN for the given fileID in the Replicas table.

        If the SQL fails this method returns S_ERROR()
        If the replica already exists it returns S_OK(0)
        If the replica was inserted it returns S_OK(1)
    """
    req = "SELECT FileID FROM Replicas WHERE FileID=%s AND SE='%s';" % (fileID,se)
    res = self._query(req)
    if not res['OK']:
      return S_ERROR("TransformationDB.addReplica: %s" % res['Message'])
    elif len(res['Value']) == 0:
      req = "INSERT INTO Replicas (FileID,SE,PFN) VALUES (%s,'%s','%s');" % (fileID,se,pfn)
      res = self._update(req)
      if not res['OK']:
        return S_ERROR("TransformationDB.addReplica: %s" % res['Message'])
      else:
        return S_OK(1)
    else:
      return S_OK(0)


  def removeReplica(self,replicaTuples):
    """ Remove replica pfn of lfn. If this is the last replica then remove the file.
    """
    gLogger.info("TransformationDB.removeReplica: Attempting to remove %s replicas." % len(replicaTuples))
    successful = {}
    failed = {}
    for lfn,pfn,se in replicaTuples:
      req = "DELETE r FROM Replicas as r,DataFiles as d WHERE r.FileID=d.FileID AND d.LFN='%s' AND r.SE='%s';" % (lfn,se)
      res = self._update(req)
      if not res['OK']:
        failed[lfn] = "TransformationDB.removeReplica. Failed to remove replica. %s" % res['Message']
      else:
          successful[lfn] = True
          failedToRemove = False
          res = self.getReplicas([lfn],True)
          if not res['OK']:
            gLogger.error("TransformationDB.removeReplica. Failed to get replicas for file removal",res['Message'])
            failedToRemove = True
          elif not res['Value']['Successful'].has_key(lfn):
            gLogger.error("TransformationDB.removeReplica. Failed to get replicas for file removal",res['Value']['Failed'][lfn])
            failedToRemove = True
          else:
            replicas = res['Value']['Successful'][lfn]
            if len(replicas.keys()) == 0:
              res = self.removeFile([lfn])
              if not res['OK']:
                failedToRemove = True
              elif not res['Value']['Successful'].has_key(lfn):
                failedToRemove = True
          if failedToRemove:
            successful.pop(lfn)
            failed[lfn] = "TransformationDB.removeReplica. Failed to remove replica and associated file."
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def getReplicas(self,lfns,getAll=False):
    """ Get replicas for the files specified by the lfn list
    """
    gLogger.info("TransformationDB.getReplicas: Attempting to get replicas for %s files." % len(lfns))
    fileIDs = self.__getFileIDsForLfns(lfns)
    failed = {}
    successful = {}
    for lfn in lfns:
      if not lfn in fileIDs.values():
        successful[lfn] = {}
    if len(fileIDs.keys()) > 0:
      req = "SELECT FileID,SE,PFN,Status FROM Replicas WHERE FileID IN (%s);" % intListToString(fileIDs.keys())
      res = self._query(req)
      if not res['OK']:
        return res
      for fileID,se,pfn,status in res['Value']:
        takeReplica = True
        if status != "AprioriGood":
          if not getAll:
            takeReplica = False
        if takeReplica:
          lfn = fileIDs[fileID]
          if not successful.has_key(lfn):
            successful[lfn] = {}
          successful[lfn][se] = pfn
    for lfn in fileIDs.values():
      if not successful.has_key(lfn):
        successful[lfn] = {} #"TransformationDB.getReplicas: No replicas found."
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def setReplicaStatus(self,replicaTuples):
    """Set status for the supplied replica tuples
    """
    gLogger.info("TransformationDB.setReplicaStatus: Attempting to set statuses for %s replicas." % len(replicaTuples))
    successful = {}
    failed = {}
    for lfn,pfn,se,status in replicaTuples:
      fileIDs = self.__getFileIDsForLfns([lfn])
      if not lfn in fileIDs.values():
        failed[lfn] = "TransformationDB.setReplicaStatus: File not found."
      else:
        fileID = fileIDs.keys()[0]
        if se.lower() == "any" :
          req = "UPDATE Replicas SET Status='%s' WHERE FileID=%s;" % (status,fileID)
        else:
          req = "UPDATE Replicas SET Status='%s' WHERE FileID= %s AND SE = '%s';" % (status,fileID,se)
        res = self._update(req)
        if not res['OK']:
          failed[lfn] = "TransformationDB.setReplicaStatus: Failed to update status."
        else:
          successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def getReplicaStatus(self,replicaTuples):
    """ Get the status for the supplied file replicas
    """
    gLogger.info("TransformationDB.getReplicaStatus: Attempting to get statuses of file replicas.")
    lfns = []
    for lfn,se in replicaTuples:
      lfns.append(lfn)
    fileIDs = self.__getFileIDsForLfns(lfns)
    failed = {}
    successful = {}
    for lfn,se in replicaTuples:
      if not lfn in fileIDs.values():
        failed[lfn] = "TransformationDB.getReplicaStatus: File not found."
    req = "SELECT FileID,SE,Status FROM Replicas WHERE FileID IN (%s);" % intListToString(fileIDs.keys())
    res = self._query(req)
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
    for lfn,pfn,oldse,newse in replicaTuples:
      fileIDs = self.__getFileIDsForLfns([lfn])
      if not lfn in fileIDs.values():
        failed[lfn] = "TransformationDB.setReplicaHost: File not found."
      else:
        ############## Need to consider the case where the new se already exists for the file (breaks the primary key restriction)
        fileID = fileIDs.keys()[0]
        req = "UPDATE Replicas SET SE='%s' WHERE FileID=%s AND SE ='%s';" % (newse,fileID,oldse)
        res = self._update(req)
        if not res['OK']:
          failed[lfn] = "TransformationDB.setReplicaHost: Failed to update status."
        else:
          successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)
