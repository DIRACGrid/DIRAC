########################################################################
# $Id: TransformationDB.py,v 1.2 2008/01/25 13:59:13 acsmith Exp $
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

  def __getLFCClient(self):
    """Gets the LFC client instance
    """

    try:
      self.catalog = LcgFileCatalogClient()
      self.catalog.setAuthenticationID('/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev')
      result = S_OK()
    except Exception,x:
      self.log.exception("Failed to create LcgFileCatalogClient")
      result = S_ERROR(str(x))

    return result

####################################################################################
#
#  This part contains the transformation manipulation methods
#
####################################################################################

  def addTransformation(self,name,fileMask,groupSize):
    """ Add new transformation definition including its input streams
    """
    inFields = ['TransformationName', 'CreationDate', 'FileMask', 'FileGroupSize']
    inValues = [name,'NOW()',fileMask,groupSize]
    self.lock.acquire()
    result = self._insert('Transformations',inFields,inValues)
    if not result['OK']:
      self.lock.release()
      return result
    req = " SELECT LAST_INSERT_ID()"
    result = self._query(req)
    self.lock.release()
    if not result['OK']:
      return result
    transID = int(result['Value'])
    self.filters.append((transID,re.compile(fileMask)))
    result = self.__addTransformationTable(transID)
    # Add already existing files to this transformation if any
    result = self.__addExistingFiles(transID)
    return S_OK(transID)

  def removeTransformation(self,name):
    """ Remove the transformation specified by transID
    """
    res = self.getTransformation(name)
    transID = res['Transformation']['TransID']
    req = "DELETE FROM Transformations WHERE TransformationName='"+str(name)+"'"
    result = self._update(req)
    if not result['OK']:
      return result
    req = "DROP TABLE IF EXISTS T_"+str(transID)
    result = self._update(req)
    if not result['OK']:
      return result
    # Update the filter information
    self.filters = self.__getFilters()
    return S_OK()

  def setTransformationStatus(self,transID,status):
    """ Set the status of the transformation specified by transID
    """
    req = "UPDATE Transformations SET Status='"+status+"' WHERE TransformationID="+transID
    result = self._update(req)
    if not result['OK']:
      return result
    return S_OK()

  def getName(self):
    """  Get the database name
    """
    return self.dbname

  def addDirectory(self,directory,force=False):
    """ Adds all the files stored in a given directory in the LFC catalog.
        Adds all the replicas of the processed files
    """
    print "Adding files in the directory",directory
    if self.catalog is None:
      result = self.__getLFCClient()
    if self.catalog is None:
      print "Failed to get the LFC client"
      return S_ERROR("Failed to get the LFC client")

    start = time.time()
    result = self.catalog.getPfnsInDir(directory)
    end = time.time()
    print "getPfnsInDir",directory,"operation time",(end-start)
    if result['Status'] == 'OK':
      lfndict =  result['Replicas']
      counter_lfn = 0
      counter = 0
      lfns = []
      for lfn,repdict in lfndict.items():
        counter_lfn += 1
        for se,pfn in repdict.items():
          counter += 1
          lfns.append((se,lfn))

      result = self.addFiles(lfns,force)
      added = result['Added']
      forced = result['Forced']

      rstring = "Lookup "+str(counter_lfn)+" lfns, added "+ \
                str(added)+ " replicas, forced"+ str(forced)+" replicas"
      print rstring
      return S_OK(rstring)
    else:
      print "Failed to ls directory",directory
      return S_ERROR("Failed to ls directory "+directory)


  def getTransformationStats(self,production):
    """Get Transformation statistics

       Get the statistics of Transformation idendified by production ID
    """

    result = self.getTransformation(production)
    if result['Status'] == "OK":
      transID = result['Transformation']['TransID']
      req = "SELECT COUNT(*) FROM T_"+str(transID)
      result = self.query(req)
      if not result['OK']:
        return result
      total = int(result['Value'][0][0])
      req = "SELECT COUNT(*) FROM T_"+str(transID)+" WHERE Status='unused'"
      result = self.query(req)
      if not result['OK']:
        return result
      unused = int(result['Value'][0][0])
      req = "SELECT COUNT(*) FROM T_"+str(transID)+" WHERE Status='assigned'"
      result = self.query(req)
      if not result['OK']:
        return result
      assigned = int(result['Value'][0][0])
      req = "SELECT COUNT(*) FROM T_"+str(transID)+" WHERE Status='done'"
      result = self.query(req)
      if not result['OK']:
        return result
      done = int(result['Value'][0][0])

      stats = {}
      stats['Total'] = total
      stats['Unused'] = unused
      stats['Assigned'] = assigned
      stats['Done'] = done
      result = S_OK()
      result['Stats'] = stats
      return result

    else:
      print "ProcessingDB: unknown transformation",production
      return S_ERROR("ProcessingDB: unknown transformation")

  def getTransformation(self,name):
    """Get Transformation definition

       Get the parameters of Transformation idendified by production ID
    """

    result = S_OK()
    transdict = {}
    req = "SELECT TransformationID,Status,FileMask FROM Transformations "+\
          "WHERE TransformationName='"+str(name)+"'"
    result = self.query(req)
    if not result['OK']:
      return result
    if result['Value']:
      row = result['Value'][0]
      transdict['TransID'] = row[0]
      transdict['Status'] = row[1]
      transdict['FileMask'] = row[2]
      result["Transformation"] = transdict
      return result
    else:
      return S_ERROR('Transformation not found')

  def modifyTransformationInput(self,name,fileMask):
    """ Modify the input stream definition for the given transformation
        identified by production
    """

    result = self.getTransformation(name)
    transID = result["Transformation"]['TransID']
    req = "UPDATE Transformations SET FileMask='%s' WHERE TransformationID=%s" % (fileMask,str(transID))
    result = self._update(req)
    if not result['OK']:
      return result

    return S_OK()

  def changeTransformationName(self,transID,new_name):
    """ Change the transformation name
    """

    req = "UPDATE Transformations SET TransformationName='"+new_name+ \
          "' where TransformationID="+str(transID)
    result = self._update(req)
    if not result['OK']:
      return result

    result = S_OK()
    return result

  def getAllTransformations(self):
    """ Get parameters of all the Transformations
    """

    result = S_OK()
    translist = []
    req = "SELECT TransformationID,TransformationName,Status FROM Transformations "
    print req
    resQ = self._query(req)
    if not resQ['OK']:
      return resQ
    for row in resQ['Value']:
      transdict = {}
      transdict['TransID'] = row[0]
      transdict['Name'] = row[1]
      transdict['Status'] = row[2]
      translist.append(transdict)

    result["Transformations"] = translist
    return result

  def getFilesForTransformation(self,name,order_by_job=False):
    """ Get files and their status for the given transformation
    """

    res = self.getTransformation(name)
    if res['Status'] != "OK":
      return S_ERROR("Transformation is not found")
    transID = res['Transformation']['TransID']

    flist = []
    req = "SELECT LFN,p.Status,p.JobID,p.UsedSE FROM DataFiles AS d,T_"+ \
          str(transID)+" AS p WHERE "+"p.FileID=d.FileID ORDER by LFN"
    if order_by_job:
      req = "SELECT LFN,p.Status,p.JobID,p.UsedSE FROM DataFiles AS d,T_"+ \
            str(transID)+" AS p WHERE "+"p.FileID=d.FileID ORDER by p.JobID"

    result = self._query(req)
    if not result['OK']:
      return result
    for lfn,status,jobid,usedse in dbc.fetchall():
      print lfn,status,jobid,usedse
      fdict = {}
      fdict['LFN'] = lfn
      fdict['Status'] = status
      if jobid is None: jobid = 'No JobID assigned'
      fdict['JobID'] = jobid
      fdict['UsedSE'] = usedse
      flist.append(fdict)

    result = S_OK()
    result['Files'] = flist
    print result
    return result


  def getInputData(self,name,status):
    """ Get input data for the given transformation, only files
        with a given status which is defined for the file replicas.
    """

    result = self.getTransformation(name)
    if result['Status'] != "OK":
      return S_ERROR("Transformation is not found")
    transID = result['Transformation']['TransID']

    reslist = []
    req = "SELECT FileID from T_"+str(transID)+" WHERE Status='unused'"
    result = self._query(req)
    if not result['OK']:
      return result

    if result['Value']:
      ids = [ str(x[0]) for x in result['Value'] ]
    if not ids:
      result = S_OK()
      result['Data'] = []
      return result

    fileids = string.join(ids,",")
    req = "SELECT LFN,SE FROM Replicas,DataFiles WHERE Replicas.FileID=DataFiles.FileID and "+ \
          "Replicas.FileID in ("+fileids+")"
    result = self._query(req)
    if not result['OK']:
      return result
    for lfn,se in result['Value']:
      reslist.append((lfn,se))

    result = S_OK()
    result['Data'] = reslist
    return result

  def setFileSEForTransformation(self,name,se,lfns):
    """ Set file SE for the given transformation identified by transID
        for files in the list of lfns
    """

    result = self.getTransformation(name)
    if result['Status'] != "OK":
      return S_ERROR("Transformation is not found")
    transID = result['Transformation']['TransID']

    fids = self.__getFileIDsForLfns(lfns).keys()

    if fids:
      s_fids = string.join(fids,",")
      req = "UPDATE T_"+str(transID)+" SET FileSE='"+se+"' WHERE FileID IN ( "+ \
            s_fids+" ) "
      print req
      result = self._update(req)
      return result

    return S_ERROR('Files not found')

  def setFileStatusForTransformation(self,name,status,lfns):
    """ Set file status for the given transformation identified by transID
        for the given stream for files in the list of lfns
    """

    fids = self.__getFileIDsForLfns(lfns).keys()

    result = self.getTransformation(name)
    if result['Status'] != "OK":
      return S_ERROR("Transformation is not found")
    transID = result['Transformation']['TransID']

    if fids:
      s_fids = string.join(fids,",")
      req = "UPDATE T_"+str(transID)+" SET Status='"+status+"' WHERE FileID IN ( "+ \
              s_fids+" )"

      print req
      result = self._update(req)
      return result

  def setFileStatus(self,name,lfn,status):
    """ Set file status for the given production identified by production
        for the given lfn
    """
    result = self.getTransformation(name)
    if result['Status'] == "OK":
      transID = result['Transformation']['TransID']
      result = self.setFileStatusForTransformation(transID,status,[lfn])
      if result['Status'] != "OK":
        print "Failed to set status for file",lfn,"transformation",transID,'/',name
    else:
      print "Failed to set status for file",lfn,"transformation",transID,'/',name

    return result


  def setFileJobID(self,name,jobID,lfns):
    """ Set file job ID for the given transformation identified by transID
        for the given stream for files in the list of lfns
    """

    fids = self.__getFileIDsForLfns(lfns).keys()

    result = self.getTransformation(name)
    if result['Status'] != "OK":
      return S_ERROR("Transformation is not found")
    transID = result['Transformation']['TransID']


    if fids:
      s_fids = string.join(fids,",")
      req = "UPDATE T_"+str(transID)+" SET JobID='"+jobID+"' WHERE FileID IN ( "+ s_fids+" )"
      result = self._update(req)
      return result

    else:
      return S_ERROR('Files not found')


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
    req = "SELECT LFN,FileID FROM DataFiles"
    result = self._query(req)
    if not result['OK']:
      return result
    files = result['Value']
    for lfn,fileID in files:
      resultFilter = self.__filterFile(lfn,filters)
      if resultFilter:
        result = self.__addFileToTransformation(fileID,resultFilter)
    return S_OK()

  def __addTransformationTable(self,transID):
    """ Add a new Transformation table for a given transformation
    """
    req = """CREATE TABLE T_%s(
FileID INTEGER NOT NULL,
Status VARCHAR(32) DEFAULT "unused",
ErrorCount INT(4) NOT NULL DEFAULT 0,
JobID VARCHAR(32),
UsedSE VARCHAR(32) DEFAULT "Unknown",
PRIMARY KEY (FileID,StreamName)
)""" % str(transID)
    result = self._update(req)
    if not result['OK']:
      return S_ERROR("Failed to add new transformation table "+str(x))
    else:
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
        req = "SELECT * FROM T_"+str(transID)+" WHERE FileID="+str(fileID)
        result = self._query(req)
        if not result['OK']:
          return result
        if result['Value']:
          req = "INSERT INTO T_%s ( FileID ) VALUES ( '%s' )" % (str(transID),str(fileID))
          result = self._update(req)
          if not result['OK']:
            return result
        else:
          print "File",fileID,"already added to transformation",transID

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
      for transformation in result['Value']:
        transID = transformation['TransID']
        res = self.setFileStatusForTransformation(transID,'deleted',lfns)
    fileIDs = self.__getFileIDsForLfns(lfns)
    failed = {}
    successful = {}
    for lfn in lfns:
      if not lfn in fileIDs.values():
        successful[lfn] = True
    req = "DELETE Replicas FROM Replicas WHERE FileID IN (%s);" % intListToString(fileIDs.keys())
    res = self._update(req)
    if not res['OK']:
      return S_ERROR("TransformationDB.removeFile: Failed to remove file replicas.")
    req = "DELETE FROM DataFiles WHERE FileID IN (%s);" % intListToString(fileIDs.keys())
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
      fileTuples.append((lfn,pfn,0,se,'IGNORED-GUID'))
    res = self.addFile(fileTuples,force)
    return res

  def addFile(self,fileTuples,force=False):
    """  Add a new file to the TransformationDB together with its first replica.
    """
    gLogger.info("TransformationDB.addFile: Attempting to add %s files." % len(fileTuples))
    successful = {}
    failed = {}
    for lfn,pfn,size,se,guid in fileTuples:

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
          if pass_filter:
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
      fileID = result['Value'][0][0]

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
    elif len(result['Value']) == 0:
      return S_OK(0)
    else:
      req = "INSERT INTO Replicas (FileID,SE,PFN) VALUES (%s,'%s','%s');" % (fileID,se,pfn)
      res = self._update(req)
      if not res['OK']:
        return S_ERROR("TransformationDB.addReplica: %s" % res['Message'])
      else:
        return S_OK(1)

  def removeReplica(self,replicaTuples):
    """ Remove replica pfn of lfn. If this is the last replica then remove the file.
    """
    gLogger.info("TransformationDB.removeReplica: Attempting to remove %s replicas." % len(replicaTuples))
    successful = {}
    failed = {}
    for lfn,pfn,se in replicaTuples:
      res = self.getReplicas([lfn])
      if not res['OK']:
        failed[lfn] = "TransformationDB.removeReplica. Failed to get replicas before removal. %s" % res['Message']
      elif not res['Value']['Successful'].has_key(lfn):
        failed[lfn] = "TransformationDB.removeReplica. Failed to get replicas before removal. %s" % res['Value']['Failed'][lfn]
      else:
        replicas = res['Value']['Successful'][lfn]
        if len(replics.keys()) < 2:
          res = self.removeFile([lfn])
          if not res['OK']:
            failed[lfn] = "TransformationDB.removeReplica. Failed to remove replica. %s" % res['Message']
          else:
            successful[lfn] = True
        else:
          req = "DELETE FROM Replicas as r,DataFiles as d WHERE r.FileID=d.FileID AND d.LFN='%s' r.SE='%s';" % (lfn,se)
          res = self._update(req)
          if not res['OK']:
            failed[lfn] = "TransformationDB.removeReplica. Failed to remove replica. %s" % res['Message']
          else:
            successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def getReplicas(self,lfns):
    """ Get replicas for the files specified by the lfn list
    """
    gLogger.info("TransformationDB.getReplicas: Attempting to get replicas for %s files." % len(lfns))
    fileIDs = self.__getFileIDsForLfns(lfns)
    failed = {}
    for lfn in lfns:
      if not lfn in fileIDs.values():
        failed[lfn] = "TransformationDB.getReplicas: File not found."
    req = "SELECT FileID,SE,PFN FROM Replicas WHERE FileID IN (%s);" % intListToString(fileIDs.keys())
    res = self._query(req)
    if not res['OK']:
      return res
    successful = {}
    for fileID,se,pfn in res['Value']:
      lfn = fileIDs[fileID]
      if not successful.has_key(lfn):
        successful[lfn] = {}
      successful[lfn][se] = pfn
    for lfn in fileIDs.values():
      if not successful.has_key(lfn):
        failed[lfn] = "TransformationDB.getReplicas: No replicas found."
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def setReplicaStatus(self,replicaTuples):
    """Set status for the supplied replica tuples
    """
    gLogger.info("TransformationDB.getReplicaStatus: Attempting to set statuses for %s replicas." % len(replicaTuples))
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

  def getReplicaStatus(self,lfns):
    """ Get the status for the supplied file replicas
    """
    gLogger.info("TransformationDB.getReplicaStatus: Attempting to get statuses of file replicas.")
    fileIDs = self.__getFileIDsForLfns(lfns)
    failed = {}
    for lfn in lfns:
      if not lfn in fileIDs.values():
        failed[lfn] = "TransformationDB.getReplicaStatus: File not found."
    req = "SELECT FileID,SE,Status FROM Replicas WHERE FileID IN (%s);" % intListToString(fileIDs.keys())
    res = self._query(req)
    if not res['OK']:
      return res
    successful = {}
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
    for lfn,pfn,oldse,newse in replicaTuples:
      fileIDs = self.__getFileIDsForLfns([lfn])
      if not lfn in fileIDs.values():
        failed[lfn] = "TransformationDB.setReplicaHost: File not found."
      else:
        fileID = fileIDs[lfn]
        req = "UPDATE Replicas SET SE='%s' WHERE FileID=%s AND SE ='%s';" % (newse,fileID,oldse)
        res = self._update(req)
        if not res['OK']:
          failed[lfn] = "TransformationDB.setReplicaHost: Failed to update status."
        else:
          successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)
