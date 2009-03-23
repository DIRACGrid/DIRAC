""" Class for the LCG File Catalog Client

"""
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase import FileCatalogueBase
from stat import *
import os, re, string, commands, types,time

global importCorrectly
try:
  import lfc
  importCorrectly = True
  gLogger.debug("LcgFileCatalogClient.__init__: Successfully imported lfc module.")
except ImportError, x:
  gLogger.exception("LcgFileCatalogClient.__init__: Failed to import lfc module.")
  importCorrectly = False

class LcgFileCatalogClient(FileCatalogueBase):

  def __init__(self,infosys=None,host=None):

    if importCorrectly:
      self.valid = True
    else:
      self.valid = False

    self.host = host
    result = gConfig.getOption('/DIRAC/Setup')
    if not result['OK']:
      gLogger.fatal('Failed to get the /DIRAC/Setup')
      return
    setup = result['Value']

    if host:
      os.environ['LFC_HOST'] = host
    if infosys:
      os.environ['LCG_GFAL_INFOSYS'] = infosys

    result = gConfig.getOption('/LocalSite/Site')
    if not result['OK']:
      gLogger.error('Failed to get the /LocalSite/Site')
      self.site = 'Unknown'
    else:
      self.site = result['Value']

    self.prefix = '/grid'
    self.timeout = 30
    self.session = False
    self.name = "LFC"

  ####################################################################
  #
  # These are the get/set methods for use within the client
  #

  def isOK(self):
    return self.valid

  def getName(self):
    return S_OK(self.name)

  ####################################################################
  #
  # These are the methods for session/transaction manipulation
  #

  def __openSession(self):
    """Open the LFC client/server session"""
    sessionName = 'DIRAC_%s.%s at %s' % (DIRAC.majorVersion,DIRAC.minorVersion,self.site)
    lfc.lfc_startsess(self.host,sessionName)
    self.session = True

  def __closeSession(self):
    """Close the LFC client/server session"""
    lfc.lfc_endsess()
    self.session = False

  def __startTransaction(self):
    """ Begin transaction for one time commit """
    transactionName = 'Transaction: DIRAC_%s.%s at %s' % (DIRAC.majorVersion,DIRAC.minorVersion,self.site)
    lfc.lfc_starttrans(self.host,transactionName)
    self.transaction = True

  def __abortTransaction(self):
    """ Abort transaction """
    lfc.lfc_aborttrans()
    self.transaction = False

  def __endTransaction(self):
    """ End transaction gracefully """
    lfc.lfc_endtrans()
    self.transaction = False

  def setAuthorizationId(self,dn):
    """ Set authorization id for the proxy-less LFC communication """
    lfc.lfc_client_setAuthorizationId(0,0,'GSI',dn)

  ####################################################################
  #
  # The following are read methods for paths
  #

  def exists(self,path):
    """ Check if the path exists
    """
    res = self.__checkArgumentFormat(path)
    if not res['OK']:
      return res
    lfns = res['Value']
    self.__openSession()
    failed = {}
    successful = {}
    for lfn,guid in lfns.items():
      res = self.__existsLfn(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      elif res['Value']:
        successful[lfn] = lfn
      elif not guid:
        successful[lfn] = False
      else:
        res = self.__existsGuid(guid)
        if not res['OK']:
          failed[lfn] = res['Message']
        elif not res['Value']:
          successful[lfn] = False
        else:
          successful[lfn] = self.__getLfnForGUID(guid)['Value']
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getPathPermissions(self,path):
    """ Determine the VOMs based ACL information for a supplied path
    """
    res = self.__checkArgumentFormat(path)
    if not res['OK']:   
      return res
    lfns = res['Value']
    self.__openSession()
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__getBasePath(path)
      if not res['OK']:
        failed[path] = res['Message']
      else:
        basePath = res['Value']
        res = self.__getACLInformation(basePath)
        if not res['OK']:
          failed[path] = res['Message']
        else:
          successful[path] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are read methods for files
  #

  def isFile(self, lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      elif S_ISREG(res['Value'].filemode):
        successful[lfn] = True
      else:
        successful[lfn] = False
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileMetadata(self,lfn):
    """ Returns the file metadata associated to a supplied LFN
    """
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res    
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        fstat = res['Value']
        successful[lfn] = {}
        successful[lfn]['Size'] = fstat.filesize
        successful[lfn]['CheckSumType'] = fstat.csumtype
        successful[lfn]['CheckSumValue'] = fstat.csumvalue
        successful[lfn]['GUID'] = fstat.guid
        successful[lfn]['Status'] = fstat.status
        successful[lfn]['CreationTime'] = time.ctime(fstat.ctime)
        successful[lfn]['ModificationTime'] = time.ctime(fstat.mtime)
        successful[lfn]['NumberOfLinks'] = fstat.nlink
        res = self.__getDNFromUID(fstat.uid)
        if res['OK']:
          successful[lfn]['OwnerDN'] = res['Value']
        else:
          successful[lfn]['OwnerDN'] = None  
        res = self.__getRoleFromGID(fstat.gid)
        if res['OK']:
          successful[lfn]['OwnerRole'] = res['Value']
        else:
          successful[lfn]['OwnerRole'] = None
        successful[lfn]['Permissions'] = S_IMODE(fstat.filemode)
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileSize(self, lfn):
    """ Get the size of a supplied file
    """
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat(lfn)
      if not res['OK']:   
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value'].filesize
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getReplicas(self,lfn,allStatus=False):
    """ Returns replicas for an LFN or list of LFNs
    """
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getFileReplicas(lfn,allStatus)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value']
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getReplicaStatus(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn,se in lfns.items():
      res = self.__getFileReplicaStatus(lfn,se)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value']
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following a write methods for files
  #

  def removeFile(self, lfn):
    """ Remove the supplied path
    """
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res  
    lfns = res['Value']
    self.__openSession()
    res = self.exists(lfns)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn,exists in res['Value']['Successful'].items():
      if not exists:
        successful[lfn] = True
      else:
        res = self.__unlinkPath(lfn)
        if res['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = res['Message']
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following a read methods for directories
  #

  def isDirectory(self,lfn):
    """ Determine whether the path is a directory
    """
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      elif S_ISDIR(res['Value'].filemode):
        successful[lfn] = True
      else:
        successful[lfn] = False
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}   
    return S_OK(resDict)

  def getDirectoryMetadata(self, lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']: 
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        fstat = res['Value']
        successful[lfn] = {}   
        successful[lfn]['Size'] = fstat.filesize
        successful[lfn]['CheckSumType'] = fstat.csumtype
        successful[lfn]['CheckSumValue'] = fstat.csumvalue
        successful[lfn]['GUID'] = fstat.guid
        successful[lfn]['Status'] = fstat.status
        successful[lfn]['CreationTime'] = time.ctime(fstat.ctime)
        successful[lfn]['ModificationTime'] = time.ctime(fstat.mtime)
        successful[lfn]['NumberOfSubPaths'] = fstat.nlink
        res = self.__getDNFromUID(fstat.uid)
        if res['OK']:
          successful[lfn]['OwnerDN'] = res['Value']
        else:
          successful[lfn]['OwnerDN'] = None
        res = self.__getRoleFromGID(fstat.gid)
        if res['OK']:
          successful[lfn]['OwnerRole'] = res['Value']
        else:
          successful[lfn]['OwnerRole'] = None
        successful[lfn]['Permissions'] = S_IMODE(fstat.filemode)
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectoryReplicas(self, lfn, allStatus=False):
    """ This method gets all of the pfns in the directory
    """
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    self.__openSession()
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__getDirectoryContents(path)
      if not res['OK']:
        failed[path] = res['Message']
      else:
        pathReplicas = {}
        files = res['Value']['Files']
        for lfn,fileDict in files.items():
          pathReplicas[lfn] = {}
          for se,seDict in fileDict['Replicas'].items():
            pfn = seDict['PFN']
            status = seDict['Status']
            if (status != 'P') or allStatus:
              pathReplicas[lfn][se] = pfn
        successful[path] = pathReplicas
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def listDirectory(self,lfn):
    """ Returns the result of __getDirectoryContents for multiple supplied paths
    """
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    self.__openSession()
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__getDirectoryContents(path)
      if res['OK']:
        successful[path] = res['Value']
      else:
        failed[path] = res['Message']
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectorySize(self, lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']   
    self.__openSession()
    failed = {}
    successful = {}   
    for path in lfns.keys():
      res = self.__getDirectorySize(path)
      if res['OK']:
        successful[path] = res['Value']
      else:
        failed[path] = res['Message']
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are write methods for directories
  #

  def removeDirectory(self, lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value'] 
    self.__openSession()
    res = self.exists(lfns)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn,exists in res['Value']['Successful'].items():
      if not exists:   
        successful[lfn] = True
      else:
        res = self.__removeDirectory(lfn)  
        if res['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = res['Message']
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def createDirectory(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    self.__openSession()
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__makeDirs(path)
      if res['OK']:
        successful[path] = True
      else:
        failed[path] = res['Message']
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)


  ####################################################################
  #
  # The following are read methods for links
  #

  def isLink(self, link):
    res = self.__checkArgumentFormat(link)
    if not res['OK']:
      return res
    links = res['Value']    
    # If we have less than three lfns to query a session doesn't make sense
    if len(links) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    self.__openSession()
    for link in links.keys():
      res = self.__getLinkStat(link)
      if not res['OK']:
        failed[link] = res['Message']
      elif S_ISLNK(res['Value'].filemode):
        successful[link] = True
      else:
        successful[link] = False
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def readLink(self,link):
    res = self.__checkArgumentFormat(link)
    if not res['OK']:
      return res
    links = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    if len(links) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for link in links.keys():
      res = self.__readLink(link)
      if res['OK']:
        successful[link] = res['Value']
      else:
        failed[link] = res['Message']
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are write methods for links
  #

  def createLink(self,link):
    res = self.__checkArgumentFormat(link)
    if not res['OK']:
      return res
    links = res['Value'] 
    # If we have less than three lfns to query a session doesn't make sense
    self.__openSession()
    failed = {}
    successful = {}
    for link,target in links.items():
      res = self.__makeDirs(os.path.dirname(link))
      if not res['OK']:
        failed[link] = res['Message']
      else:
        res = self.__makeLink(link,target)
        if not res['OK']:
          failed[link] = res['Message']
        else:
          successful[link] = target
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeLink(self,link):
    res = self.__checkArgumentFormat(link)
    if not res['OK']:
      return res
    links = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    if len(links) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for link in links.keys():
      res = self.__unlinkPath(link)  
      if not res['OK']:
        failed[link] = res['Message']
      else:
        successful[link] = True
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are read methods for datasets
  #
    
  def resolveDataset(self,dataset):
    res = self.__checkArgumentFormat(dataset)
    if not res['OK']:
      return res
    datasets = res['Value']
    self.__openSession()
    successful = {}
    failed = {}
    for datasetName in datasets.keys():
      res = self.__getDirectoryContents(datasetName)
      if not res['OK']:
        failed[datasetName] = res['Message']
      else:
        #linkDict = res['Value']['Links']
        linkDict = res['Value']['Files']
        datasetFiles = {}
        for link,fileMetadata in linkDict.items():
          #target = fileMetadata[link]['MetaData']['Target']
          target = link
          datasetFiles[target] = fileMetadata['Replicas']
        successful[datasetName] = datasetFiles
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are write methods for datasets
  # 
  
  def createDataset(self,dataset):
    res = self.__checkArgumentFormat(dataset)
    if not res['OK']:
      return res
    datasets = res['Value']
    self.__openSession()
    successful = {}
    failed = {}
    for datasetName,lfns in datasets.items():
      res = self.__executeOperation(datasetName,'exists')
      if not res['OK']:
        return res
      elif res['Value']:
        return S_ERROR("LcgFileCatalogClient.createDataset: This dataset already exists.")
      res = self.__createDataset(datasetName,lfns)
      if res['OK']:
        successful[datasetName] = True
      else:
        self.__executeOperation(datasetName,'removeDataset')
        failed[datasetName] = res['Message']
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeDataset(self,dataset):
    res = self.__checkArgumentFormat(dataset)
    if not res['OK']:
      return res
    datasets = res['Value']
    self.__openSession()
    successful = {}
    failed = {}
    for datasetName in datasets.keys():
      res = self.__removeDataset(datasetName)
      if not res['OK']:
        failed[datasetName] = res['Message']
      else:
        successful[datasetName] = True
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeFileFromDataset(self,dataset):
    res = self.__checkArgumentFormat(dataset)
    if not res['OK']:
      return res
    datasets = res['Value']
    self.__openSession()
    successful = {}
    failed = {}
    for datasetName,lfns in datasets.items():
      res = self.__removeFilesFromDataset(datasetName,lfns)
      if not res['OK']:
        failed[datasetName] = res['Message']
      else:
        successful[datasetName] = True
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

###############################################################################################   
###############################################################################################   
###############################################################################################   
###############################################################################################   
###############################################################################################   


  def addFile(self, fileTuple):
    """ A tuple should be supplied to this method which contains:
        (lfn,pfn,size,se,guid)
        A list of tuples may also be supplied.
    """
    if type(fileTuple) == types.TupleType:
      files = [fileTuple]
    elif type(fileTuple) == types.ListType:
      files = fileTuple
    else:
      return S_ERROR('LFCClient.addFile: Must supply a file tuple of list of tuples')
    failed = {}
    successful = {}
    self.__openSession()
    for lfn,pfn,size,se,guid,checksum in files:
      #Check the registration is correctly specified
      res = self.__checkAddFile(lfn,pfn,size,se,guid)
      if not res['OK']:
        failed[lfn] = "LFCClient.addFile: %s" % res['Message']
      else:
        size = long(size)
        res = self.__addFile(lfn,pfn,size,se,guid,checksum)
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          #Finally, register the pfn replica
          replicaTuple = (lfn,pfn,se,True)
          res = self.addReplica(replicaTuple)
          if not res['OK']:
            failed[lfn] = res['Message']
            res = self.removeLfn(lfn)
          elif not lfn in res['Value']['Successful']:
            failed[lfn] = res['Value']['Failed']
            res = self.removeLfn(lfn)
          else:
            successful[lfn] = True
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __addFile(self,lfn,pfn,size,se,guid,checksum):
    self.__startTransaction()
    bdir = os.path.dirname(lfn)
    res = self.exists(bdir)
    # If we failed to find out whether the directory exists
    if not res['OK']:
      self.__abortTransaction()
      return S_ERROR(res['Message'])
    # If we failed to find out whether the directory exists
    if lfn in res['Value']['Failed'].keys():
      self.__abortTransaction()
      return S_ERROR(res['Value']['Failed'][lfn])
    # If the directory doesn't exist
    if not res['Value']['Successful'][bdir]:
      #Make the directories recursively if needed
      res = self.__makeDirs(bdir)
      # If we failed to make the directory for the file
      if not res['OK']:
        self.__abortTransaction()
        return S_ERROR(res['Message'])
    #Create a new file
    fullLfn = '%s%s' % (self.prefix,lfn)
    value = lfc.lfc_creatg(fullLfn,guid,0664)
    if value != 0:
      self.__abortTransaction()
      errStr = lfc.sstrerror(lfc.cvar.serrno)
      # Remove the file we just attempted to add
      res = self.removeFile(lfn)
      return S_ERROR("__addFile: Failed to create GUID: %s" % errStr)
    #Set the size of the file
    if not checksum:
      checksum = ''
    value = lfc.lfc_setfsizeg(guid,size,'AD',checksum)
    if value != 0:
      self.__abortTransaction()
      errStr = lfc.sstrerror(lfc.cvar.serrno)
      # Remove the file we just attempted to add
      res = self.removeFile(lfn)
      return S_ERROR("__addFile: Failed to set file size: %s" % errStr)
    self.__endTransaction()
    return S_OK()

  def __checkAddFile(self,lfn,pfn,size,se,guid):
    errStr = ""
    try:
      size = long(size)
    except:
      errStr += "The size of the file must be an 'int','long' or 'string'"
    if not guid:
      errStr += "There is no GUID, don't be silly"
    res = self.__existsGuid(guid)
    if res['OK'] and res['Value']:
      errStr += "You can't register the same GUID twice"
    if not se:
      errStr += "You really want to register a file without giving the SE?!?!?"
    if not pfn:
      errStr += "Without a PFN a registration is nothing"
    if not lfn:
      errStr += "You really are rubbish!!!! Sort it out"
    res = self.exists(lfn)
    if res['OK'] and res['Value']['Successful'].has_key(lfn):
      if res['Value']['Successful'][lfn]:
        errStr += "This LFN is already taken, try another one"
    if errStr:
      return S_ERROR(errStr)
    else:
      return S_OK()

  def addReplica(self, replicaTuple):
    """ This adds a replica to the catalogue
        The tuple to be supplied is of the following form:
          (lfn,pfn,se,master)
        where master = True or False
    """
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('LFCClient.addReplica: Must supply a replica tuple of list of tuples')
    failed = {}
    successful = {}
    self.__openSession()
    for lfn,pfn,se,master in replicas:
      res = self.__getLFNGuid(lfn)
      if res['OK']:
        guid = res['Value']
        fid = lfc.lfc_fileid()
        status = 'U'
        f_type = 'D'
        poolname = ''
        fs = ''

        value = lfc.lfc_addreplica(guid,fid,se,pfn,status,f_type,poolname,fs)
        """
        if master:
          r_type = 'S' # S = secondary, P = primary
        setname = 'SpaceToken'
        value = lfc.lfc_addreplica(guid,fid,se,pfn,status,f_type,poolname,fs,r_type,setname)
        """
        if value == 0:
          successful[lfn] = True
        else:
          errStr = lfc.sstrerror(lfc.cvar.serrno)
          # This replica already exists, not an error but a duplicate registration - to review !
          if errStr == "File exists":
            successful[lfn] = True
          else:
            failed[lfn] = errStr
      else:
        failed[lfn] = lfc.sstrerror(lfc.cvar.serrno)
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeReplica(self, replicaTuple):
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('LFCClient.removeReplica: Must supply a file tuple or list of file typles')
    self.__openSession()
    failed = {}
    successful = {}
    for lfn,pfn,se in replicas:
      fid = lfc.lfc_fileid()
      value = lfc.lfc_delreplica('',fid,pfn)
      if value == 0:
        successful[lfn] = True
      else:
        errno = lfc.cvar.serrno
        errStr = lfc.sstrerror(errno).lower()
        if (errStr.find("no such file or directory") >= 0 ):
          successful[lfn] = True
        else:
          failed[lfn] = lfc.sstrerror(errno)
    lfnRemoved = successful.keys()
    if len(lfnRemoved) > 0:
      res = self.getReplicas(lfnRemoved,True)
      zeroReplicaFiles = []
      if not res['OK']:
        return res
      else:
        for lfn,repDict in res['Value']['Successful'].items():
          if len(repDict.keys()) == 0:
            zeroReplicaFiles.append(lfn)
      if len(zeroReplicaFiles) > 0:
        res = self.removeFile(zeroReplicaFiles)
        if not res['OK']:
          return res
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def setReplicaStatus(self,replicaTuple):
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('LFCClient.setReplicaStatus: Must supply a file tuple or list of file typles')
    successful = {}
    failed = {}
    # If we have less than three lfns to query a session doesn't make sense
    if len(replicas) > 2:
      self.__openSession()
    for replicaTuple in replicas:
      lfn,pfn,se,status = replicaTuple
      value = lfc.lfc_setrstatus(pfn,status[0])
      if not value == 0:
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
      else:
        successful[lfn] = True
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def setReplicaHost(self,replicaTuple):
    """ This modifies the replica metadata for the SE and space token.
        The tuple supplied must be of the following form:
        (lfn,pfn,oldse,newse)
    """
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('LFCClient.setReplicaHost: Must supply a file tuple or list of file typles')
    successful = {}
    failed = {}
    # If we have less than three lfns to query a session doesn't make sense
    if len(replicas) > 2:
      self.__openSession()
    for lfn,pfn,oldse,newse in replicas:
      value = lfc.lfc_modreplica(pfn,'','',newse)
      if not value == 0:
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
      else:
        successful[lfn] = True
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################

  ####################################################################
  #
  # These are the internal methods to be used by all methods
  #

  def __checkArgumentFormat(self,path):   
    if type(path) in types.StringTypes:
      urls = {path:False}
    elif type(path) == types.ListType:
      urls = {}
      for url in path:
        urls[url] = False
    elif type(path) == types.DictType:
     urls = path
    else:
      return S_ERROR("LcgFileCatalogClient.__checkArgumentFormat: Supplied path is not of the correct format.")
    return S_OK(urls)

  def __executeOperation(self,path,method):
    """ Executes the requested functionality with the supplied path
    """
    execString = "res = self.%s(path)" % method
    try:
      exec(execString)
      if not res['OK']:
        return S_ERROR(res['Message'])
      elif not res['Value']['Successful'].has_key(path):
        return S_ERROR(res['Value']['Failed'][path])
      else:
        return S_OK(res['Value']['Successful'][path])
    except AttributeError,errMessage:
      exceptStr = "LcgFileCatalogClient.__executeOperation: Exception while perfoming %s." % method
      gLogger.exception(exceptStr,'',errMessage)
      return S_ERROR("%s%s" % (exceptStr,errMessage))
  
  def __existsLfn(self,lfn):
    """ Check whether the supplied LFN exists
    """
    fullLfn = '%s%s' % (self.prefix,lfn)
    value = lfc.lfc_access(fullLfn,0)
    if value == 0:
      return S_OK(True)
    else:
      errno = lfc.cvar.serrno
      if errno == 2:
        return S_OK(False)
      else:
        return S_ERROR(lfc.sstrerror(errno))

  def __existsGuid(self,guid):
    """ Check if the guid exists
    """
    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg('',guid,fstat)
    if value == 0:
      return S_OK(True)
    else:
      errno = lfc.cvar.serrno
      if errno == 2:
        return S_OK(False)
      else:
        return S_ERROR(lfc.sstrerror(errno))

  def __getLfnForGUID(self,guid):
    """ Resolve the LFN for a supplied GUID
    """
    list = lfc.lfc_list()
    lfnlist = []
    listlinks = lfc.lfc_listlinks('',guid,lfc.CNS_LIST_BEGIN,list)
    while listlinks:
       ll = listlinks.path
       if re.search ('^'+self.prefix,ll):
          ll = listlinks.path.replace(self.prefix,"",1)
       lfnlist.append(ll)
       listlinks = lfc.lfc_listlinks('',guid,lfc.CNS_LIST_CONTINUE,list)
    else:
       lfc.lfc_listlinks('',guid,lfc.CNS_LIST_END,list)
    return S_OK(lfnlist[0])

  def __getBasePath(self,path):
    exists = False
    while not exists:
      res = self.__executeOperation(path,'exists')
      if not res['OK']:
        return res
      else:
        exists = res['Value']
        if not exists:
          path = os.path.dirname(path)
    return S_OK(path)
    
  def __getACLInformation(self,path):
    fullLfn = '%s%s' % (self.prefix,path)
    results,objects = lfc.lfc_getacl(fullLfn,256)#lfc.CNS_ACL_GROUP_OBJ)
    if results == -1:
      errStr = "LcgFileCatalogClient.__getACLInformation: Failed to obtain all path ACLs."
      gLogger.error(errStr,"%s %s" % (path,lfc.sstrerror(lfc.cvar.serrno)))
      return S_ERROR(errStr)
    permissionsDict = {}
    for object in objects:
      if object.a_type == lfc.CNS_ACL_USER_OBJ:
        res = self.__getDNFromUID(object.a_id)
        if not res['OK']:
          return res
        permissionsDict['DN'] = res['Value']
        permissionsDict['user'] = object.a_perm
      elif object.a_type == lfc.CNS_ACL_GROUP_OBJ:
        res = self.__getRoleFromGID(object.a_id)
        if not res['OK']:
          return res
        role = res['Value']
        permissionsDict['Role'] = role
        permissionsDict['group'] = object.a_perm
      elif object.a_type == lfc.CNS_ACL_OTHER:
        permissionsDict['world'] = object.a_perm
      else:
        errStr = "LcgFileCatalogClient.__getACLInformation: ACL type not considered."
        gLogger.debug(errStr,object.a_type)
    gLogger.verbose("LcgFileCatalogClient.__getACLInformation: %s owned by %s:%s." % (path,permissionsDict['DN'],permissionsDict['Role'])) 
    return S_OK(permissionsDict)

  def __getPathStat(self,path):
    fullLfn = '%s%s' % (self.prefix,path)
    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg(fullLfn,'',fstat)
    if value == 0:
      return S_OK(fstat)
    else:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))  

  def __getDNFromUID(self,userID):
    buffer = ""
    for i in range(0,lfc.CA_MAXNAMELEN+1):
      buffer = buffer+" "
    res = lfc.lfc_getusrbyuid(userID,buffer)
    if res == 0:
      dn = buffer[:buffer.find('\x00')]
      gLogger.debug("LcgFileCatalogClient.__getDNFromUID: UID %s maps to %s." % (userID,dn))
      return S_OK(dn)
    else:
      errStr = "LcgFileCatalogClient.__getDNFromUID: Failed to get DN from UID"
      gLogger.error(errStr,"%s %s" % (userID,lfc.sstrerror(lfc.cvar.serrno)))
      return S_ERROR(errStr)

  def __getRoleFromGID(self,groupID):
    buffer = ""
    for i in range(0,lfc.CA_MAXNAMELEN+1):
      buffer = buffer+" "
    res = lfc.lfc_getgrpbygid(groupID,buffer)
    if res == 0:
      role = buffer[:buffer.find('\x00')]
      if role == 'lhcb':
        role = 'lhcb/Role=user'
      gLogger.debug("LcgFileCatalogClient.__getRoleFromGID: GID %s maps to %s." % (groupID,role))
      return S_OK(role)
    else:
      errStr = "LcgFileCatalogClient:__getRoleFromGID: Failed to get role from GID"
      gLogger.error(errStr,"%s %s" % (groupID,lfc.sstrerror(lfc.cvar.serrno)))
      return S_ERROR()

  def __getFileReplicas(self,lfn,allStatus):
    fullLfn = '%s%s' % (self.prefix,lfn)
    value,replicaObjects = lfc.lfc_getreplica(fullLfn,'','')
    if value != 0:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))
    replicas = {}
    for replica in replicaObjects:
      status = replica.status
      if (status != 'P') or allStatus:
        se = replica.host
        pfn = replica.sfn.strip()  
        replicas[se] = pfn
    return S_OK(replicas)

  def __getFileReplicaStatus(self,lfn,se):
    fullLfn = '%s%s' % (self.prefix,lfn)
    value,replicaObjects = lfc.lfc_getreplica(fullLfn,'','')
    if value != 0:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))
    replicas = {}
    for replica in replicaObjects:
      if se == replica.host:
        return S_OK(replica.status)
    return S_ERROR("No replica at supplied site")
    
  def __unlinkPath(self, lfn):
    fullLfn = '%s%s' % (self.prefix,lfn)
    value = lfc.lfc_unlink(fullLfn)
    if value == 0:
      return S_OK()
    else:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))

  def __removeDirectory(self, path):
    fullLfn = '%s%s' % (self.prefix,path)
    value = lfc.lfc_rmdir(fullLfn)
    if value == 0:
      return S_OK()
    else:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))

  def __makeDirs(self,path):
    """  Black magic contained within....
    """
    dir = os.path.dirname(path)
    res = self.__executeOperation(path,'exists')
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK()
    res = self.__executeOperation(dir,'exists')
    if not res['OK']:
      return res
    if res['Value']:
      res = self.__makeDirectory(path)
    else:
      res = self.__makeDirs(dir)
      res = self.__makeDirectory(path)
    return res

  def __makeDirectory(self, path):
    fullLfn = '%s%s' % (self.prefix,path)
    lfc.lfc_umask(0000)
    value = lfc.lfc_mkdir(fullLfn, 0775)
    if value == 0:
      return S_OK()
    else:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))

  def __openDirectory(self,path):
    lfcPath = "%s%s" % (self.prefix,path)
    value = lfc.lfc_opendirg(lfcPath,'')
    if value:
      return S_OK(value)
    else:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))

  def __closeDirectory(self,oDirectory):
    value = lfc.lfc_closedir(oDirectory)
    if value == 0:
      return S_OK()
    else:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))

  def __getDirectoryContents(self,path):
    """ Returns a dictionary containing all of the contents of a directory.
        This includes the metadata associated to files (replicas, size, guid, status) and the subdirectories found.
    """
    # First check that the directory exists
    res = self.__executeOperation(path,'exists')
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR('LcgFileCatalogClient.__getDirectoryContents: The supplied path does not exist')

    res = self.__getPathStat(path)
    if not res['OK']:
      return res
    nbfiles = res['Value'].nlink
    res = self.__openDirectory(path)
    oDirectory = res['Value']
    subDirs = {}
    links = {}
    files = {}
    for i in  range(nbfiles):
      entry,fileInfo = lfc.lfc_readdirxr(oDirectory,"")
      pathMetadata = {}
      pathMetadata['Permissions'] = S_IMODE(entry.filemode)
      if S_ISDIR(entry.filemode):
        subDir = '%s/%s' % (path,entry.d_name)
        subDirs[subDir] = pathMetadata
      else:
        subPath = '%s/%s' % (path,entry.d_name)
        replicaDict = {}
        if fileInfo:
          for replica in fileInfo:
            replicaDict[replica.host] = {'PFN':replica.sfn,'Status':replica.status}
        pathMetadata['Size'] = entry.filesize   
        pathMetadata['GUID'] = entry.guid
        if S_ISLNK(entry.filemode):
          res = self.__executeOperation(subPath,'readLink')
          if res['OK']:
            pathMetadata['Target'] = res['Value']
          links[subPath] = {}
          links[subPath]['MetaData'] = pathMetadata
          links[subPath]['Replicas'] = replicaDict
        elif S_ISREG(entry.filemode):
          files[subPath] = {}
          files[subPath]['Replicas'] = replicaDict
          files[subPath]['MetaData'] = pathMetadata
    pathDict = {}
    res = self.__closeDirectory(oDirectory)
    pathDict = {'Files': files,'SubDirs':subDirs,'Links':links}
    return S_OK(pathDict)

  def __getDirectorySize(self,path):
    res = self.__executeOperation(path,'exists')
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR('LcgFileCatalogClient.__getDirectorySize: The supplied path does not exist')

    res = self.__getPathStat(path) 
    if not res['OK']:
      return res
    nbfiles = res['Value'].nlink
    res = self.__openDirectory(path)
    oDirectory = res['Value']
    pathDict = {'SubDirs':[],'Files':0,'TotalSize':0,'SiteUsage':{}}
    for i in  range(nbfiles):
      entry,fileInfo = lfc.lfc_readdirxr(oDirectory,"")
      if S_ISDIR(entry.filemode):
        subDir = '%s/%s' % (path,entry.d_name)
        pathDict['SubDirs'].append(subDir)
      else:
        fileSize = entry.filesize
        pathDict['TotalSize'] += fileSize
        pathDict['Files'] += 1
        replicaDict = {}
        for replica in fileInfo:
          if not pathDict['SiteUsage'].has_key(replica.host):
            pathDict['SiteUsage'][replica.host] = {'Files':0,'Size':0}
          pathDict['SiteUsage'][replica.host]['Size'] += fileSize
          pathDict['SiteUsage'][replica.host]['Files'] += 1
    res = self.__closeDirectory(oDirectory)
    return S_OK(pathDict)

  def __getLinkStat(self, link):
    fullLink = '%s%s' % (self.prefix,link)
    lstat = lfc.lfc_filestat()
    value = lfc.lfc_lstat(fullLink,lstat)
    if value == 0:
      return S_OK(lstat)
    else:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno)) 

  def __readLink(self, link):
    fullLink = '%s%s' % (self.prefix,link)
    strBuff = ''
    for i in range(lfc.CA_MAXPATHLEN):
      strBuff+=' '
    chars = lfc.lfc_readlink(fullLink,strBuff,lfc.CA_MAXPATHLEN)
    if chars > 0:
      return S_OK(strBuff[:chars].replace(self.prefix,'').replace('\x00',''))
    else:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))

  def __makeLink(self, source, target):
    fullLink = '%s%s' % (self.prefix,source)
    fullLfn = '%s%s' % (self.prefix,target)
    value = lfc.lfc_symlink(fullLfn,fullLink)
    if value == 0:
      return S_OK()
    else:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))

  def __getLFNGuid(self,lfn):
    """Get the GUID for the given lfn"""
    fstat = lfc.lfc_filestatg()
    fullLfn = '%s%s' % (self.prefix,lfn)
    value = lfc.lfc_statg(fullLfn,'',fstat)
    if value == 0:
      return S_OK(fstat.guid)
    else:
      errStr = lfc.sstrerror(lfc.cvar.serrno)
      return S_ERROR(errStr)

  def __createDataset(self,datasetName,lfns):
    res = self.__makeDirs(datasetName)
    if not res['OK']:
      return res
    links = {}
    for lfn in lfns:
      res = self.__getLFNGuid(lfn)
      if not res['OK']:
        return res
      else:
        link = "%s/%s" % (datasetName, res['Value'])
        links[link] = lfn
    res = self.createLink(links)
    if len(res['Value']['Successful']) == len(links.keys()):
      return S_OK()
    totalError = ""
    for link,error in res['Value']['Failed'].items():
      gLogger.error("LcgFileCatalogClient.__createDataset: Failed to create link for %s." % link, error)
      totalError = "%s\n %s : %s" % (totalError,link,error)
    return S_ERROR(totalError)

  def __removeDataset(self,datasetName):
    res = self.__getDirectoryContents(datasetName)
    if not res['OK']:
      return res
    #links = res['Value']['Links'].keys()
    links = res['Value']['Files'].keys()
    res = self.removeLink(links)
    if not res['OK']:
      return res
    elif len(res['Value']['Failed'].keys()):
      return S_ERROR("Failed to remove all links")
    else:
      res = self.__executeOperation(datasetName,'removeDirectory')
      return res

  def __removeFilesFromDataset(self,datasetName,lfns):
    links = []
    for lfn in lfns:
      res = self.__getLFNGuid(lfn)
      if not res['OK']:
        return res
      guid = res['Value']
      linkPath = "%s/%s" % (datasetName,guid)
      links.append(linkPath)
    res = self.removeLink(links)
    if not res['OK']:
      return res
    if len(res['Value']['Successful']) == len(links):
      return S_OK()
    totalError = ""
    for link,error in res['Value']['Failed'].items():
      gLogger.error("LcgFileCatalogClient.__removeFilesFromDataset: Failed to remove link %s." % link, error)
      totalError = "%s\n %s : %s" % (totalError,link,error)
    return S_ERROR(totalError)

