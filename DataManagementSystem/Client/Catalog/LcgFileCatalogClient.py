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

  def isOK(self):
    return self.valid
  ####################################################################
  #
  # These are the get/set methods for use within the client
  #

  def changeDirectory(self, path):
    self.cwd = path
    return S_OK()

  def getCurrentDirectory(self):
    return S_OK(self.cwd)

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
  # These are the methods for determining whether paths exist
  #

  def exists(self,path):
    """ Check if the path exists
    """
    if type(path) in types.StringTypes:
      lfns = [path]
    elif type(path) == types.ListType:
      lfns = path
    else:
      return S_ERROR('LFCClient.exists: Must supply a path or list of paths')
    resdict = {}
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns:
      fullLfn = '%s%s' % (self.prefix,lfn)
      value = lfc.lfc_access(fullLfn,0)
      if value == 0:
        successful[lfn] = True
      else:
       errno = lfc.cvar.serrno
       errStr = lfc.sstrerror(errno).lower()
       if (errStr.find("no such file or directory") >= 0 ):
          successful[lfn] = False
       else:
         failed[lfn] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __existsGuid(self,guid):
    """ Check if the guid exists
    """
    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg('',guid,fstat)
    if value == 0:
       res = S_OK(1)
    else:
       errno = lfc.cvar.serrno
       errStr = lfc.sstrerror(errno).lower()
       if (errStr.find("no such file or directory") >= 0):
          res = S_OK(0)
       else:
          res = S_ERROR(lfc.sstrerror(errno))
    return res

  ####################################################################
  #
  # These are the methods for link manipulation
  #

  def isLink(self, link):
    if type(link) == types.StringType:
      links = [link]
    elif type(link) == types.ListType:
      links = link
    else:
      return S_ERROR('LFCClient.isLink: Must supply a link list of link')
    failed = {}
    successful = {}
    # If we have less than three lfns to query a session doesn't make sense
    self.__openSession()
    for link in links:
      fullLink = '%s%s' % (self.prefix,link)
      fstat = lfc.lfc_filestat()
      value = lfc.lfc_lstat(fullLink,fstat)
      if value == 0:
        if S_ISLNK(fstat.filemode):
          successful[link] = True
        else:
          successful[link] = False
      else:
        errno = lfc.cvar.serrno
        failed[link] = lfc.sstrerror(errno)
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def createLink(self,linkTuple):
    if type(linkTuple) == types.TupleType:
      links = [linkTuple]
    elif type(linkTuple) == types.ListType:
      links = linkTuple
    else:
      return S_ERROR('LFCClient.createLink: Must supply a link tuple of list of tuples')
    # If we have less than three lfns to query a session doesn't make sense
    self.__openSession()
    failed = {}
    successful = {}
    for link,lfn in links:
      fullLink = '%s%s' % (self.prefix,link)
      fullLfn = '%s%s' % (self.prefix,lfn)
      value = lfc.lfc_symlink(fullLfn,fullLink)
      if value == 0:
        successful[lfn] = True
      else:
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeLink(self,link):
    if type(link) == types.StringType:
      links = [link]
    elif type(link) == types.ListType:
      links = link
    else:
      return S_ERROR('LFCClient.removeLink: Must supply a link list of link')
    # If we have less than three lfns to query a session doesn't make sense
    if len(links) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for link in links:
      fullLink = '%s%s' % (self.prefix,link)
      value = lfc.lfc_unlink(fullLink)
      if value == 0:
        successful[link] = True
      else:
        errno = lfc.cvar.serrno
        failed[link] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def readLink(self,link):
    if type(link) == types.StringType:
      links = [link]
    elif type(link) == types.ListType:
      links = link
    else:
      return S_ERROR('LFCClient.removeLink: Must supply a link list of link')
    # If we have less than three lfns to query a session doesn't make sense
    if len(links) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for link in links:
      fullLink = '%s%s' % (self.prefix,link)
      """  The next six lines of code should be hung, drawn and quartered
      """
      strBuff = ''
      for i in range(256):
        strBuff+=' '
      value = lfc.lfc_readlink(fullLink,strBuff,256)
      if re.search('/',strBuff):
        successful[link] = strBuff.replace('\x00','').strip().replace(self.prefix,'')
      else:
        errno = lfc.cvar.serrno
        failed[link] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ####################################################################
  #
  # These are the methods for file manipulation
  #

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

  def removeFile(self, path):
    if type(path) == types.StringType:
      lfns = [path]
    elif type(path) == types.ListType:
       lfns = path
    else:
      return S_ERROR('LFCClient.removeFile: Must supply a path or list of paths')
    failed = {}
    successful = {}
    res = self.exists(lfns)
    if not res['OK']:
      return res
    lfnsToRemove = res['Value']['Successful'].keys()
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfnsToRemove) > 2:
      self.__openSession()
    for lfn in lfnsToRemove:
      # If the files exist
      if res['Value']['Successful'][lfn]:
        fullLfn = '%s%s' % (self.prefix,lfn)
        value = lfc.lfc_unlink(fullLfn)
        if value == 0:
          successful[lfn] = True
        else:
          failed[lfn] = lfc.sstrerror(lfc.cvar.serrno)
      # If they don't exist the removal can be considered successful
      else:
        successful[lfn] = True
    for lfns in res['Value']['Failed'].keys():
      failed[lfn] = res['Value']['Failed'][lfn]
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def isFile(self, lfn):
    if type(lfn) == types.StringType:
      lfns = [lfn]
    elif type(lfn) == types.ListType:
       lfns = lfn
    else:
      return S_ERROR('LFCClient.isFile: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns:
      fullLfn = '%s%s' % (self.prefix,lfn)
      fstat = lfc.lfc_filestatg()
      value = lfc.lfc_statg(fullLfn,'',fstat)
      if value == 0:
        if S_ISREG(fstat.filemode):
          successful[lfn] = True
        else:
          successful[lfn] = False
      else:
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileMetadata(self, path):
    """ Returns replicas for an LFN or list of LFNs
    """
    if type(path) == types.StringType:
      lfns = [path]
    elif type(path) == types.ListType:
      lfns = path
    else:
      return S_ERROR('LFCClient.getFileMetadata: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns:
      fullLfn = '%s%s' % (self.prefix,lfn)
      fstat = lfc.lfc_filestatg()
      value = lfc.lfc_statg(fullLfn,'',fstat)
      if value == 0:
        successful[lfn] = {}
        successful[lfn]['Size'] = fstat.filesize
        successful[lfn]['CheckSumType'] = fstat.csumtype
        successful[lfn]['CheckSumValue'] = fstat.csumvalue
        successful[lfn]['GUID'] = fstat.guid
        successful[lfn]['Status'] = fstat.status
      else:
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getReplicas(self,path,allStatus=False):
    """ Returns replicas for an LFN or list of LFNs
    """
    if type(path) in types.StringTypes:
      lfns = [path]
    elif type(path) == types.ListType:
      lfns = path
    else:
      return S_ERROR('LFCClient.getReplicas: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns:
      fullLfn = '%s%s' % (self.prefix,lfn)
      value,replicaObjects = lfc.lfc_getreplica(fullLfn,'','')
      if not (value == 0):
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
      else:
        successful[lfn] = {}
        for replica in replicaObjects:
          status = replica.status
          if not (status == 'P') or allStatus:
            se = replica.host
            pfn = replica.sfn.strip()
            successful[lfn][se] = pfn
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getReplicaStatus(self,replicaTuple):
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('LFCClient.getReplicaStatus: Must supply a file tuple or list of file typles')
    failed = {}
    successful = {}
    if len(replicas) > 2:
      self.__openSession()
    for lfn,pfn,se in replicas:
      fullLfn = '%s%s' % (self.prefix,lfn)
      value,replicaObjects = lfc.lfc_getreplica(fullLfn,'','')
      if value == 0:
        for replicaObject in replicaObjects:
          if (replicaObject.sfn == pfn) and (replicaObject.host == se):
            successful[lfn] = replicaObject.status
      else:
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
    if self.session:
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

  def getFileSize(self, path):
    if type(path) in types.StringTypes:
      paths = [path]
    elif type(path) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.getFileSize: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(paths) > 2:
      self.__openSession()
    successful = {}
    failed = {}
    for path in paths:
      fullLfn = '%s%s' % (self.prefix,path)
      fstat = lfc.lfc_filestatg()
      value = lfc.lfc_statg(fullLfn,'',fstat)
      if value == 0:
        successful[path] = fstat.filesize
      else:
        errno = lfc.cvar.serrno
        failed[path] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ####################################################################
  #
  # These are the methods for directory manipulation
  #

  def createDirectory(self,path):
    if type(path) == types.StringType:
      paths = [path]
    elif type(paths) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.createDirectory: Must supply a path or list of paths')
    self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      res = self.__makeDirs(path)
      if res['OK']:
        successful[path] = True
      else:
        failed[path] = res['Message']
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def isDirectory(self, path):
    if type(path) == types.StringType:
      paths = [path]
    elif type(path) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.isDirectory: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(paths) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      fullLfn = '%s%s' % (self.prefix,path)
      fstat = lfc.lfc_filestatg()
      value = lfc.lfc_statg(fullLfn,'',fstat)
      if value == 0:
        if S_ISDIR(fstat.filemode):
          successful[path] = True
        else:
          successful[path] = False
      else:
        errno = lfc.cvar.serrno
        failed[path] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectoryReplicas(self, path):
    """ This method gets all of the pfns in the directory
    """
    if type(path) == types.StringType:
      paths = [path]
    elif type(path) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.getDirectoryReplicas: Must supply a path or list of paths')
    resDict ={}
    # If we have less than three lfns to query a session doesn't make sense
    if len(paths) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      resDict[path] = {}
      res = self.__getDirectoryContents(path)
      if res['OK']:
        successful[path] = {}
        files = res['Value']['Files']
        for lfn in files.keys():
          repDict = files[lfn]['Replicas']
          successful[path][lfn] = {}
          for se in repDict.keys():
            successful[path][lfn][se] = repDict[se]['PFN']
      else:
        failed[path] = res['Message']
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def listDirectory(self, path):
    if type(path) == types.StringType:
      paths = [path]
    elif type(path) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.listDirectory: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(paths) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      res = self.__getDirectoryContents(path)
      if res['OK']:
        successful[path]['Files'] = res['Value']['Files'].keys()
        successful[path]['SubDirs'] = res['Value']['SubDirs']
      else:
        failed[path] = res['Message']
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeDirectory(self, path):
    if type(path) == types.StringType:
      paths = [path]
    elif type(paths) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.removeDirectory: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(paths) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      fullLfn = '%s%s' % (self.prefix,path)
      value = lfc.lfc_rmdir(fullLfn)
      if value == 0:
        successful[path] = True
      else:
        errno = lfc.cvar.serrno
        errStr = lfc.sstrerror(errno).lower()
        if (errStr.find("no such file or directory") >= 0):
          successful[path] = True
        else:
          failed[path] = errStr
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectoryMetadata(self, path):
    if type(path) == types.StringType:
      paths = [path]
    elif type(path) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.getDirectoryReplicas: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(paths) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      fullLfn = '%s%s' % (self.prefix,path)
      fstat = lfc.lfc_filestatg()
      value = lfc.lfc_statg(fullLfn,'',fstat)
      if value == 0:
        res = self.__getDNFromUID(fstat.uid)
        if res['OK']:
          successful[path] = {'CreationTime':time.ctime(fstat.ctime),'NumberOfSubPaths':fstat.nlink,'Status':fstat.status,'CreatorDN':res['Value']}
        else:
          successful[path] = {'CreationTime':time.ctime(fstat.ctime),'NumberOfSubPaths':fstat.nlink,'Status':fstat.status,'CreatorDN':None}
      else:
        errno = lfc.cvar.serrno
        failed[path] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __getDNFromUID(self,userID):
    buffer = ""
    for i in range(0,lfc.CA_MAXNAMELEN+1):
      buffer = buffer+" "
    res = lfc.lfc_getusrbyuid(userID,buffer)
    if res == 0:
      dn = buffer[:buffer.find('\x00')]
      return S_OK(dn)
    else:
      return S_ERROR()

  def getDirectorySize(self, path):
    if type(path) == types.StringType:
      paths = [path]
    elif type(path) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.getDirectorySize: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(paths) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      res = self.__getDirectorySize(path)
      if res['OK']:
        successful[path] = res['Value']
      else:
        failed[path] = res['Message']
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __getDirectorySize(self,path):
    res = self.exists(path)
    if not res['OK']:
      return res
    if not res['Value']['Successful'].has_key(path):
      return S_ERROR('__getDirectoryContents: There was an error accessing the supplied path')
    if not res['Value']['Successful'][path]:
      return S_ERROR('__getDirectoryContents: There supplied path does not exist')

    lfcPath = self.prefix+path
    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg(lfcPath,'',fstat)
    if not value == 0:
      return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))
    nbfiles = fstat.nlink
    direc = lfc.lfc_opendirg(lfcPath,'')

    pathDict = {'SubDirs':[],'Files':0,'TotalSize':0,'SiteUsage':{}}
    for i in  range(nbfiles):
      entry,fileInfo = lfc.lfc_readdirxr(direc,"")
      if S_ISDIR(entry.filemode):
        subDir = '%s/%s' % (path,entry.d_name)
        pathDict['SubDirs'].append(subDir)
      else:
        replicaDict = {}
        if entry:
          fileSize = entry.filesize
          pathDict['TotalSize'] += fileSize
          pathDict['Files'] += 1
          if fileInfo:
            for replica in fileInfo:
              if not pathDict['SiteUsage'].has_key(replica.host):
                pathDict['SiteUsage'][replica.host] = {'Files':0,'Size':0}
              pathDict['SiteUsage'][replica.host]['Size'] += fileSize
              pathDict['SiteUsage'][replica.host]['Files'] += 1
    lfc.lfc_closedir(direc)
    return S_OK(pathDict)

  def getDirectoryContents(self,path):
    """ Returns the result of __getDirectoryContents for multiple supplied paths
    """
    if type(path) == types.StringType:
      paths = [path]
    elif type(path) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.getDirectoryContents: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(paths) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      res = self.__getDirectoryContents(path)
      if res['OK']:
        successful[path] = res['Value']
      else:
        failed[path] = res['Message']
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __getDirectoryContents(self,path):
    """ Returns a dictionary containing all of the contents of a directory.
        This includes the metadata associated to files (replicas, size, guid, status) and the subdirectories found.
    """
    # First check that the directory exists, this
    res = self.exists(path)
    if not res['OK']:
      return res
    if not res['Value']['Successful'].has_key(path):
      return S_ERROR('__getDirectoryContents: There was an error accessing the supplied path')
    if not res['Value']['Successful'][path]:
      return S_ERROR('__getDirectoryContents: There supplied path does not exist')

    lfcPath = self.prefix+path
    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg(lfcPath,'',fstat)
    nbfiles = fstat.nlink
    direc = lfc.lfc_opendirg(lfcPath,'')

    resDict = {}
    subDirs = []
    links = {}
    for i in  range(nbfiles):
      entry,fileInfo = lfc.lfc_readdirxr(direc,"")
      if S_ISDIR(entry.filemode):
        subDir = '%s/%s' % (path,entry.d_name)
        subDirs.append(subDir)
      else:
        subPath = '%s/%s' % (path,entry.d_name)
        replicaDict = {}
        if fileInfo:
          for replica in fileInfo:
            replicaDict[replica.host] = {'PFN':replica.sfn,'Status':replica.status}
        metadataDict = {'Size':entry.filesize,'GUID':entry.guid}
        if S_ISLNK(entry.filemode):
          links[subPath]['Replicas'] = replicaDict
          links[subPath]['MetaData'] = metadataDict
          links[subPath]['MetaData']['Target'] = ''
          res = self.readLink(subPath)
          if subPath in res['Value']['Successful'].keys():
            links[subPath]['MetaData']['Target'] = res['Value']['Successful'][subPath]
        elif S_ISREG(entry.filemode):
          resDict[subPath] = {}
          resDict[subPath]['Replicas'] = replicaDict
          resDict[subPath]['MetaData'] = metadataDict
    lfc.lfc_closedir(direc)
    pathDict = {}
    pathDict = {'Files': resDict,'SubDirs':subDirs,'Links':links}
    return S_OK(pathDict)

  def __makeDir(self, path):
    fullLfn = '%s%s' % (self.prefix,path)
    value = lfc.lfc_mkdir(fullLfn, 0775)
    if value == 0:
      return S_OK()
    else:
      errStr = lfc.sstrerror(lfc.cvar.serrno)
      return S_ERROR(errStr)

  def __makeDirs(self,path):
    """  Black magic contained within....
    """
    dir = os.path.dirname(path)
    res = self.exists(path)
    if not res['OK']:
      return res
    if res['OK']:
      if res['Value']['Successful'].has_key(path):
        if res['Value']['Successful'][path]:
          return S_OK()
        else:
          res = self.exists(dir)
          if res['Value']['Successful'].has_key(dir):
            if res['Value']['Successful'][dir]:
              res = self.__makeDir(path)
            else:
              res = self.__makeDirs(dir)
              res = self.__makeDir(path)
    return res

  ####################################################################
  #
  # These are the methods for dataset manipulation
  #

  def deleteDataset(self,datasetDirectory):
    res = self.__getDirectoryContents(datasetDirectory)
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
      result = self.removeDirectory(datasetDirectory)
      return result

  def resolveDataset(self,datasetDirectory):
    res = self.__getDirectoryContents(datasetDirectory)
    if not res['OK']:
      return res
    linkDict = res['Value']['Links']
    replicas = {}
    for link in linkDict.keys():
      target = linkDict[link]['MetaData']['Target']
      replicas[target] = inkDict[link]['Replicas']
    return S_OK(replicas)

  def removeFileFromDataset(self,datasetDirectory,lfn):
    if type(lfn) == types.StringType:
      lfns = [lfn]
    elif type(lfn) == types.ListType:
      lfns = lfn
    else:
      return S_ERROR('LFCClient.removeFileFromDataset: Must supply a LFN of list of LFNs')
    failed = {}
    successful = {}
    self.__openSession()
    for lfn in lfns:
      res = self.__getLFNGuid(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        guid = res['Value']
        linkPath = "%s/%s" % (datasetDirectory,guid)
        res = self.removeLink(linkPath)
        if not res['OK']:
          failed[lfn] = res['Message']
        elif lfn in res['Value']['Failed'].keys():
          failed[lfn] = res['Value']['Failed'][lfn]
        else:
          successful[lfn] = True
    self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def createDataset(self,datasetDirectory,lfn):
    if type(lfn) == types.StringType:
      lfns = [lfn]
    elif type(lfn) == types.ListType:
      lfns = lfn
    else:
      return S_ERROR('LFCClient.createDataset: Must supply a LFN of list of LFNs')

    if not self.session:
      self.__openSession()
    res = self.exists(datasetDirectory)
    if not res['OK']:
      return res
    elif datasetDirectory in res['Value']['Failed'].keys():
      return S_ERROR(res['Value']['Failed'][datasetDirectory])
    elif res['Value']['Successful'][datasetDirectory]:
      return S_ERROR("createDataset: This dataset already exists.")
    else:
      res = self.__makeDirs(datasetDirectory)

    linkTuples = []
    successful = {}
    failed = {}
    for lfn in lfns:
      res = self.__getLFNGuid(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        guid = res['Value']
        link = "%s/%s" % (datasetDirectory,guid)
        linkTuples.append((link,lfn))
    if linkTuples:
      res = self.createLink(linkTuples)
      if not res['OK']:
        return res
      successful.update(res['Value']['Successful'])
      failed.update(res['Value']['Failed'])
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

