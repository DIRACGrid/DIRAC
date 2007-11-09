""" Class for the LCG File Catalog Client

"""
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.DataManagementSystem.Client.FileCatalogueBase import FileCatalogueBase
from stat import *
import os, re, string, commands, types

try:
  import lfc
except ImportError, x:
  print "Failed to import lfc module !"
  print str(x)

class LcgFileCatalogClient(FileCatalogueBase):

  def __init__(self,infosys=None,host=None):
    self.host = host
    result = gConfig.getOption('/DIRAC/Setup')
    if not result['OK']:
      gLogger.fatal('Failed to get the /DIRAC/Setup')
      return
    setup = result['Value']

    os.environ['LFC_HOST'] = host
    os.environ['LCG_GFAL_INFOSYS'] = infosys

    result = gConfig.getOption('/DIRAC/Site')
    if not result['OK']:
      gLogger.error('Failed to get the /DIRAC/Site')
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

  def changeDirectory(self, path):
    self.cwd = path
    return S_OK()

  def getCurrentDirectory(self):
    return S_OK(self.cwd)

  def getName(self):
    return S_OK(self.name)

  ####################################################################
  #
  # These are the methods for session manipulation
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

  ####################################################################
  #
  # These are the methods for determining whether paths exist
  #

  def exists(self,path):
    """ Check if the path exists
    """
    if type(path) == types.StringType:
      lfns = [path]
    elif type(path) == types.ListType:
      lfns = path
    else:
      return S_ERROR('LFCClient.getFileMetadata: Must supply a path or list of paths')
    resdict = {}
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns:
      fullLfn = '%s%s' % (self.prefix,lfn)
      value = lfc.lfc_access(path_pre,0)
      if value == 0:
        successful[lfn] = True
      else:
       errno = lfc.cvar.serrno
       errStr = lfc.sstrerror(errno).lower()
       if (mess.find("no such file or directory") >= 0 ):
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
    if (value == 0):
       res = S_OK(1)
    else:
       errno = lfc.cvar.serrno
       mess = lfc.sstrerror(errno).lower()
       if (mess.find("no such file or directory") >= 0):
          res = S_OK(0)
       else:
          res = S_ERROR(lfc.sstrerror(errno))
    return res

  ####################################################################
  #
  # These are the methods for link manipulation
  #

  def isLink(self, path):
    if type(link) == types.StringType:
      links = [link]
    elif type(link) == types.ListType:
      links = link
    else:
      return S_ERROR('LFCClient.isLink: Must supply a link list of link')
    failed = {}
    successful = {}
    # If we have less than three lfns to query a session doesn't make sense
    if len(links) > 2:
      self.__openSession()
    for link in links:
      fullLink = '%s%s' % (self.prefix,linkName)
      fstat = lfc.lfc_filestat()
      value = lfc.lfc_lstat(linkName,fstat)
      if value == 0:
        if S_ISLNK(fstat.filemode):
          successful[link] = True
        else:
          successful[link] = False
      else:
        errno = lfc.cvar.serrno
        failed[link] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def createLink(self,linkTuple):
    if type(replicaTuple) == types.TupleType:
      links = [linkTuple]
    elif type(path) == types.ListType:
      links = linkTuple
    else:
      return S_ERROR('LFCClient.addReplica: Must supply a link tuple of list of tuples')
    # If we have less than three lfns to query a session doesn't make sense
    if len(links) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for linkName,lfn in links:
      fullLink = '%s%s' % (self.prefix,linkName)
      fullLfn = '%s%s' % (self.prefix,lfn)
      value = lfn.lfc_symlink(fullLfn,fullLink)
      if value == 0:
        successful[linkName] = True
      else:
        errno = lfc.cvar.serrno
        failed[linkName] = lfc.sstrerror(errno)
    if self.session:
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
      if value == 0:
        successful[link] = a.replace('\x00','').strip().replace(self.prefix,'')
      else:
        errno = lfc.cvar.serrno
        failed[link] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_ERROR('Implement me')


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
    else:
      files = fileTuple
    for fileTuple in files:
      lfn,pfn,size,se,guid = fileTuple
    return S_ERROR('Implement me')

  def addReplica(self, replicaTuple):
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(path) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('LFCClient.addReplica: Must supply a replica tuple of list of tuples')
    for replicaTuple in replicas:
      lfn,pfn,se = replicaTuple
    return S_ERROR('Implement me')

  def removeReplica(self, replicaTuple):
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('LFCClient.setReplicaStatus: Must supply a file tuple or list of file typles')
    resDict = {}
    for replicaTuple in replicas:
      lfn,pfn,se = replicaTuple
    return S_ERROR('Implement me')

  def removeFile(self, path):
    if type(replicaTuple) == types.TupleType:
      lfns = [lfn]
    elif type(path) == types.ListType:
       lfns = lfn
    else:
      return S_ERROR('LFCClient.isFile: Must supply a path or list of paths')
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
    if type(replicaTuple) == types.TupleType:
      lfns = [lfn]
    elif type(path) == types.ListType:
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
      value = lfc.lfc_statg(lfn,'',fstat)
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
      value = lfc.lfc_statg(lfn,'',fstat)
      if value == 0:
        successful[path] = {}
        successful[path]['Size'] = fstat.filesize
        successful[path]['CheckSumType'] = fstat.csumtype
        successful[path]['CheckSumValue'] = fstat.csumvalue
        successful[path]['GUID'] = fstat.guid
        successful[path]['Status'] = fstat.status
      else:
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getReplicas(self,path):
    """ Returns replicas for an LFN or list of LFNs
    """
    if type(path) == types.StringType:
      lfns = [path]
    elif type(path) == types.ListType:
      lfns = path
    else:
      return S_ERROR('LFCClient.getReplicas: Must supply a path or list of paths')
    resdict = {}
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
          se = replica.host
          pfn = replica.sfn.strip()
          successful[lfn][se] = pfn
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resdict)

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
    for replicaTuple in replicas:
      lfn,pfn,se = replicaTuple
      fullLfn = '%s%s' % (self.prefix,path)
      value,replicaObjects = lfc.lfc_getreplica(fullLfn,'','')
      if value == 0:
        for replicaObject in replicaObjects:
          if replicObject.host == se:
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
      value = lfc_setrtype(pfn,status)
      if not value == 0:
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
      else:
        successful[lfn] = value
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileSize(self, path):
    if type(path) == types.StringType:
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
      value = lfc.lfc_statg(lfn,'',fstat)
      if value == 0:
        successful[path] = fstat.filesize
      else:
        errno = lfc.cvar.serrno
        failed[lfn] = lfc.sstrerror(errno)
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
    elif type(paths) == types.ListType:
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
    elif type(paths) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.getDirectoryReplicas: Must supply a path or list of paths')
    resDict ={}
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      resDict[path] = {}
      res = self.__getDirectoryContents(path)
      if res['OK']:
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
    elif type(paths) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.removeDirectory: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(lfns) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      res = self.__getDirectoryContents(path)
      if res['OK']:
        successful[path] = res['Value']['Files'].keys()
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
    elif type(paths) == types.ListType:
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
        successful[path] = {'CreationTime':fstat.ctime,'NumberOfSubDirs':fstat.nlink}
      else:
        errno = lfc.cvar.serrno
        failed[path] = lfc.sstrerror(errno)
    if self.session:
      self.__closeSession()
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectorySize(self, path):
    if type(path) == types.StringType:
      paths = [path]
    elif type(paths) == types.ListType:
      paths = path
    else:
      return S_ERROR('LFCClient.getDirectoryReplicas: Must supply a path or list of paths')
    # If we have less than three lfns to query a session doesn't make sense
    if len(paths) > 2:
      self.__openSession()
    failed = {}
    successful = {}
    for path in paths:
      res = self.__getDirectoryContents(path)
      if res['OK']:
        pathDict = {'Files':0,'TotalSize':0,'SiteUsage':{}}
        files = res['Value']['Files']
        for lfn in files.keys():
          fileSize = files[lfn]['MetaData']['Size']
          pathDict['Files'] += 1
          pathDict['TotalSize'] += fileSize
          repDict = files[lfn]['Replicas']
          for se in repDict.keys():
            if not pathDict['SiteUsage'].has_key(se):
              pathDict['SiteUsage'][se] = 0
            pathDict['SiteUsage'][se] += fileSize
        successful[path] = pathDict
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
    for i in  range(nbfiles):
      entry,fileInfo = lfc.lfc_readdirxr(direc,"")
      if fileInfo:
        lfn = '%s/%s' % (path,entry.d_name)
        resDict[lfn] = {'Replicas':{}}
        for replica in fileInfo:
          resDict[lfn]['Replicas'][rep.host] = {'PFN':rep.sfn,'Status':rep.status}
        resDict[lfn]['MetaData'] = {'Size':entry.filesize,'GUID':entry.guid}
      else:
        subDir = '%s/%s' % (path,entry.d_name)
        subDirs.append(subDir)
    lfc.lfc_closedir(direc)

    pathDict = {}
    pathDict = {'Files': resDict,'SubDirs':subDirs}
    return S_OK(pathDict)

  def __makeDir(self, path):
    fullLfn = '%s%s' % (self.prefix,path)
    value = lfc.lfc_mkdir(dirname, 0775)
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
          if res['Value']['Successful'].has_key(path):
            if res['Value']['Successful'][path]:
              res = self.__makeDir(path)
            else:
              res = self.__makeDirs(dir)
              res = self.__makeDir(path)
    return res
