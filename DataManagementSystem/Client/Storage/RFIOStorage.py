""" This is the RFIO StorageClass """

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.Utilities.File import getSize
from stat import *
import types, re,os

ISOK = True

class RFIOStorage(StorageBase):

  def __init__(self,storageName,protocol,path,host,port,spaceToken,wspath):
    self.isok = ISOK

    self.protocolName = 'RFIO'
    self.name = storageName
    self.protocol = protocol
    self.path = path
    self.host = host
    self.port = port
    self.wspath = wspath
    self.spaceToken = spaceToken
    self.cwd = self.path
    apply(StorageBase.__init__,(self,self.name,self.path))

    self.timeout = 100
    self.long_timeout = 600

  def isOK(self):
    return self.isok

  def exists(self,path):
    """ Check if the given path exists. The 'path' variable can be a string or a list of strings.
    """
    if type(path) == types.StringType:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.exists: Supplied path must be string or list of strings")

    gLogger.info("RFIOStorage.exists: Determining the existance of %s files." % len(urls))
    comm = "nsls"
    for url in urls:
      comm = " %s %s" % (comm,url)
    res = shellCall(self.timeout,comm)
    successful = {}
    failed = {}
    if res['OK']:
      returncode,stdout,stderr = res['Value']
      if returncode in [0,1]:
        for line in stdout.splitlines():
          url = line.strip()
          successful[url] = True
        for line in stderr.splitlines():
          pfn,error = line.split(': ')
          url = pfn.strip()
          successful[url] = False
      else:
        errStr = "RFIOStorage.exists: Completely failed to determine the existance files."
        gLogger.error(errStr,"%s %s" % (self.name,stderr))
        return S_ERROR(errStr)
    else:
      errStr = "RFIOStorage.exists: Completely failed to determine the existance files."
      gLogger.error(errStr,"%s %s" % (self.name,res['Message']))
      return S_ERROR(errStr)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  #############################################################
  #
  # These are the methods for file manipulation
  #

  def isFile(self,path):
    """Check if the given path exists and it is a file
    """
    if type(path) == types.StringType:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.isFile: Supplied path must be string or list of strings")

    gLogger.info("RFIOStorage.isFile: Determining whether %s paths are files." % len(urls))
    res = self.__getPathMetadata(urls)
    if not res['OK']:
      return res
    else:
      failed = res['Value']['Failed']
      successful = {}
      for pfn,pfnDict in res['Value']['Successful'].items():
        if pfnDict['Permissions'][0] == 'd':
          successful[pfn] = False
        else:
          successful[pfn] = True
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __getPathMetadata(self,paths):
    gLogger.info("RFIOStorage.__getPathMetadata: Attempting to get metadata for %s paths." % (len(urls)))
    comm = "nsls -ld"
    for url in urls:
      comm = " %s %s" % (comm,url)
    res = shellCall(self.timeout,comm)
    successful = {}
    failed = {}
    if res['OK']:
      returncode,stdout,stderr = res['Value']
      if returncode in [0,1]:
        for line in stdout.splitlines():
          permissions,subdirs,owner,group,size,month,date,timeYear,pfn = line.split()
          successful[pfn] = {}
          successful[pfn]['Permissions'] = permissions
          successful[pfn]['NbSubDirs'] = subdirs
          successful[pfn]['Owner'] = owner
          successful[pfn]['Group'] = group
          successful[pfn]['Size'] = size
          successful[pfn]['Month'] = month
          successful[pfn]['Date'] = date
          successful[pfn]['Year'] = timeYear
        for line in stderr.splitlines():
          pfn,error = line.split(': ')
          url = pfn.strip()
          failed[url] = error
      else:
        errStr = "RFIOStorage.__getPathMetadata: Completely failed to get path metadata."
        gLogger.error(errStr,"%s %s" % (self.name,stderr))
        return S_ERROR(errStr)
    else:
      errStr = "RFIOStorage.__getPathMetadata: Completely failed to get path metadata."
      gLogger.error(errStr,"%s %s" % (self.name,res['Message']))
      return S_ERROR(errStr)

  def getFile(self,fileTuple):
    """Get a local copy in the current directory of a physical file specified by its path
    """
    if type(fileTuple) == types.TupleType:
      urls = [fileTuple]
    elif type(fileTuple) == types.ListType:
      urls = fileTuple
    else:
      return S_ERROR("RFIOStorage.getFile: Supplied file information must be tuple of list of tuples")
    MIN_BANDWIDTH = 1024*100 # 100 KB/s
    failed = {}
    successful = {}
    for srcUrl,destFile,size in urls:
      timeout = size/MIN_BANDWIDTH + 300
      gLogger.info("RFIOStorage.getFile: Executing transfer of %s to %s" % (srcUrl, destFile))
      comm = "rfcp %s %s" % (srcUrl,destFile)
      res = shellCall(timeout,comm)
      removeFile = True
      if res['OK']:
        returncode,stdout,stderr = res['Value']
        if returncode == 0:
          gLogger.info('RFIOStorage.getFile: Got file from storage, performing post transfer check.')
          localSize = getSize(destFile)
          if localSize == size:
            gLogger.info("RFIOStorage.getFile: Post transfer check successful.")
            successful[srcUrl] = True
          else:
            errStr = "RFIOStorage.getFile: Source and destination file sizes do not match."
            gLogger.error(errStr,srcUrl)
            if os.path.exists(destFile):
              gLogger.info("RFIOStorage.getFile: Removing local file.")
              os.remove(destFile)
            failed[srcUrl] = errStr
        else:
          errStr = "RFIOStorage.getFile: Failed to get local copy of file."
          gLogger.error(errStr,stderr)
          failed[srcUrl] = errStr
      else:
        errorMessage = "RFIOStorage.getFile: Failed to get local copy of file."
        gLogger.error(errorMessage,"%s: %s" % (destFile,res['Message']))
        failed[srcUrl] = errorMessage
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def putFile(self,fileTuple):
    """Put a copy of the local file to the current directory on the physical storage
    """
    if type(fileTuple) == types.TupleType:
      urls = [fileTuple]
    elif type(fileTuple) == types.ListType:
      urls = fileTuple
    else:
      return S_ERROR("SRM2Storage.putFile: Supplied file info must be tuple of list of tuples.")
    MIN_BANDWIDTH = 1024*100 # 100 KB/s
    failed = {}
    successful = {}
    for srcFile,destUrl,size in urls:
      timeout = size/MIN_BANDWIDTH + 300
      gLogger.info("RFIOStorage.putFile: Executing transfer of %s to %s" % (srcFile, destUrl))
      comm = "rfcp %s %s" % (srcFile,destUrl)
      res = shellCall(timeout,comm)
      removeFile = True
      if res['OK']:
        returncode,stdout,stderr = res['Value']
        if returncode == 0:
          gLogger.info('RFIOStorage.putFile: Put file to storage, performing post transfer check.')
          res = self.getFileSize(destUrl)
          if res['OK']:
            if res['Value']['Successful'].has_key(destUrl):
              if res['Value']['Successful'][destUrl] == size:
                gLogger.info("RFIOStorage.putFile: Post transfer check successful.")
                successful[dest_url] = True
                removeFile = False
              else:
                errMessage = "RFIOStorage.putFile: Source and destination file sizes do not match."
                gLogger.error(errMessage,destUrl)
                failed[dest_url] = errMessage
            else:
              errMessage = "RFIOStorage.putFile: Failed to determine remote file size."
              gLogger.error(errMessage,destUrl)
              failed[dest_url] = errMessage
          else:
            errMessage = "RFIOStorage.putFile: Failed to determine remote file size."
            gLogger.error(errMessage,destUrl)
            failed[dest_url] = errMessage
        else:
          errStr = "RFIOStorage.putFile: Failed to put file to remote storage."
          gLogger.error(errStr,stderr)
          failed[destUrl] = errStr
      else:
        errStr = "RFIOStorage.putFile: Failed to put file to remote storage."
        gLogger.error(errStr,res['Message'])
        failed[destUrl] = errStr
      if removeFile:
        # This is because some part of the transfer failed.
        infoStr = "RFIOStorage.putFile: Removing destination url."
        gLogger.info(infoStr)
        res = self.removeFile(destUrl)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeFile(self,path):
    """Remove physically the file specified by its path
    """
    if type(path) == types.StringType:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.removeFile: Supplied path must be string or list of strings")
    gLogger.info("RFIOStorage.removeFile: Attempting to remove %s files." % len(urls))
    successful = {}
    failed = {}
    comm = 'nsrm'
    for url in urls:
      comm = "%s %s" % (comm,url)
    res = shellCall(100,comm)
    if res['OK']:
      returncode,stdout,stderr = res['Value']
      if returncode in [0,1]:
        for pfn in urls:
          successful[pfn] = True
      else:
        errStr = "RFIOStorage.removeFile. Completely failed to remove files."
        gLogger.error(errStr,stderr)
        return S_ERROR(errStr)
    else:
      errStr = "RFIOStorage.removeFile: Completely failed to remove files."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileMetadata(self,path):
    # !!!!!!!!!!!!!!!! THIS NEED TO BE CHANGED TO MEET INTERFACE.
    """  Get metadata associated to the file
    """
    if type(path) == types.StringType:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.getFileMetadata: Supplied path must be string or list of strings")
    gLogger.info("RFIOStorage.getFileMetadata: Obtaining metadata for %s files." % len(urls))
    res = self.__getPathMetadata(urls)
    if not res['OK']:
      return res
    else:
      failed = res['Value']['Failed']
      successful = {}
      for pfn,pfnDict in res['Value']['Successful'].items():
        successful[pfn] = pfnDict
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileSize(self,path):
    """Get the physical size of the given file
    """
    if type(path) == types.StringType:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.getFileSize: Supplied path must be string or list of strings")
    gLogger.info("RFIOStorage.getFileSize: Determining the sizes for  %s files." % len(urls))
    res = self.__getPathMetadata(urls)
    if not res['OK']:
      return res
    else:
      failed = res['Value']['Failed']
      successful = {}
      for pfn,pfnDict in res['Value']['Successful'].items():
        successful[pfn] = pfnDict['Size']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def prestageFile(self,path):
    """ Issue prestage request for file
    """
    if type(path) == types.StringType:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.prestageFile: Supplied path must be string or list of strings")
    comm = "stager_get"
    for url in urls:
      comm = "%s -M %s" % (comm,url)
    res = shellCall(100,comm)
    if res['OK']:
      returncode,stdout,stderr = res['Value']
      print returncode
      print stdout
      print stderr
    else:
      errStr = "RFIOStorage.prestageFile: Completely failed to issue stage requests."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getTransportURL(self,path,protocols=False):
    """ Obtain the TURLs for the supplied path and protocols
    """
    return S_ERROR("Storage.getTransportURL: implement me!")

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  def isDirectory(self,path):
    """Check if the given path exists and it is a directory
    """
    if type(path) == types.StringType:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.isDirectory: Supplied path must be string or list of strings")
    gLogger.info("RFIOStorage.isDirectory: Determining whether %s paths are directories." % len(urls))
    res = self.__getPathMetadata(urls)
    if not res['OK']:
      return res
    else:
      failed = res['Value']['Failed']
      successful = {}
      for pfn,pfnDict in res['Value']['Successful'].items():
        if pfnDict['Permissions'][0] == 'd':
          successful[pfn] = True
        else:
          successful[pfn] = False
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectory(self,path):
    """Get locally a directory from the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR("Storage.getDirectory: implement me!")

  def putDirectory(self,path):
    """Put a local directory to the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR("Storage.putDirectory: implement me!")

  def createDirectory(self,newdir):
    if type(path) == types.StringType:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.createDirectory: Supplied path must be string or list of strings")
    successful = {}
    failed = {}

    gLogger.info("RFIOStorage.createDirectory: Attempting to create %s directories." % len(urls))
    for url in urls:
      strippedUrl = url.rstrip('/')
      res = self.__makeDirs(strippedUrl)
      if res['OK']:
        gLogger.info("RFIOStorage.createDirectory: Successfully created directory on storage: %s" % url)
        successful[url] = True
      else:
        gLogger.error("RFIOStorage.createDirectory: Failed to create directory on storage.", url)
        failed[url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __makeDir(self,path):
    # First create a local file that will be used as a directory place holder in storage name space
    dfile = open("dirac_directory",'w')
    dfile.write("This is a DIRAC system directory")
    dfile.close()
    srcFile = '%s/%s' % (os.getcwd(),'dirac_directory')
    size = getSize(srcFile)
    if size == -1:
      infoStr = "SRM2Storage.createDirectory: Failed to get file size."
      gLogger.error(infoStr,srcFile)
      return S_ERROR(infoStr)

    destFile = '%s/%s' % (path,'dirac_directory')
    directoryTuple = (srcFile,destFile,size)
    res = self.putFile(directoryTuple)
    if os.path.exists(srcFile):
      os.remove(srcFile)
    if not res['OK']:
      return res
    if res['Value']['Successful'].has_key(destFile):
      return S_OK()
    else:
      return S_ERROR(res['Value']['Failed'][destFile])

  def __makeDirs(self,path):
    """  Black magic contained within....
    """
    dir = os.path.dirname(path)
    res = self.isDirectory(path)
    if not res['OK']:
      return res
    if res['OK']:
      if res['Value']['Successful'].has_key(path):
        if res['Value']['Successful'][path]:
          return S_OK()
        else:
          res = self.isDirectory(dir)
          if res['OK']:
            if res['Value']['Successful'].has_key(dir):
              if res['Value']['Successful'][dir]:
                res = self.__makeDir(path)
              else:
                res = self.__makeDirs(dir)
                res = self.__makeDir(path)
    return res

  def removeDirectory(self,path):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
    """
    if type(path) == types.StringType:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.removeDirectory: Supplied path must be string or list of strings")
    gLogger.info("RFIOStorage.removeDirectory: Attempting to remove %s directories." % len(urls))
    successful = {}
    failed = {}
    for url in urls:
      comm = "nsrm -r %s" % url
      res = shellCall(100,comm)
      if res['OK']:
        returncode,stdout,stderr = res['Value']
        if returncode ==  0:
          successful[url] = True
        elif returncode == 1:
          successful[url] = True
        else:
          failed[url] = stderr
      else:
        errStr = "RFIOStorage.removeDirectory: Completely failed to remove directory."
        gLogger.error(errStr, "%s %s" % (url,res['Message']))
        failed[url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def listDirectory(self,path):
    """ List the supplied path
    """
    return S_ERROR("Storage.listDirectory: implement me!")

  def getDirectoryMetadata(self,path):
    """ Get the metadata for the directory
    """
    return S_ERROR("Storage.getDirectoryMetadata: implement me!")

  def getDirectorySize(self,path):
    """ Get the size of the directory on the storage
    """
    return S_ERROR("Storage.getDirectorySize: implement me!")


  #############################################################
  #
  # These are the methods for manipulting the client
  #

  def isOK(self):
    return self.isok

  def changeDirectory(self,newdir):
    """ Change the current directory
    """
    self.cwd = newdir
    return S_OK()

  def getCurrentDirectory(self):
    """ Get the current directory
    """
    return S_OK(self.cwd)

  def getName(self):
    """ The name with which the storage was instantiated
    """
    return S_OK(self.name)

  def getParameters(self):
    """ Get the parameters with which the storage was instantiated
    """
    return S_ERROR("Storage.getParameters: implement me!")

  def getProtocolPfn(self,pfnDict,withPort):
    """ Get the PFN for the protocol with or without the port
    """
    return S_ERROR("Storage.getProtocolPfn: implement me!")

  def getCurrentURL(self,fileName):
    """ Create the full URL for the storage using the configuration, self.cwd and the fileName
    """
    return S_ERROR("Storage.getCurrentURL: implement me!")
