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
    if type(path) in types.StringTypes:
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
    if type(path) in types.StringTypes:
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

  def __getPathMetadata(self,urls):
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
          successful[pfn]['Size'] = int(size)
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
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

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
      return S_ERROR("RFIOStorage.putFile: Supplied file info must be tuple of list of tuples.")
    MIN_BANDWIDTH = 1024*100 # 100 KB/s
    failed = {}
    successful = {}
    for srcFile,destUrl,size in urls:
      timeout = size/MIN_BANDWIDTH + 300
      res = self.getTransportURL(destUrl)
      if res['OK']:
        if res['Value']['Successful'].has_key(destUrl):
          turl = res['Value']['Successful'][destUrl]
          gLogger.info("RFIOStorage.putFile: Executing transfer of %s to %s" % (srcFile, destUrl))
          comm = "rfcp %s '%s'" % (srcFile,turl)
          res = shellCall(timeout,comm)
      removeFile = True
      if res['OK']:
        returncode,stdout,stderr = res['Value']
        if returncode == 0:
          gLogger.info('RFIOStorage.putFile: Put file to storage, performing post transfer check.')
          res = self.getFileSize(destUrl)
          if res['OK']:
            if res['Value']['Successful'].has_key(destUrl):
              if int(res['Value']['Successful'][destUrl]) == int(size):
                gLogger.info("RFIOStorage.putFile: Post transfer check successful.")
                successful[destUrl] = True
                removeFile = False
              else:
                errMessage = "RFIOStorage.putFile: Source and destination file sizes do not match."
                gLogger.error(errMessage,destUrl)
                failed[destUrl] = errMessage
            else:
              errMessage = "RFIOStorage.putFile: Failed to determine remote file size."
              gLogger.error(errMessage,destUrl)
              failed[destUrl] = errMessage
          else:
            errMessage = "RFIOStorage.putFile: Failed to determine remote file size."
            gLogger.error(errMessage,destUrl)
            failed[destUrl] = errMessage
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
    if type(path) in types.StringTypes:
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
    """  Get metadata associated to the file
    """
    if type(path) in types.StringTypes:
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
    if type(path) in types.StringTypes:
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
      failed = {}
      for pfn,err in res['Value']['Failed'].items():
        failed[pfn] = "RFIOStorage.getFileMetadata: %s." % err 
      successful = {}
      for pfn,pfnDict in res['Value']['Successful'].items():
        successful[pfn] = int(pfnDict['Size'])
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def prestageFile(self,path):
    """ Issue prestage request for file
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.prestageFile: Supplied path must be string or list of strings")
    comm = "stager_get -S %s" % self.spaceToken
    for url in urls:
      comm = "%s -M %s" % (comm,url)
    res = shellCall(100,comm)
    successful = {}
    failed = {}
    if res['OK']:
      returncode,stdout,stderr = res['Value']
      if stderr:
        errStr = "RFIOStorage.prestageFile: Comepletely failed to issue stage requests."
        gLogger.error(errStr,stderr)
        return S_ERROR(errStr)
      else:
        for line in stdout.splitlines():
          if re.search('SUBREQUEST_READY',line):
            pfn,status = line.split()
            successful[pfn] = True
          elif re.search('SUBREQUEST_FAILED',line):
            pfn,status,err = line.split(' ',2)
            failed[pfn] = err
        if not successful:
          errStr = "RFIOStorage.prestageFile: Completely failed to issue stage requests."
          gLogger.error(errStr,err)
          return S_ERROR(errStr)
    else:
      errStr = "RFIOStorage.prestageFile: Completely failed to issue stage requests."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getTransportURL(self,path,protocols=False):
    """ Obtain the TURLs for the supplied path and protocols
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.getTransportURL: Supplied path must be string or list of strings")
    successful = {}
    failed = {}
    for path in urls:
      try:
        if self.spaceToken:
          tURL = "%s://%s:%s/?svcClass=%s&castorVersion=2&path=%s" % (self.protocol,self.host,self.port,self.spaceToken,path)
        else:
          tURL = "castor:%s" % (path)
        successful[path] = tURL
      except Exception,x:
        errStr = "RFIOStorage.getTransportURL: Failed to create tURL for path."
        gLogger.error(errStr,"% %s" % (self.name,x))
        failed[path] = errStr
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  def isDirectory(self,path):
    """Check if the given path exists and it is a directory
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.isDirectory: Supplied path must be string or list of strings")
    gLogger.info("RFIOStorage.isDirectory: Determining whether %s paths are directories." % len(urls))
    files = {}
    for url in urls:
      destFile = '%s/dirac_directory' % url
      files[destFile] = url
    res = self.__getPathMetadata(files.keys())
    if not res['OK']:
      return res
    else:
      successful = {}
      failed = {}
      for pfn,errorMessage in res['Value']['Failed'].items():
        if errorMessage == 'No such file or directory':
          successful[files[pfn]] = False
        else:
          failed[files[pfn]] = errorMessage
      for pfn,pfnDict in res['Value']['Successful'].items():
        successful[files[pfn]] = True
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectory(self,directoryTuple):
    """ Get locally a directory from the physical storage together with all its files and subdirectories.
    """
    if type(directoryTuple) == types.TupleType:
      urls = [directoryTuple]
    elif type(directoryTuple) == types.ListType:
      urls = directoryTuple
    else:
      return S_ERROR("RFIOStorage.getDirectory: Supplied directory info must be tuple of list of tuples.")
    successful = {}
    failed = {}
    gLogger.info("RFIOStorage.getDirectory: Attempting to get local copies of %s directories." % len(urls))

    for src_directory,destination_directory in urls:
      res = self.__getDir(src_directory,destination_directory)
      if res['OK']:
        if res['Value']['AllGot']:
          gLogger.info("RFIOStorage.getDirectory: Successfully got local copy of %s" % src_directory)
          successful[src_directory] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
        else:
          gLogger.error("RFIOStorage.getDirectory: Failed to get entire directory.", src_directory)
          failed[src_directory] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
      else:
        gLogger.error("RFIOStorage.getDirectory: Completely failed to get local copy of directory.", src_directory)
        failed[src_directory] = {'Files':0,'Size':0}
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __getDir(self,srcDirectory,destDirectory):
    """ Black magic contained within...
    """
    filesGot = 0
    sizeGot = 0

    # Check the remote directory exists
    res = self.isDirectory(srcDirectory)
    if not res['OK']:
      errStr = "RFIOStorage.__getDir: Failed to find the supplied source directory."
      gLogger.error(errStr,srcDirectory)
      return S_ERROR(errStr)
    if not res['Value']['Successful'].has_key(srcDirectory):
      errStr = "RFIOStorage.__getDir: Failed to find the supplied source directory."
      gLogger.error(errStr,srcDirectory)
      return S_ERROR(errStr)
    if not res['Value']['Successful'][srcDirectory]:
      errStr = "RFIOStorage.__getDir: The supplied source directory does not exist."
      gLogger.error(errStr,srcDirectory)
      return S_ERROR(errStr)

    # Check the local directory exists and create it if not
    if not os.path.exists(destDirectory):
      os.makedirs(destDirectory)

    # Get the remote directory contents
    res = self.listDirectory(srcDirectory)
    if not res['OK']:
      errStr = "RFIOStorage.__getDir: Failed to list the source directory."
      gLogger.error(errStr,srcDirectory)
    if not res['Value']['Successful'].has_key(srcDirectory):
      errStr = "RFIOStorage.__getDir: Failed to list the source directory."
      gLogger.error(errStr,srcDirectory)

    surlsDict = res['Value']['Successful'][srcDirectory]['Files']
    subDirsDict = res['Value']['Successful'][srcDirectory]['SubDirs']

    # First get all the files in the directory
    gotFiles = True
    for surl in surlsDict.keys():
      surlGot = False
      fileSize = surlsDict[surl]['Size']
      fileName = os.path.basename(surl)
      localPath = '%s/%s' % (destDirectory,fileName)
      fileTuple = (surl,localPath,fileSize)
      res = self.getFile(fileTuple)
      if res['OK']:
        if res['Value']['Successful'].has_key(surl):
          filesGot += 1
          sizeGot += fileSize
          surlGot = True
      if not surlGot:
        gotFiles = False

    # Then recursively get the sub directories
    subDirsGot = True
    for subDir in subDirsDict.keys():
      subDirName = os.path.basename(subDir)
      localPath = '%s/%s' % (destDirectory,subDirName)
      dirSuccessful = False
      res = self.__getDir(subDir,localPath)
      if res['OK']:
        if res['Value']['AllGot']:
          dirSuccessful = True
        filesGot += res['Value']['Files']
        sizeGot += res['Value']['Size']
      if not dirSuccessful:
        subDirsGot = False

    # Check whether all the operations were successful
    if subDirsGot and gotFiles:
      allGot = True
    else:
      allGot = False
    resDict = {'AllGot':allGot,'Files':filesGot,'Size':sizeGot}
    return S_OK(resDict)

  def putDirectory(self, directoryTuple):
    """ Put a local directory to the physical storage together with all its files and subdirectories.
    """
    if type(directoryTuple) == types.TupleType:
      urls = [directoryTuple]
    elif type(directoryTuple) == types.ListType:
      urls = directoryTuple
    else:
      return S_ERROR("RFIOStorage.putDirectory: Supplied directory info must be tuple of list of tuples.")
    successful = {}
    failed = {}
    gLogger.info("RFIOStorage.putDirectory: Attemping to put %s directories to remote storage." % len(urls))
    for sourceDir,destDir in urls:
      res = self.__putDir(sourceDir,destDir)
      if res['OK']:
        if res['Value']['AllPut']:
          gLogger.info("RFIOStorage.putDirectory: Successfully put directory to remote storage: %s" % destDir)
          successful[destDir] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
        else:
          gLogger.error("RFIOStorage.putDirectory: Failed to put entire directory to remote storage.", destDir)
          failed[destDir] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
      else:
        gLogger.error("RFIOStorage.putDirectory: Completely failed to put directory to remote storage.", destDir)
        failed[destDir] = {'Files':0,'Size':0}
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __putDir(self,src_directory,dest_directory):
    """ Black magic contained within...
    """
    filesPut = 0
    sizePut = 0

    remote_cwd = dest_directory
    # Check the local directory exists
    if not os.path.isdir(src_directory):
      errStr = "RFIOStorage.__putDir: The supplied directory does not exist."
      gLogger.error(errStr,src_directory)
      return S_ERROR(errStr)

    # Create the remote directory
    res = self.createDirectory(dest_directory)
    if not res['OK']:
      errStr = "RFIOStorage.__putDir: Failed to create destination directory."
      gLogger.error(errStr,dest_directory)
      return S_ERROR(errStr)

    # Get the local directory contents
    contents = os.listdir(src_directory)
    allSuccessful = True
    for file in contents:
      pathSuccessful = False
      localPath = '%s/%s' % (src_directory,file)
      remotePath = '%s/%s' % (dest_directory,file)
      if os.path.isdir(localPath):
        res = self.__putDir(localPath,remoteDirectory)
        if res['OK']:
          if res['Value']['AllPut']:
            pathSuccessful = True
          filesPut += res['Value']['Files']
          sizePut += res['Value']['Size']
        else:
          return S_ERROR('Failed to put directory')
      else:
        localFileSize = getSize(localPath)
        fileTuple = (localPath,remotePath,localFileSize)
        res = self.putFile(fileTuple)
        if res['OK']:
          if res['Value']['Successful'].has_key(remotePath):
            filesPut += 1
            sizePut += localFileSize
            pathSuccessful = True
      if not pathSuccessful:
        allSuccessful = False
    resDict = {'AllPut':allSuccessful,'Files':filesPut,'Size':sizePut}
    return S_OK(resDict)

  def createDirectory(self,path):
    if type(path) in types.StringTypes:
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
      infoStr = "RFIOStorage.createDirectory: Failed to get file size."
      gLogger.error(infoStr,srcFile)
      return S_ERROR(infoStr)

    comm = "nsmkdir -m 775 %s" % path
    res = shellCall(100,comm)
    if res['OK']:
      returncode,stdout,stderr = res['Value']
      if not returncode in [0,1]:
        return S_ERROR(stderr)

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
    if type(path) in types.StringTypes:
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
    """ List the supplied path. First checks whether the path is a directory then gets the contents.
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.listDirectory: Supplied path must be string or list of strings")
    gLogger.info("RFIOStorage.listDirectory: Attempting to list %s directories." % len(urls))
    res = self.isDirectory(urls)
    if not res['OK']:
      return res
    successful = {}
    failed = res['Value']['Failed']
    directories = []
    for url,isDirectory in res['Value']['Successful'].items():
      if isDirectory:
        directories.append(url)
      else:
        errStr = "RFIOStorage.listDirectory: Directory does not exist."
        gLogger.error(errStr, url)
        failed[url] = errStr

    for directory in directories:
      comm = "nsls -l %s" % directory
      res = shellCall(self.timeout,comm)
      if res['OK']:
        returncode,stdout,stderr = res['Value']
        if not returncode == 0:
          errStr = "RFIOStorage.listDirectory: Failed to list directory."
          gLogger.error(errStr,"%s %s" % (directory,stderr))
          failed[directory] = errStr
        else:
          subDirs = {}
          files = {}
          successful[directory] = {}
          for line in stdout.splitlines():
            permissions,subdirs,owner,group,size,month,date,timeYear,pfn = line.split()
            if not pfn == 'dirac_directory':
              path = "%s/%s" % (directory,pfn)
              if permissions[0] == 'd':
                # If the subpath is a directory
                subDirs[path] = True
              elif permissions[0] == 'm':
                # In the case that the path is a migrated file
                files[path] = {'Size':size,'Migrated':1}
              else:
                # In the case that the path is not migrated file
                files[path] = {'Size':size,'Migrated':0}
          successful[directory]['SubDirs'] = subDirs
          successful[directory]['Files'] = files
      else:
        errStr = "RFIOStorage.listDirectory: Completely failed to list directory."
        gLogger.error(errStr,"%s %s" % (directory,res['Message']))
        return S_ERROR(errStr)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectoryMetadata(self,path):
    """ Get the metadata for the directory
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.getDirectoryMetadata: Supplied path must be string or list of strings")
    gLogger.info("RFIOStorage.getDirectoryMetadata: Attempting to get metadata for %s directories." % len(urls))
    res = self.isDirectory(urls)
    if not res['OK']:
      return res
    successful = {}
    failed = res['Value']['Failed']
    directories = []
    for url,isDirectory in res['Value']['Successful'].items():
      if isDirectory:
        directories.append(url)
      else:
        errStr = "RFIOStorage.getDirectoryMetadata: Directory does not exist."
        gLogger.error(errStr, url)
        failed[url] = errStr
    res = self.__getPathMetadata(directories)
    if not res['OK']:
      return res
    else:
      failed.update = res['Value']['Failed']
      successful = res['Value']['Successful']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectorySize(self,path):
    """ Get the size of the directory on the storage
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("RFIOStorage.getDirectorySize: Supplied path must be string or list of strings")
    gLogger.info("RFIOStorage.getDirectorySize: Attempting to get size of %s directories." % len(urls))
    res = self.listDirectory(urls)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for directory,dirDict in res['Value']['Successful'].items():
      directorySize = 0
      directoryFiles = 0
      filesDict = dirDict['Files']
      for fileURL,fileDict in filesDict.items():
        directorySize += fileDict['Size']
        directoryFiles += 1
      gLogger.info("RFIOStorage.getDirectorySize: Successfully obtained size of %s." % directory)
      successful[directory] = {'Files':directoryFiles,'Size':directorySize}
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  #############################################################
  #
  # These are the methods for manipulting the client
  #

  def changeDirectory(self,directory):
    """ Change the current directory
    """
    if directory[0] == '/':
      directory = directory.lstrip('/')
    self.cwd = '%s/%s' % (self.cwd,directory)

  def resetWorkingDirectory(self):
    """ Reset the working directory to the base dir
    """
    self.cwd = self.path

  def getCurrentDirectory(self):
    """ Get the current directory
    """
    return S_OK(self.cwd)

  def getName(self):
    """ The name with which the storage was instantiated
    """
    return S_OK(self.name)

  def getParameters(self):
    """ This gets all the storage specific parameters pass when instantiating the storage
    """
    parameterDict = {}
    parameterDict['StorageName'] = self.name
    parameterDict['ProtocolName'] = self.protocolName
    parameterDict['Protocol'] = self.protocol
    parameterDict['Host'] = self.host
    parameterDict['Path'] = self.path
    parameterDict['Port'] = self.port
    parameterDict['SpaceToken'] = self.spaceToken
    parameterDict['WSUrl'] = self.wspath
    return S_OK(parameterDict)

  def getProtocolPfn(self,pfnDict,withPort):
    """ From the pfn dict construct the pfn to be used
    """
    pfnDict['Protocol'] = ''
    pfnDict['Host'] = ''
    pfnDict['Port'] = ''
    pfnDict['WSUrl'] = ''
    res = pfnunparse(pfnDict)
    return res

  def getCurrentURL(self,fileName):
    """ Obtain the current file URL from the current working directory and the filename
    """
    if fileName:
      if fileName[0] == '/':
        fileName = fileName.lstrip('/')
    try:
      fullUrl = '%s/%s' % (self.cwd,fileName)
      return S_OK(fullUrl)
    except Exception,x:
      errStr = "RFIOStorage.getCurrentURL: Failed to create URL %s" % x
      return S_ERROR(errStr)

  def isPfnForProtocol(self,pfn):
    res = pfnparse(pfn)
    if not res['OK']:
      return res
    pfnDict = res['Value']
    if pfnDict['Protocol'] == self.protocol:
      return S_OK(True)
    else:
      return S_OK(False)
