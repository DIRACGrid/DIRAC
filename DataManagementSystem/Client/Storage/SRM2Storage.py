""" This is the SRM2 StorageClass """

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.File import getSize

from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient

from stat import *
import types, re,os,time,sys

ISOK = True

try:
  import lcg_util
  infoStr = 'Using lcg_util from: %s' % lcg_util.__file__
  gLogger.info(infoStr)
  infoStr = "The version of lcg_utils is %s" % lcg_util.lcg_util_version()
  gLogger.info(infoStr)
except Exception,x:
  errStr = "SRM2Storage.__init__: Failed to import lcg_util: %s" % (x)
  gLogger.exception(errStr)
  ISOK = False

try:
  import gfalthr as gfal
  infoStr = "Using gfalthr from: %s" % gfal.__file__
  gLogger.info(infoStr)
  infoStr = "The version of gfalthr is %s" % gfal.gfal_version()
  gLogger.info(infoStr)
except Exception,x:
  errStr = "SRM2Storage.__init__: Failed to import gfalthr: %s." % (x)
  gLogger.warn(errStr)
  try:
    import gfal
    infoStr = "Using gfal from: %s" % gfal.__file__
    gLogger.info(infoStr)
    infoStr = "The version of gfal is %s" % gfal.gfal_version()
    gLogger.info(infoStr)
  except Exception,x:
    errStr = "SRM2Storage.__init__: Failed to import gfal: %s" % (x)
    gLogger.exception(errStr)
    ISOK = False

class SRM2Storage(StorageBase):

  def __init__(self,storageName,protocol,path,host,port,spaceToken,wspath):
    self.isok = ISOK

    self.protocolName = 'SRM2'
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
    self.long_timeout = 1200
    self.fileTimeout = 10
    self.filesPerCall = 20

    # setting some variables for use with lcg_utils
    self.nobdii = 1
    self.defaulttype = 2
    self.vo = 'lhcb'
    self.nbstreams = 4
    self.verbose = 0
    self.conf_file = 'ignored'
    self.insecure = 0
    self.defaultLocalProtocols = gConfig.getValue('/Resources/StorageElements/DefaultProtocols',[])

  def isOK(self):
    return self.isok

  #############################################################
  #
  # These are the methods for file manipulation
  #

  def exists(self,path):
    """ Check if the given path exists. The 'path' variable can be a string or a list of strings.
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("SRM2Storage.exists: Supplied path must be string or list of strings")

    gLogger.debug("SRM2Storage.exists: Checking the existance of %s path(s)" % len(urls))
    resDict = self.__gfalls_wrapper(urls,0)['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key('surl'):
        pathSURL = self.getUrl(urlDict['surl'])['Value']
        if urlDict['status'] == 0:
          gLogger.debug("SRM2Storage.exists: Path exists: %s" % pathSURL)
          successful[pathSURL] = True
        elif urlDict['status'] == 2:
          gLogger.debug("SRM2Storage.exists: Path does not exist: %s" % pathSURL)
          successful[pathSURL] = False
        else:
          errStr = "SRM2Storage.exists: Failed to get path metadata."
          errMessage = os.strerror(urlDict['status'])
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def isFile(self,path):
    """Check if the given path exists and it is a file
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("SRM2Storage.isFile: Supplied path must be string or list of strings")

    gLogger.debug("SRM2Storage.isFile: Checking whether %s path(s) are file(s)." % len(urls))
    resDict = self.__gfalls_wrapper(urls,0)['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key('surl'):
        pathSURL = self.getUrl(urlDict['surl'])['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata(urlDict)
          if statDict['File']:
            successful[pathSURL] = True
          else:
            gLogger.debug("SRM2Storage.isFile: Path is not a file: %s" % pathSURL)
            successful[pathSURL] = False
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.isFile: File does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.isFile: Failed to get file metadata."
          errMessage = os.strerror(urlDict['status'])
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
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
      return S_ERROR("SRM2Storage.getFileMetadata: Supplied path must be string or list of strings")

    gLogger.debug("SRM2Storage.getFileMetadata: Obtaining metadata for %s file(s)." % len(urls))
    resDict = self.__gfalls_wrapper(urls,0)['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key('surl'):
        pathSURL = self.getUrl(urlDict['surl'])['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata(urlDict)
          if statDict['File']:
            successful[pathSURL] = statDict
          else:
            errStr = "SRM2Storage.getFileMetadata: Supplied path is not a file."
            gLogger.error(errStr,pathSURL)
            failed[pathSURL] = errStr
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.getFileMetadata: File does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.getFileMetadata: Failed to get file metadata."
          errMessage = os.strerror(urlDict['status'])
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
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
      return S_ERROR("SRM2Storage.getFileSize: Supplied path must be string or list of strings")

    gLogger.debug("SRM2Storage.getFileSize: Obtaining the size of %s file(s)." % len(urls))
    resDict = self.__gfalls_wrapper(urls,0)['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key('surl'):
        pathSURL = self.getUrl(urlDict['surl'])['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata(urlDict)
          if statDict['File']:
            successful[pathSURL] = statDict['Size']
          else:
            errStr = "SRM2Storage.getFileSize: Supplied path is not a file."
            gLogger.error(errStr,pathSURL)
            failed[pathSURL] = errStr
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.getFileSize: File does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.getFileSize: Failed to get file metadata."
          errMessage = os.strerror(urlDict['status'])
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
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
      return S_ERROR("SRM2Storage.removeFile: Supplied path must be string or list of strings")
    if not len(urls) > 0:
      return S_ERROR("SRM2Storage.removeFile: No surls supplied.")

    gLogger.debug("SRM2Storage.removeFile: Performing the removal of %s file(s)" % len(urls))
    resDict = self.__gfaldeletesurls_wrapper(urls)['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key('surl'):
        pathSURL = urlDict['surl']
        if urlDict['status'] == 0:
          infoStr = 'SRM2Storage.removeFile: Successfully removed file: %s' % pathSURL
          gLogger.debug(infoStr)
          successful[pathSURL] = True
        elif urlDict['status'] == 2:
          # This is the case where the file doesn't exist.
          infoStr = 'SRM2Storage.removeFile: File did not exist, sucessfully removed: %s' % pathSURL
          gLogger.debug(infoStr)
          successful[pathSURL] = True
        else:
          # We failed to remove the file
          errStr = "SRM2Storage.removeFile: Failed to remove file."
          reason = os.strerror(urlDict['status'])
          gLogger.error(errStr,'%s: %s' % (pathSURL,reason))
          failed[pathSURL] = reason
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getTransportURL(self,path,protocols=False):
    """ Obtain the tURLs for the supplied path and protocols
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("SRM2Storage.getTransportURL: Supplied path must be string or list of strings")

    if not protocols:
      infoStr = "SRM2Storage.getTransportURL: No protocols provided, using defaults."
      gLogger.debug(infoStr)
      listProtocols = gConfig.getValue('/Resources/StorageElements/DefaultProtocols',[])
      if not listProtocols:
        return S_ERROR("SRM2Storage.getTransportURL: No local protocols defined and no defaults found")
    elif type(protocols) == types.StringType:
      listProtocols = [protocols]
    elif type(protocols) == types.ListType:
      listProtocols = protocols
    else:
      return S_ERROR("SRM2Storage.getTransportURL: Must supply desired protocols to this plug-in.")

    gLogger.debug("SRM2Storage.getTransportURL: Obtaining tURLs for %s file(s)." % len(urls))
    resDict = self.__gfalturlsfromsurls_wrapper(urls,listProtocols)['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key('surl'):
        pathSURL = urlDict['surl']
        if urlDict['status'] == 0:
          gLogger.debug("SRM2Storage.getTransportURL: Obtained tURL for file. %s" % pathSURL)
          successful[pathSURL] = urlDict['turl']
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.getTransportURL: File does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.getTransportURL: Failed issue stage request."
          errMessage = os.strerror(urlDict['status'])
          gLogger.error(errStr,"%s: %s" % (errMessage,pathSURL))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
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
      return S_ERROR("SRM2Storage.prestageFile: Supplied path must be string or list of strings")

    gLogger.debug("SRM2Storage.prestageFile: Attempting to issue stage requests for %s file(s)." % len(urls))
    resDict = self.__gfalprestage_wrapper(urls)['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key('surl'):
        pathSURL = urlDict['surl']
        if urlDict['status'] == 0:
          gLogger.debug("SRM2Storage.prestageFile: Issued stage request for file %s." % pathSURL)
          successful[pathSURL] = True
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.prestageFile: File does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.prestageFile: Failed issue stage request."
          errMessage = os.strerror(urlDict['status'])
          gLogger.error(errStr,"%s: %s" % (errMessage,pathSURL))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  #############################################################
  #
  # These are the methods for file transfer
  #

  def getFile(self,fileTuple):
    """Get a local copy in the current directory of a physical file specified by its path
    """
    if type(fileTuple) == types.TupleType:
      urls = [fileTuple]
    elif type(fileTuple) == types.ListType:
      urls = fileTuple
    else:
      return S_ERROR("SRM2Storage.getFile: Supplied file information must be tuple of list of tuples")

    MAX_SINGLE_STREAM_SIZE = 1024*1024*10 # 10 MB
    MIN_BANDWIDTH = 5 * (1024*1024) # 5 MB/s

    srctype = self.defaulttype
    src_spacetokendesc = self.spaceToken
    dsttype = 0
    dest_spacetokendesc = ''
    failed = {}
    successful = {}
    for src_url,dest_file,size in urls:
      timeout = size/MIN_BANDWIDTH + 300
      if size > MAX_SINGLE_STREAM_SIZE:
        nbstreams = 4
      else:
        nbstreams = 1
      dest_url = 'file:%s' % dest_file
      gLogger.debug("SRM2Storage.getFile: Executing transfer of %s to %s" % (src_url, dest_url))
      errCode,errStr = lcg_util.lcg_cp3(src_url, dest_url, self.defaulttype, srctype, dsttype, self.nobdii, self.vo, nbstreams, self.conf_file, self.insecure, self.verbose, timeout,src_spacetokendesc,dest_spacetokendesc)
      if errCode == 0:
        gLogger.debug('SRM2Storage.getFile: Got file from storage, performing post transfer check.')
        localSize = getSize(dest_file)
        if localSize == size:
          gLogger.debug("SRM2Storage.getFile: Post transfer check successful.")
          successful[src_url] = True
        else:
          errStr = "SRM2Storage.getFile: Source and destination file sizes do not match."
          gLogger.error(errStr,src_url)
          if os.path.exists(dest_file):
            gLogger.debug("SRM2Storage.getFile: Removing local file.")
            os.remove(dest_file)
          failed[src_url] = errStr
      else:
        errorMessage = "SRM2Storage.getFile: Failed to get local copy of file."
        gLogger.error(errorMessage,"%s: %s" % (dest_file,errStr))
        failed[src_url] = errStr
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def putFile(self,fileTuple):
    """Put a file to the physical storage
    """
    if type(fileTuple) == types.TupleType:
      urls = [fileTuple]
    elif type(fileTuple) == types.ListType:
      urls = fileTuple
    else:
      return S_ERROR("SRM2Storage.putFile: Supplied file info must be tuple of list of tuples.")

    MAX_SINGLE_STREAM_SIZE = 1024*1024*10 # 10 MB
    MIN_BANDWIDTH = 1024*100 # 100 KB/s

    dsttype = self.defaulttype
    src_spacetokendesc = ''
    dest_spacetokendesc = self.spaceToken
    failed = {}
    successful = {}
    for src_file,dest_url,size in urls:
      timeout = size/MIN_BANDWIDTH + 300
      if size > MAX_SINGLE_STREAM_SIZE:
        nbstreams = 4
      else:
        nbstreams = 1
      if re.search('srm:',src_file) or re.search('gsiftp:',src_file):
        src_url = src_file
        srctype = 2
      else:
        src_url = 'file:%s' % src_file
        srctype = 0
      gLogger.debug("SRM2Storage.putFile: Executing transfer of %s to %s" % (src_url, dest_url))
      errCode,errStr = lcg_util.lcg_cp3(src_url, dest_url, self.defaulttype, srctype, dsttype, self.nobdii, self.vo, nbstreams, self.conf_file, self.insecure, self.verbose, timeout,src_spacetokendesc,dest_spacetokendesc)
      removeFile = True
      if errCode == 0:
        gLogger.debug("SRM2Storage.putFile: Put file to storage, performing post transfer check.")
        res = self.getFileSize(dest_url)
        if res['OK']:
          if res['Value']['Successful'].has_key(dest_url):
            remoteSize = res['Value']['Successful'][dest_url]
            #######################################################################
            # This is a dirty hack because gfal is rubbish
            if size > 1024*1024*1024*2-1:
              gLogger.debug("SRM2Storage.putFile: The file put was larger than 2GB.")
              gLogger.debug("SRM2Storage.putFile: Checking whether (remoteSize-size)%(2**32) == 0.")
              gLogger.debug("SRM2Storage.putFile: gfal returned size = %s and the file size is %s" % (remoteSize,size))
              gLogger.debug("SRM2Storage.putFile: Checking whether (remoteSize-size)%(2**32) == 0.")
              if (remoteSize-size)%(2**32) != 0:
                gLogger.debug("SRM2Storage.putFile: != 0")
                errMessage = "SRM2Storage.putFile: Source and destination file sizes do not match."
                gLogger.error(errMessage,dest_url)
                failed[dest_url] = errMessage
              else:
                gLogger.debug("SRM2Storage.putFile: = 0")
                successful[dest_url] = True
                removeFile = False
            #######################################################################
            elif remoteSize == size:
              gLogger.debug("SRM2Storage.putFile: Post transfer check successful.")
              successful[dest_url] = True
              removeFile = False
            else:
              errMessage = "SRM2Storage.putFile: Source and destination file sizes do not match."
              gLogger.error(errMessage,dest_url)
              failed[dest_url] = errMessage
          else:
            errMessage = "SRM2Storage.putFile: Failed to determine remote file size."
            gLogger.error(errMessage,dest_url)
            failed[dest_url] = errMessage
            gLogger.info("SRM2Storage.putFile: Even though we failed to get the file size I am not removing the file.")
            gLogger.info("SRM2Storage.putFile: Please remove the next line of code.")
            removeFile = False # HACK REMOVE
        else:
          errMessage = "SRM2Storage.putFile: Completely failed to determine remote file size."
          gLogger.error(errMessage,dest_url)
          failed[dest_url] = errMessage
      else:
        errMessage = "SRM2Storage.putFile: Failed to put file to remote storage."
        gLogger.error(errMessage,errStr)
        failed[dest_url] = errStr
      if removeFile:
        # This is because some part of the transfer failed.
        infoStr = "SRM2Storage.putFile: Removing destination url."
        gLogger.debug(infoStr)
        res = self.removeFile(dest_url)
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
      return S_ERROR("SRM2Storage.isDirectory: Supplied path must be string or list of strings")

    files = {}
    for url in urls:
      destFile = '%s/dirac_directory' % url
      files[destFile] = url

    gLogger.debug("SRM2Storage.isDirectory: Checking whether %s path(s) are directory(ies)" % len(files.keys()))
    resDict = self.__gfalls_wrapper(files.keys(),0)['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key('surl'):
        pathSURL = self.getUrl(urlDict['surl'])['Value']
        if files.has_key(pathSURL):
          dirSURL = files[pathSURL]
          if urlDict['status'] == 0:
            gLogger.debug("SRM2Storage.isDirectory: Supplied path is a DIRAC directory: %s" % dirSURL)
            successful[dirSURL] = True
          elif urlDict['status'] == 2:
            gLogger.debug("SRM2Storage.isDirectory: Supplied path is not a DIRAC directory: %s" % dirSURL)
            successful[dirSURL] = False
          else:
            errStr = "SRM2Storage.isDirectory: Failed to get file metadata."
            errMessage = os.strerror(urlDict['status'])
            gLogger.error(errStr,"%s: %s" % (dirSURL,errMessage))
            failed[dirSURL] = "%s %s" % (errStr,errMessage)
        else:
          gLogger.warn("SRM2Storage.isDirectory: faulty path URL %s from getUrl()" % pathSURL)
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
      return S_ERROR("SRM2Storage.getDirectoryMetadata: Supplied path must be string or list of strings")

    gLogger.debug("SRM2Storage.getDirectoryMetadata: Attempting to obtain metadata for %s directories." % len(urls))
    resDict = self.__gfalls_wrapper(urls,0)['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key('surl'):
        pathSURL = self.getUrl(urlDict['surl'])['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata(urlDict)
          if statDict['Directory']:
            successful[pathSURL] = statDict
          else:
            errStr = "SRM2Storage.getDirectoryMetadata: Supplied path is not a directory."
            gLogger.error(errStr,pathSURL)
            failed[pathSURL] = errStr
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.getDirectoryMetadata: Directory does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.getDirectoryMetadata: Failed to get directory metadata."
          errMessage = os.strerror(urlDict['status'])
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
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
      return S_ERROR("SRM2Storage.getDirectory: Supplied directory info must be tuple of list of tuples.")
    successful = {}
    failed = {}
    gLogger.debug("SRM2Storage.getDirectory: Attempting to get local copies of %s directories." % len(urls))

    for src_directory,destination_directory in urls:
      res = self.__getDir(src_directory,destination_directory)
      if res['OK']:
        if res['Value']['AllGot']:
          gLogger.debug("SRM2Storage.getDirectory: Successfully got local copy of %s" % src_directory)
          successful[src_directory] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
        else:
          gLogger.error("SRM2Storage.getDirectory: Failed to get entire directory.", src_directory)
          failed[src_directory] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
      else:
        gLogger.error("SRM2Storage.getDirectory: Completely failed to get local copy of directory.", src_directory)
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
      errStr = "SRM2Storage.__getDir: Failed to find the supplied source directory."
      gLogger.error(errStr,srcDirectory)
      return S_ERROR(errStr)
    if not res['Value']['Successful'].has_key(srcDirectory):
      errStr = "SRM2Storage.__getDir: Failed to find the supplied source directory."
      gLogger.error(errStr,srcDirectory)
      return S_ERROR(errStr)
    if not res['Value']['Successful'][srcDirectory]:
      errStr = "SRM2Storage.__getDir: The supplied source directory does not exist."
      gLogger.error(errStr,srcDirectory)
      return S_ERROR(errStr)

    # Check the local directory exists and create it if not
    if not os.path.exists(destDirectory):
      os.makedirs(destDirectory)

    # Get the remote directory contents
    res = self.listDirectory(srcDirectory)
    if not res['OK']:
      errStr = "SRM2Storage.__getDir: Failed to list the source directory."
      gLogger.error(errStr,srcDirectory)
    if not res['Value']['Successful'].has_key(srcDirectory):
      errStr = "SRM2Storage.__getDir: Failed to list the source directory."
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
      return S_ERROR("SRM2Storage.putDirectory: Supplied directory info must be tuple of list of tuples.")
    successful = {}
    failed = {}

    gLogger.debug("SRM2Storage.putDirectory: Attemping to put %s directories to remote storage." % len(urls))
    for sourceDir,destDir in urls:
      res = self.__putDir(sourceDir,destDir)
      if res['OK']:
        if res['Value']['AllPut']:
          gLogger.debug("SRM2Storage.putDirectory: Successfully put directory to remote storage: %s" % destDir)
          successful[destDir] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
        else:
          gLogger.error("SRM2Storage.putDirectory: Failed to put entire directory to remote storage.", destDir)
          failed[destDir] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
      else:
        gLogger.error("SRM2Storage.putDirectory: Completely failed to put directory to remote storage.", destDir)
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
      errStr = "SRM2Storage.__putDir: The supplied directory does not exist."
      gLogger.error(errStr,src_directory)
      return S_ERROR(errStr)

    # Create the remote directory
    res = self.createDirectory(dest_directory)
    if not res['OK']:
      errStr = "SRM2Storage.__putDir: Failed to create destination directory."
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
        res = self.__putDir(localPath,remotePath)
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
    """ Make recursively new directory(ies) on the physical storage
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("SRM2Storage.createDirectory: Supplied path must be string or list of strings")
    successful = {}
    failed = {}

    gLogger.debug("SRM2Storage.createDirectory: Attempting to create %s directories." % len(urls))
    for url in urls:
      strippedUrl = url.rstrip('/')
      res = self.__makeDirs(strippedUrl)
      if res['OK']:
        gLogger.debug("SRM2Storage.createDirectory: Successfully created directory on storage: %s" % url)
        successful[url] = True
      else:
        gLogger.error("SRM2Storage.createDirectory: Failed to create directory on storage.", "%s: %s" % (url,res['Message']))
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
              elif path.endswith(self.path):
                res = self.__makeDir(path)
              else:
                res = self.__makeDirs(dir)
                res = self.__makeDir(path)
    return res

  def removeDirectory(self,path):
    """Remove the recursively the files and sub directories
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("SRM2Storage.removeDirectory: Supplied path must be string or list of strings")
    successful = {}
    failed = {}
    gLogger.debug("SRM2Storage.removeDirectory: Attempting to remove %s directories" % len(urls))
    for url in urls:
      gLogger.debug("SRM2Storage.removeDirectory: Attempting to remove %s" % url)
      res = self.__removeDir(url)
      if res['OK']:
        if res['Value']['AllRemoved']:
          gLogger.debug("SRM2Storage.removeDirectory: Successfully removed all files. Removing 'dirac_directory' file.")
          successful[url] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
          # If all we successful then remove the dirac_directory file
          diracDirectoryFile = "%s/%s" % (url,'dirac_directory')
          res = self.removeFile(diracDirectoryFile)
        else:
          gLogger.error("SRM2Storage.removeDirectory: Failed to remove all files in directory.", url)
          failed[url] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
      else:
        gLogger.error("SRM2Storage.removeDirectory: Failed to remove any files in directory.", url)
        failed[url] = {'Files':0,'Size':0}
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __removeDir(self,directory):
    """ Black magic to recursively remove the directory and sub dirs. Repeatedly calls itself to delete recursively.
    """
    filesRemoved = 0
    sizeRemoved = 0
    res = self.listDirectory(directory)
    if not res['OK']:
      return S_ERROR("Failed to list directory")
    if not res['Value']['Successful'].has_key(directory):
      return S_ERROR("Failed to list directory")
    allFilesRemoved = False
    surlsDict = res['Value']['Successful'][directory]['Files']
    subDirsDict = res['Value']['Successful'][directory]['SubDirs']
    filesToRemove = []
    for url in surlsDict.keys():
      filesToRemove.append(url)
    if len(filesToRemove) > 0:
      res = self.removeFile(filesToRemove)
      if res['OK']:
        for removedSurl in res['Value']['Successful'].keys():
          filesRemoved += 1
          sizeRemoved += surlsDict[removedSurl]['Size']
          if len(res['Value']['Failed'].keys()) == 0:
            allFilesRemoved = True
    else:
      allFilesRemoved = True

    # Remove the sub directories found
    subDirsRemoved = True
    for subDir in subDirsDict.keys():
      res = self.__removeDir(subDir)
      if not res['OK']:
        subDirsRemoved = False
      if not res['Value']['AllRemoved']:
        subDirsRemoved = False
      filesRemoved += res['Value']['Files']
      sizeRemoved += res['Value']['Size']
    if subDirsRemoved and allFilesRemoved:
      allRemoved = True
    else:
      allRemoved = False
    resDict = {'AllRemoved':allRemoved,'Files':filesRemoved,'Size':sizeRemoved}
    return S_OK(resDict)

  def listDirectory(self,path):
    """ List the supplied path. First checks whether the path is a directory then gets the contents.
    """
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    else:
      return S_ERROR("SRM2Storage.listDirectory: Supplied path must be string or list of strings")

    gLogger.debug("SRM2Storage.listDirectory: Attempting to list %s directories." % len(urls))

    res = self.isDirectory(urls)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']

    directories = []
    for url,isDirectory in res['Value']['Successful'].items():
      if isDirectory:
         directories.append(url)
      else:
         errStr = "SRM2Storage.listDirectory: Directory does not exist."
         gLogger.error(errStr, url)
         failed[url] = errStr

    # Create the dictionary used by gfal
    gfalDict = {}
    gfalDict['surls'] = directories
    gfalDict['nbfiles'] =  len(directories)
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_lslevels'] = 1
    gfalDict['timeout'] = self.long_timeout

    errCode,gfalObject,errMessage = gfal.gfal_init(gfalDict)
    if not errCode == 0:
      errStr = "SRM2Storage.listDirectory: Failed to initialise gfal_init: %s" % errMessage
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.debug("SRM2Storage.listDirectory: Initialised gfal_init.")

    errCode,gfalObject,errMessage = gfal.gfal_ls(gfalObject)
    if not errCode == 0:
      errStr = "SRM2Storage.listDirectory: Failed to perform gfal_ls: %s" % errMessage
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.debug("SRM2Storage.listDirectory: Performed gfal_ls.")

    numberOfResults,gfalObject,listOfResults = gfal.gfal_get_results(gfalObject)
    if numberOfResults <= 0:
      errStr = "SRM2Storage.listDirectory: Did not obtain results with gfal_get_results."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.debug("SRM2Storage.listDirectory: Retrieved %s results from gfal_get_results." % numberOfResults)

    successful = {}
    for pathDict in listOfResults:
      pathSURL = self.getUrl(pathDict['surl'])['Value']
      if not pathDict['status'] == 0:
        errMessage = "SRM2Storage.listDirectory: Failed to list directory."
        errStr = os.strerror(pathDict['status'])
        gLogger.error(errMessage, "%s: %s." % (pathSURL,errStr))
        failed[pathSURL] = "%s %s" % (errMessage,errStr)
      else:
        successful[pathSURL] = {}
        gLogger.debug("SRM2Storage.listDirectory: Successfully listed directory %s" % pathSURL)
        if pathDict.has_key('subpaths'):
          subPathDirs = {}
          subPathFiles = {}
          subPaths = pathDict['subpaths']
          # Parse the subpaths for the directory
          for subPathDict in subPaths:
            subPathSURL = self.getUrl(subPathDict['surl'])['Value']
            if not os.path.basename(subPathSURL) == 'dirac_directory':
              subPathLocality = subPathDict['locality']
              if re.search('ONLINE',subPathLocality):
                subPathCached = 1
              else:
                subPathCached = 0
              if re.search('NEARLINE',subPathLocality):
                subPathMigrated = 1
              else:
                subPathMigrated = 0
              subPathStat = subPathDict['stat']
              subPathSize = subPathStat[ST_SIZE]
              subPathIsDir = S_ISDIR(subPathStat[ST_MODE])
              if subPathIsDir:
                # If the subpath is a directory
                subPathDirs[subPathSURL] = True
              else:
                # In the case that the subPath is a file
                subPathFiles[subPathSURL] = {'Size':subPathSize,'Cached':subPathCached,'Migrated':subPathMigrated}
          # Keep the infomation about this path's subpaths
          successful[pathSURL]['SubDirs'] = subPathDirs
          successful[pathSURL]['Files'] = subPathFiles
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
      return S_ERROR("SRM2Storage.getDirectorySize: Supplied path must be string or list of strings")

    gLogger.debug("SRM2Storage.getDirectorySize: Attempting to get size of %s directories." % len(urls))
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
      gLogger.debug("SRM2Storage.getDirectorySize: Successfully obtained size of %s." % directory)
      successful[directory] = {'Files':directoryFiles,'Size':directorySize}
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)


  ################################################################################
  #
  # The methods below are for manipulating the client
  #

  def resetWorkingDirectory(self):
    """ Reset the working directory to the base dir
    """
    self.cwd = self.path

  def changeDirectory(self,directory):
    """ Change the directory to the supplied directory
    """
    if directory[0] == '/':
      directory = directory.lstrip('/')
    self.cwd = '%s/%s' % (self.cwd,directory)

  def getCurrentURL(self,fileName):
    """ Obtain the current file URL from the current working directory and the filename
    """
    if fileName:
      if fileName[0] == '/':
        fileName = fileName.lstrip('/')
    try:
      fullUrl = '%s://%s:%s%s%s/%s' % (self.protocol,self.host,self.port,self.wspath,self.cwd,fileName)
      return S_OK(fullUrl)
    except Exception,x:
      errStr = "Failed to create URL %s" % x
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

  def getProtocolPfn(self,pfnDict,withPort):
    """ From the pfn dict construct the SURL to be used
    """
    #For srm2 keep the file name and path
    pfnDict['Protocol'] = self.protocol
    pfnDict['Host'] = self.host
    if withPort:
      pfnDict['Port'] = self.port
      pfnDict['WSUrl'] = self.wspath
    else:
      pfnDict['Port'] = ''
      pfnDict['WSUrl'] = ''
    res = pfnunparse(pfnDict)
    return res

  ################################################################################
  #
  # The methods below are URL manipulation methods
  #

  def getPFNBase(self,withPort=False):
    """ This will get the pfn base. This is then appended with the LFN in LHCb convention.
    """
    if withPort:
      pfnBase = 'srm://%s:%s%s' % (self.host,self.port,self.path)
    else:
      pfnBase = 'srm://%s%s' % (self.host,self.path)
    return S_OK(pfnBase)

  def getUrl(self,path,withPort=True):
    """ This gets the URL for path supplied. With port is optional.
    """
    # If the filename supplied already contains the storage base path then do not add it again
    if re.search(self.path,path):
      if withPort:
        url = 'srm://%s:%s%s%s' % (self.host,self.port,self.wspath,path)
      else:
        url = 'srm://%s%s' % (self.host,path)
    # If it is not prepend it to the file name
    else:
      pfnBase = self.getPFNBase(withPort)['Value']
      url = '%s%s' % (pfnBase,path)
    return S_OK(url)

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

  ################################################################################
  #
  # This is code to wrap the gfal binding
  #

  def __gfalprestage_wrapper(self,urls):
    """ This is a function that can be reused everywhere to perform the gfal_prestage
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken
    gfalDict['srmv2_desiredpintime'] = 60*60*24
    gfalDict['protocols'] = self.defaultLocalProtocols

    oAccounting = DataStoreClient()
    allResults = []
    failed = {}
    listOfLists = breakListIntoChunks(urls,self.filesPerCall)
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] =  len(urls)
      gfalDict['timeout'] = self.fileTimeout*len(urls)
      # Create a single DataOperation record for each
      oDataOperation = self.__initialiseAccountingObject('gfal_prestage',self.name,len(urls))
      failedLoop = False
      res = self.__create_gfal_object(gfalDict)
      if not res['OK']:
        failedLoop = True
      else:
        gfalObject = res['Value']
        oDataOperation.setStartTime()
        start = time.time()
        res = self.__gfal_prestage(gfalObject)
        end = time.time()
        oDataOperation.setEndTime()
        oDataOperation.setValueByKey('TransferTime',end-start)
        if not res['OK']:
          failedLoop = True
        else:
          gfalObject = res['Value']
          res = self.__get_results(gfalObject)
          if not res['OK']:
            failedLoop = True
          else:
            allResults.extend(res['Value'])
        self.__destroy_gfal_object(gfalObject)
      if failedLoop:
        oDataOperation.setValueByKey('TransferOK',0)
        oDataOperation.setValueByKey('FinalStatus','Failed')
        for url in urls:
          failed[url] = res['Message']

      oAccounting.addRegister(oDataOperation)
    oAccounting.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __gfalturlsfromsurls_wrapper(self,urls,listProtocols):
    """ This is a function that can be reused everywhere to perform the gfal_turlsfromsurls
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['protocols'] = listProtocols
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken

    oAccounting = DataStoreClient()
    allResults = []
    failed = {}
    listOfLists = breakListIntoChunks(urls,self.filesPerCall)
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] =  len(urls)
      gfalDict['timeout'] = self.fileTimeout*len(urls)
      # Create a single DataOperation record for each
      oDataOperation = self.__initialiseAccountingObject('gfal_turlsfromsurls',self.name,len(urls))
      failedLoop = False
      res = self.__create_gfal_object(gfalDict)
      if not res['OK']:
        failedLoop = True
      else:
        gfalObject = res['Value']
        oDataOperation.setStartTime()
        start = time.time()
        res = self.__gfal_turlsfromsurls(gfalObject)
        end = time.time()
        oDataOperation.setEndTime()
        oDataOperation.setValueByKey('TransferTime',end-start)
        if not res['OK']:
          failedLoop = True
        else:
          gfalObject = res['Value']
          res = self.__get_results(gfalObject)
          if not res['OK']:
            failedLoop = True
          else:
            allResults.extend(res['Value'])
        self.__destroy_gfal_object(gfalObject)
      if failedLoop:
        oDataOperation.setValueByKey('TransferOK',0)
        oDataOperation.setValueByKey('FinalStatus','Failed')
        for url in urls:
          failed[url] = res['Message']

      oAccounting.addRegister(oDataOperation)
    oAccounting.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __gfaldeletesurls_wrapper(self,urls):
    """ This is a function that can be reused everywhere to perform the gfal_deletesurls
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1

    oAccounting = DataStoreClient()

    allResults = []
    failed = {}
    listOfLists = breakListIntoChunks(urls,self.filesPerCall)
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] =  len(urls)
      gfalDict['timeout'] = self.fileTimeout*len(urls)

      # Create a single DataOperation record for each
      oDataOperation = self.__initialiseAccountingObject('gfal_deletesurls',self.name,len(urls))

      failedLoop = False
      res = self.__create_gfal_object(gfalDict)
      if not res['OK']:
        failedLoop = True
      else:
        gfalObject = res['Value']
        oDataOperation.setStartTime()
        start = time.time()
        res = self.__gfal_deletesurls(gfalObject)
        end = time.time()
        oDataOperation.setEndTime()
        oDataOperation.setValueByKey('TransferTime',end-start)
        if not res['OK']:
          failedLoop = True
        else:
          gfalObject = res['Value']
          res = self.__get_results(gfalObject)
          if not res['OK']:
            failedLoop = True
          else:
            allResults.extend(res['Value'])
        self.__destroy_gfal_object(gfalObject)
      if failedLoop:
        oDataOperation.setValueByKey('TransferOK',0)
        oDataOperation.setValueByKey('FinalStatus','Failed')
        for url in urls:
          failed[url] = res['Message']

      oAccounting.addRegister(oDataOperation)
    oAccounting.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __gfalls_wrapper(self,urls,depth):
    """ This is a function that can be reused everywhere to perform the gfal_ls
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_lslevels'] = depth

    oAccounting = DataStoreClient()

    allResults = []
    failed = {}
    listOfLists = breakListIntoChunks(urls,self.filesPerCall)
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] =  len(urls)
      gfalDict['timeout'] = self.fileTimeout*len(urls)

      # Create a single DataOperation record for each
      oDataOperation = self.__initialiseAccountingObject('gfal_ls',self.name,len(urls))

      failedLoop = False
      res = self.__create_gfal_object(gfalDict)
      if not res['OK']:
        failedLoop = True
      else:
        gfalObject = res['Value']
        oDataOperation.setStartTime()
        start = time.time()
        res = self.__gfal_ls(gfalObject)
        end = time.time()
        oDataOperation.setEndTime()
        oDataOperation.setValueByKey('TransferTime',end-start)
        if not res['OK']:
          failedLoop = True
        else:
          gfalObject = res['Value']
          res = self.__get_results(gfalObject)
          if not res['OK']:
            failedLoop = True
          else:
            allResults.extend(res['Value'])
        self.__destroy_gfal_object(gfalObject)
      if failedLoop:
        oDataOperation.setValueByKey('TransferOK',0)
        oDataOperation.setValueByKey('FinalStatus','Failed')
        for url in urls:
          failed[url] = res['Message']

      oAccounting.addRegister(oDataOperation)
    oAccounting.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __destroy_gfal_object(self,gfalObject):
    gLogger.verbose("SRM2Storage.__destroy_gfal_object: Performing gfal_internal_free.")
    errCode,gfalObject = gfal.gfal_internal_free(gfalObject)
    if errCode:
      errStr = "SRM2Storage.__destroy_gfal_object: Failed to perform gfal_internal_free:"
      gLogger.error(errStr,errCode)
      return S_ERROR()
    else:
      gLogger.verbose("SRM2Storage.__destroy_gfal_object: Successfully performed gfal_internal_free.")
      return S_OK()

  def __create_gfal_object(self,gfalDict):
    gLogger.verbose("SRM2Storage.__create_gfal_object: Performing gfal_init.")
    errCode,gfalObject,errMessage = gfal.gfal_init(gfalDict)
    if not errCode == 0:
      errStr = "SRM2Storage.__create_gfal_object: Failed to perform gfal_init:"
      gLogger.error(errStr,"%s %s" % (errMessage,os.strerror(errCode)))
      return S_ERROR()
    else:
      gLogger.verbose("SRM2Storage.__create_gfal_object: Successfully performed gfal_init.")
      gLogger.verbose("SRM2Storage.__create_gfal_object:",str(errCode))
      gLogger.verbose("SRM2Storage.__create_gfal_object:",str(gfalObject))
      gLogger.verbose("SRM2Storage.__create_gfal_object:",str(errMessage))
      return S_OK(gfalObject)

  def __gfal_deletesurls(self,gfalObject):
    gLogger.debug("SRM2Storage.__gfal_deletesurls: Performing gfal_deletesurls")
    errCode,gfalObject,errMessage = gfal.gfal_deletesurls(gfalObject)
    if not errCode == 0:
      errStr = "SRM2Storage.__gfal_deletesurls: Failed to perform gfal_deletesurls:"
      gLogger.error(errStr,"%s %s" % (errMessage,os.strerror(errCode)))
      return S_ERROR("%s%s" % (errStr,errMessage))
    else:
      gLogger.debug("SRM2Storage.__gfal_deletesurls: Successfully performed gfal_deletesurls.")
      return S_OK(gfalObject)

  def __gfal_ls(self,gfalObject):
    gLogger.debug("SRM2Storage.__gfal_ls: Performing gfal_ls")
    errCode,gfalObject,errMessage = gfal.gfal_ls(gfalObject)
    if not errCode == 0:
      errStr = "SRM2Storage.__gfal_ls: Failed to perform gfal_ls:"
      gLogger.error(errStr,"%s %s" % (errMessage,os.strerror(errCode)))
      return S_ERROR("%s%s" % (errStr,errMessage))
    else:
      gLogger.debug("SRM2Storage.__gfal_ls: Successfully performed gfal_ls.")
      return S_OK(gfalObject)

  def __gfal_prestage(self,gfalObject):
    gLogger.debug("SRM2Storage.__gfal_prestage: Performing gfal_prestage")
    gLogger.info("SRM2Storage.__gfal_prestage: WE ARE DOING gfal_get() INSTEAD TEMPORARILY.")
    errCode,gfalObject,errMessage = gfal.gfal_prestage(gfalObject)
    #errCode,gfalObject,errMessage = gfal.gfal_get(gfalObject)
    if not errCode == 0:
      errStr = "SRM2Storage.__gfal_prestage: Failed to perform gfal_prestage:"
      gLogger.error(errStr,"%s %s" % (errMessage,os.strerror(errCode)))
      return S_ERROR("%s%s" % (errStr,errMessage))
    else:
      gLogger.debug("SRM2Storage.__gfal_prestage: Successfully performed gfal_prestage.")
      return S_OK(gfalObject)

  def __gfal_turlsfromsurls(self,gfalObject):
    gLogger.debug("SRM2Storage.__gfal_turlsfromsurls: Performing gfal_turlsfromsurls")
    errCode,gfalObject,errMessage = gfal.gfal_turlsfromsurls(gfalObject)
    if not errCode == 0:
      errStr = "SRM2Storage.__gfal_turlsfromsurls: Failed to perform gfal_turlsfromsurls:"
      gLogger.error(errStr,"%s %s" % (errMessage,os.strerror(errCode)))
      return S_ERROR("%s%s" % (errStr,errMessage))
    else:
      gLogger.debug("SRM2Storage.__gfal_turlsfromsurls: Successfully performed gfal_turlsfromsurls.")
      return S_OK(gfalObject)

  def __get_results(self,gfalObject):
    gLogger.debug("SRM2Storage.__get_results: Performing gfal_get_results")
    numberOfResults,gfalObject,listOfResults = gfal.gfal_get_results(gfalObject)
    if numberOfResults <= 0:
      errStr = "SRM2Storage.__get_results: Did not obtain results with gfal_get_results."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    else:
      gLogger.debug("SRM2Storage.__get_results: Retrieved %s results from gfal_get_results." % numberOfResults)
      return S_OK(listOfResults)

  def __parse_stat(self,stat):
    statDict = {'File':False,'Directory':False}
    if S_ISREG(stat[ST_MODE]):
      statDict['File'] = True
      statDict['Size'] = stat[ST_SIZE]
    if S_ISDIR(stat[ST_MODE]):
      statDict['Directory'] = True
    statDict['Permissions'] = S_IMODE(stat[ST_MODE])
    return statDict

  def __parse_file_metadata(self,urlDict):
    statDict = self.__parse_stat(urlDict['stat'])
    if statDict['File']:
      urlLocality = urlDict['locality']
      if re.search('ONLINE',urlLocality):
        statDict['Cached'] = 1
      else:
        statDict['Cached'] = 0
      if re.search('NEARLINE',urlLocality):
        statDict['Migrated'] = 1
      else:
        statDict['Migrated'] = 0
    return statDict

  def __initialiseAccountingObject(self,operation,se,files):
    accountingDict = {}
    accountingDict['OperationType'] = operation
    accountingDict['User'] = 'acsmith'
    accountingDict['Protocol'] = 'gfal'
    accountingDict['RegistrationTime'] = 0.0
    accountingDict['RegistrationOK'] = 0
    accountingDict['RegistrationTotal'] = 0
    accountingDict['Destination'] = se
    accountingDict['TransferTotal'] = files
    accountingDict['TransferOK'] = files
    accountingDict['TransferSize'] = files
    accountingDict['TransferTime'] = 0.0
    accountingDict['FinalStatus'] = 'Successful'
    accountingDict['Source'] = gConfig.getValue('/LocalSite/Site','Unknown')
    oDataOperation = DataOperation()
    oDataOperation.setValuesFromDict(accountingDict)
    return oDataOperation
