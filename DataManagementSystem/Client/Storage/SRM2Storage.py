""" This is the SRM2 StorageClass """

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.File import getSize

from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient


from stat import *
import types, re,os,time,sys,string

ISOK = True

try:
  import lcg_util
  infoStr = 'Using lcg_util from: %s' % lcg_util.__file__
  gLogger.debug(infoStr)
  infoStr = "The version of lcg_utils is %s" % lcg_util.lcg_util_version()
  gLogger.debug(infoStr)
except Exception,x:
  errStr = "SRM2Storage.__init__: Failed to import lcg_util: %s" % (x)
  gLogger.exception(errStr,'',x)
  ISOK = False

try:
  import gfalthr as gfal
  infoStr = "Using gfalthr from: %s" % gfal.__file__
  gLogger.debug(infoStr)
  infoStr = "The version of gfalthr is %s" % gfal.gfal_version()
  gLogger.debug(infoStr)
except Exception,x:
  errStr = "SRM2Storage.__init__: Failed to import gfalthr: %s." % (x)
  gLogger.warn(errStr)
  try:
    import gfal
    infoStr = "Using gfal from: %s" % gfal.__file__
    gLogger.debug(infoStr)
    infoStr = "The version of gfal is %s" % gfal.gfal_version()
    gLogger.debug(infoStr)
  except Exception,x:
    errStr = "SRM2Storage.__init__: Failed to import gfal: %s" % (x)
    gLogger.exception(errStr,'',x)
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
    self.fileTimeout =  gConfig.getValue('/Resources/StorageElements/FileTimeout',30)
    self.filesPerCall = gConfig.getValue('/Resources/StorageElements/FilesPerCall',20)

    # setting some variables for use with lcg_utils
    self.nobdii = 1
    self.defaulttype = 2
    self.vo = 'lhcb'
    self.nbstreams = 4
    self.verbose = 0
    self.conf_file = 'ignored'
    self.insecure = 0
    self.defaultLocalProtocols = gConfig.getValue('/Resources/StorageElements/DefaultProtocols',[])

    self.MAX_SINGLE_STREAM_SIZE = 1024*1024*10 # 10 MB
    self.MIN_BANDWIDTH = 5 * (1024*1024) # 5 MB/s

  def isOK(self):
    return self.isok

################################################################################
#
# The methods below are for manipulating the client
#
################################################################################

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
################################################################################

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

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  ######################################################################
  #
  # This has to be updated once the new gfal_makedir() becomes available
  #

  def createDirectory(self,path):
    """ Make recursively new directory(ies) on the physical storage
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    successful = {}
    failed = {}
    gLogger.debug("SRM2Storage.createDirectory: Attempting to create %s directories." % len(urls))
    for url in urls.keys():
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
    srcFile = '%s/%s' % (os.getcwd(),'dirac_directory')
    dfile = open(srcFile,'w')
    dfile.write(" ")
    dfile.close()
    destFile = '%s/%s' % (path,'dirac_directory')
    directoryDict = {destFile:srcFile}
    res = self.__putFile(srcFile,destFile,0)
    if os.path.exists(srcFile):
      os.remove(srcFile)
    return res

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
      res = self.__makeDir(path)
    else:
      res = self.__makeDirs(dir)
      res = self.__makeDir(path)
    return res

################################################################################
#
# The methods below use the new generic methods for executing operations
#
################################################################################

  def removeFile(self,path):
    """Remove physically the file specified by its path
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

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
          errStr = "SRM2Storage.removeFile: Failed to remove file."
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getTransportURL(self,path,protocols=False):
    """ Obtain the tURLs for the supplied path and protocols
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    if not protocols:
      protocols = self.__getProtocols()
      if not protocols['OK']:
        return protocols
      listProtocols = protocols['Value']
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
          errStr = "SRM2Storage.getTransportURL: Failed to obtain turls."
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def prestageFile(self,path):
    """ Issue prestage request for file
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

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
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.prestageFile: File does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.prestageFile: Failed issue stage request."
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (errMessage,pathSURL))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def prestageFileStatus(self,path):
    """ Monitor prestage request for files
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    gLogger.debug("SRM2Storage.prestageFileStatus: Attempting to get status of stage requests for %s file(s)." % len(urls))
    resDict = self.__gfal_prestagestatus_wrapper(urls)['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key('surl'):
        pathSURL = urlDict['surl']
        if urlDict['status'] == 1:
          gLogger.debug("SRM2Storage.prestageFileStatus: File found to be staged %s." % pathSURL)
          successful[pathSURL] = True
        elif urlDict['status'] == 0:
          gLogger.debug("SRM2Storage.prestageFileStatus: File not staged %s." % pathSURL)
          successful[pathSURL] = False
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.prestageFileStatus: File does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.prestageFileStatus: Failed get prestage status."
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (errMessage,pathSURL))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileMetadata(self,path):
    """  Get metadata associated to the file
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

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
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def isFile(self,path):
    """Check if the given path exists and it is a file
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

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
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def pinFile(self,path,lifetime=60*60*24):
    """ Pin a file with a given lifetime
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    gLogger.debug("SRM2Storage.pinFile: Attempting to pin %s file(s)." % len(urls))
    resDict = self.__gfal_pin_wrapper(urls,lifetime)['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key('surl'):
        pathSURL = urlDict['surl']
        if urlDict['status'] == 0:
          gLogger.debug("SRM2Storage.pinFile: Issued pin request for file %s." % pathSURL)
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.pinFile: File does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.pinFile: Failed issue pin request."
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (errMessage,pathSURL))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def releaseFile(self,path):
    """ Release a file
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    gLogger.debug("SRM2Storage.releaseFile: Attempting to release %s file(s)." % len(urls))
    resDict = self.__gfal_release_wrapper(urls)['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key('surl'):
        pathSURL = urlDict['surl']
        if urlDict['status'] == 0:
          gLogger.debug("SRM2Storage.releaseFile: Issued release request for file %s." % pathSURL)
          successful[pathSURL] = urlDict['SRMReqID']
        elif urlDict['status'] == 2:
          errMessage = "SRM2Storage.releaseFile: File does not exist."
          gLogger.error(errMessage,pathSURL)
          failed[pathSURL] = errMessage
        else:
          errStr = "SRM2Storage.releaseFile: Failed issue release request."
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (errMessage,pathSURL))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def exists(self,path):
    """ Check if the given path exists.
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

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
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileSize(self,path):
    """Get the physical size of the given file
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    gLogger.debug("SRM2Storage.getFileSize: Obtaining the size of %s file(s)." % len(urls.keys()))
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
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def putFile(self,path,sourceSize=0):
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']
    failed = {}
    successful = {}
    for dest_url,src_file in urls.items():
    # Create destination directory
      res = self.__executeOperation(os.path.dirname(dest_url),'createDirectory')
      if not res['OK']:
        failed[dest_url] = res['Message']
      else:
        res = self.__putFile(src_file,dest_url,sourceSize)
        if res['OK']:
          successful[dest_url] = res['Value']
        else:
          failed[dest_url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __putFile(self,src_file,dest_url,sourceSize):
    # Pre-transfer check
    res = self.__executeOperation(dest_url,'exists')
    if not res['OK']:
      gLogger.debug("SRM2Storage.__putFile: Failed to find pre-existance of destination file.")
      return res
    if res['Value']:
      res = self.__executeOperation(dest_url,'removeFile')
      if not res['OK']:
        gLogger.debug("SRM2Storage.__putFile: Failed to remove remote file %s." % dest_url)
      else:
        gLogger.debug("SRM2Storage.__putFile: Removed remote file %s." % dest_url)
    dsttype = self.defaulttype
    src_spacetokendesc = ''
    dest_spacetokendesc = self.spaceToken
    if re.search('srm:',src_file):
      src_url = src_file
      srctype = 2
      if not sourceSize:
        return S_ERROR("SRM2Storage.__putFile: For file replication the source file size must be provided.")
    else:
      if not os.path.exists(src_file):
        errStr = "SRM2Storage.__putFile: The source local file does not exist."
        gLogger.error(errStr,src_file)
        return S_ERROR(errStr)
      sourceSize = getSize(src_file)
      if sourceSize == -1:
        errStr = "SRM2Storage.__putFile: Failed to get file size."
        gLogger.error(errStr,src_file)
        return S_ERROR(errStr)
      src_url = 'file:%s' % src_file
      srctype = 0
    if sourceSize == 0:
      errStr = "SRM2Storage.__putFile: Source file is zero size."
      gLogger.error(errStr,src_file)
      return S_ERROR(errStr)
    timeout = sourceSize/self.MIN_BANDWIDTH + 300
    if sourceSize > self.MAX_SINGLE_STREAM_SIZE:
      nbstreams = 4
    else:
      nbstreams = 1
    gLogger.debug("SRM2Storage.__putFile: Executing transfer of %s to %s" % (src_url, dest_url))
    errCode,errStr = lcg_util.lcg_cp3(src_url, dest_url, self.defaulttype, srctype, dsttype, self.nobdii, self.vo, nbstreams, self.conf_file, self.insecure, self.verbose, timeout,src_spacetokendesc,dest_spacetokendesc)
    if errCode == 0:
      gLogger.debug('SRM2Storage.__putFile: Successfully put file to storage.')
      res = self.__executeOperation(dest_url,'getFileSize')
      if res['OK']:
        destinationSize = res['Value']
        if sourceSize == destinationSize :
          gLogger.debug("SRM2Storage.__putFile: Post transfer check successful.")
          return S_OK(destinationSize)
      errorMessage = "SRM2Storage.__getFile: Source and destination file sizes do not match."
      gLogger.error(errorMessage,src_url)
    else:
      errorStr = "SRM2Storage.__putFile: Failed to put file to storage."
      errorMessage = ''
      if errCode > 0:
        errorMessage = "%s %s" % (errorMessage,os.strerror(errCode))
      errorMessage = "%s %s" % (errorMessage,errStr)
      gLogger.error(errorStr,errorMessage)
    res = self.__executeOperation(dest_url,'removeFile')
    if res['OK']:
      gLogger.debug("SRM2Storage.__putFile: Removed remote file remnant %s." % dest_url)
    else:
      gLogger.debug("SRM2Storage.__putFile: Unable to remove remote file remnant %s." % dest_url)
    return S_ERROR(errorMessage)

  def getFile(self,path,localPath=False):
    """ Get a local copy in the current directory of a physical file specified by its path
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}
    for src_url in urls.keys():
      fileName = os.path.basename(src_url)
      if localPath:
        dest_file = "%s/%s" % (localPath,fileName)
      else:
        dest_file = "%s/%s" % (os.getcwd(),fileName)
      res = self.__getFile(src_url,dest_file)
      if res['OK']:
        successful[src_url] = res['Value']
      else:
        failed[src_url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __getFile(self,src_url,dest_file):
    if not os.path.exists(os.path.dirname(dest_file)):
      os.makedirs(os.path.dirname(dest_file))
    if os.path.exists(dest_file):
      gLogger.debug("SRM2Storage.getFile: Local file already exists %s. Removing..." % dest_file)
      os.remove(dest_file)
    srctype = self.defaulttype
    src_spacetokendesc = self.spaceToken
    dsttype = 0
    dest_spacetokendesc = ''
    dest_url = 'file:%s' % dest_file
    res = self.__executeOperation(src_url,'getFileSize')
    if not res['OK']:
      return S_ERROR(res['Message'])
    remoteSize = res['Value']
    timeout = remoteSize/self.MIN_BANDWIDTH + 300
    if remoteSize > self.MAX_SINGLE_STREAM_SIZE:
      nbstreams = 4
    else:
      nbstreams = 1
    gLogger.debug("SRM2Storage.__getFile: Executing transfer of %s to %s" % (src_url, dest_url))
    errCode,errStr = lcg_util.lcg_cp3(src_url, dest_url, self.defaulttype, srctype, dsttype, self.nobdii, self.vo, nbstreams, self.conf_file, self.insecure, self.verbose, timeout,src_spacetokendesc,dest_spacetokendesc)
    if errCode == 0:
      gLogger.debug('SRM2Storage.__getFile: Got a file from storage.')
      localSize = getSize(dest_file)
      if localSize == remoteSize:
        gLogger.debug("SRM2Storage.getFile: Post transfer check successful.")
        return S_OK(localSize)
      errStr = "SRM2Storage.__getFile: Source and destination file sizes do not match."
      gLogger.error(errStr,src_url)
    else:
      errorMessage = "SRM2Storage.getFile: Failed to get local copy of file."
      if errCode > 0:
        errorMessage = "%s %s" % (errorMessage,os.strerror(errCode))
      errorMessage = "%s %s" % (errorMessage,errStr)
    if os.path.exists(dest_file):
      gLogger.debug("SRM2Storage.getFile: Removing local file %s." % dest_file)
      os.remove(dest_file)
    return S_ERROR(errorMessage)

  def __executeOperation(self,url,method):
    """ Executes the requested functionality with the supplied url
    """
    execString = "res = self.%s(url)" % method
    try:
      exec(execString)
      if not res['OK']:
        return S_ERROR(res['Message'])
      elif not res['Value']['Successful'].has_key(url):
        return S_ERROR(res['Value']['Failed'][url])
      else:
        return S_OK(res['Value']['Successful'][url])
    except AttributeError,errMessage:
      exceptStr = "SRM2Storage.__executeOperation: Exception while perfoming %s." % method
      gLogger.exception(exceptStr,'',errMessage)
      return S_ERROR("%s%s" % (exceptStr,errMessage))

  ############################################################################################
  #
  # Directory based methods
  #

  def isDirectory(self,path):
    """Check if the given path exists and it is a directory
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    gLogger.debug("SRM2Storage.isDirectory: Checking whether %s path(s) are directory(ies)" % len(urls.keys()))
    resDict = self.__gfalls_wrapper(urls,0)['Value']
    failed = resDict['Failed']
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key('surl'):
        dirSURL = self.getUrl(urlDict['surl'])['Value']
        if urlDict['status'] == 0:
          statDict = self.__parse_file_metadata(urlDict)
          if statDict['Directory']:
            successful[dirSURL] = True
          else:
            gLogger.debug("SRM2Storage.isDirectory: Path is not a directory: %s" % dirSURL)
            successful[dirSURL] = False
        elif urlDict['status'] == 2:
          gLogger.debug("SRM2Storage.isDirectory: Supplied path does not exist: %s" % dirSURL)
          failed[dirSURL] = 'Directory does not exist'
        else:
          errStr = "SRM2Storage.isDirectory: Failed to get file metadata."
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (dirSURL,errMessage))
          failed[dirSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectoryMetadata(self,path):
    """ Get the metadata for the directory
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

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
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectorySize(self,path):
    """ Get the size of the directory on the storage
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

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
      subDirectories = len(dirDict['SubDirs'])
      successful[directory] = {'Files':directoryFiles,'Size':directorySize,'SubDirs':subDirectories}
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def listDirectory(self,path):
    """ List the contents of the directory on the storage
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    gLogger.debug("SRM2Storage.listDirectory: Attempting to list %s directories." % len(urls))

    res = self.isDirectory(urls)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    directories ={}
    for url,isDirectory in res['Value']['Successful'].items():
      if isDirectory:
        directories[url] = False
      else:
        errStr = "SRM2Storage.listDirectory: Directory does not exist."
        gLogger.error(errStr, url)
        failed[url] = errStr

    resDict = self.__gfalls_wrapper(directories,1)['Value']
    failed.update(resDict['Failed'])
    listOfResults = resDict['AllResults']
    successful = {}
    for urlDict in listOfResults:
      if urlDict.has_key('surl'):
        pathSURL = self.getUrl(urlDict['surl'])['Value']
        if urlDict['status'] == 0:
          successful[pathSURL] = {}
          gLogger.debug("SRM2Storage.listDirectory: Successfully listed directory %s" % pathSURL)
          subPathDirs = {}
          subPathFiles = {}
          if urlDict.has_key('subpaths'):
            subPaths = urlDict['subpaths']
            # Parse the subpaths for the directory
            for subPathDict in subPaths:
              subPathSURL = self.getUrl(subPathDict['surl'])['Value']
              statDict = self.__parse_file_metadata(subPathDict)
              if statDict['File']:
                subPathFiles[subPathSURL] = statDict
              elif statDict['Directory']:
                subPathDirs[subPathSURL] = statDict
          # Keep the infomation about this path's subpaths
          successful[pathSURL]['SubDirs'] = subPathDirs
          successful[pathSURL]['Files'] = subPathFiles
        else:
          errStr = "SRM2Storage.listDirectory: Failed to list directory."
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)

    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def putDirectory(self,path):
    """ Put a local directory to the physical storage together with all its files and subdirectories.
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    successful = {}
    failed = {}
    gLogger.debug("SRM2Storage.putDirectory: Attemping to put %s directories to remote storage." % len(urls))
    for destDir,sourceDir in urls.items():
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
    # Check the local directory exists
    if not os.path.isdir(src_directory):
      errStr = "SRM2Storage.__putDir: The supplied directory does not exist."
      gLogger.error(errStr,src_directory)
      return S_ERROR(errStr)

    # Get the local directory contents
    contents = os.listdir(src_directory)
    allSuccessful = True
    directoryFiles = {}
    for file in contents:
      localPath = '%s/%s' % (src_directory,file)
      remotePath = '%s/%s' % (dest_directory,file)
      if not os.path.isdir(localPath):
        directoryFiles[remotePath] = localPath
      else:
        res = self.__putDir(localPath,remotePath)
        if not res['OK']:
          errStr = "SRM2Storage.__putDir: Failed to put directory to storage."
          gLogger.error(errStr,res['Message'])
        else:
          if not res['Value']['AllPut']:
            pathSuccessful = False
          filesPut += res['Value']['Files']
          sizePut += res['Value']['Size']

    if directoryFiles:
      res = self.putFile(directoryFiles)
      if not res['OK']:
        gLogger.error("SRM2Storage.__putDir: Failed to put files to storage.",res['Message'])
        allSuccessful = False
      else:
        for pfn,fileSize in res['Value']['Successful'].items():
          filesPut += 1
          sizePut += fileSize
        if res['Value']['Failed']:
          allSuccessful = False
    resDict = {'AllPut':allSuccessful,'Files':filesPut,'Size':sizePut}
    return S_OK(resDict)

  def getDirectory(self,path,localPath=False):
    """ Get a local copy in the current directory of a physical file specified by its path
    """
    res = self.checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']

    failed = {}
    successful = {}
    gLogger.debug("SRM2Storage.getDirectory: Attempting to get local copies of %s directories." % len(urls))
    for src_dir in urls.keys():
      dirName = os.path.basename(src_dir)
      if localPath:
        dest_dir = "%s/%s" % (localPath,dirName)
      else:
        dest_dir = "%s/%s" % (os.getcwd(),dirName)
      res = self.__getDir(src_dir,dest_dir)
      if res['OK']:
        if res['Value']['AllGot']:
          gLogger.debug("SRM2Storage.getDirectory: Successfully got local copy of %s" % src_dir)
          successful[src_dir] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
        else:
          gLogger.error("SRM2Storage.getDirectory: Failed to get entire directory.", src_dir)
          failed[src_dir] = {'Files':res['Value']['Files'],'Size':res['Value']['Size']}
      else:
        gLogger.error("SRM2Storage.getDirectory: Completely failed to get local copy of directory.", src_dir)
        failed[src_dir] = {'Files':0,'Size':0}
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __getDir(self,srcDirectory,destDirectory):
    """ Black magic contained within...
    """
    filesGot = 0
    sizeGot = 0

    # Check the remote directory exists
    res = self.__executeOperation(srcDirectory,'isDirectory')
    if not res['OK']:
      gLogger.error("SRM2Storage.__getDir: Failed to find the supplied source directory.",srcDirectory)
      return res
    if not res['Value']:
      errStr = "SRM2Storage.__getDir: The supplied source path is not a directory."
      gLogger.error(errStr,srcDirectory)
      return S_ERROR(errStr)

    # Check the local directory exists and create it if not
    if not os.path.exists(destDirectory):
      os.makedirs(destDirectory)

    # Get the remote directory contents
    res = self.__getDirectoryContents(srcDirectory)
    if not res['OK']:
      errStr = "SRM2Storage.__getDir: Failed to list the source directory."
      gLogger.error(errStr,srcDirectory)
    filesToGet = res['Value']['Files']
    subDirs = res['Value']['SubDirs']

    allSuccessful = True
    res = self.getFile(filesToGet.keys(),destDirectory)
    if not res['OK']:
      gLogger.error("SRM2Storage.__getDir: Failed to get files from storage.",res['Message'])
      allSuccessful = False
    else:
      for pfn,fileSize in res['Value']['Successful'].items():
        filesGot += 1
        sizeGot += fileSize
      if res['Value']['Failed']:
        allSuccessful = False

    for subDir in subDirs:
      subDirName = os.path.basename(subDir)
      localPath = '%s/%s' % (destDirectory,subDirName)
      res = self.__getDir(subDir,localPath)
      if res['OK']:
        if not res['Value']['AllGot']:
          allSuccessful = True
        filesGot += res['Value']['Files']
        sizeGot += res['Value']['Size']

    resDict = {'AllGot':allSuccessful,'Files':filesGot,'Size':sizeGot}
    return S_OK(resDict)

  def removeDirectory(self,path,recursive=False):
    """ Remove a directory
    """
    if recursive:
      return self.__removeDirectoryRecursive(path)
    else:
      return self.__removeDirectory(path)

  def __removeDirectory(self,directory):
    """ This function removes the directory on the storage
    """
    res = self.checkArgumentFormat(directory)
    if not res['OK']:
      return res
    urls = res['Value']

    gLogger.debug("SRM2Storage.__removeDirectory: Attempting to remove %s directories." % len(urls))
    resDict = self.__gfalremovedir_wrapper(urls)['Value']
    failed = resDict['Failed']
    allResults = resDict['AllResults']
    successful = {}
    for urlDict in allResults:
      if urlDict.has_key('surl'):
        pathSURL = urlDict['surl']
        if urlDict['status'] == 0:
          infoStr = 'SRM2Storage.__removeDirectory: Successfully removed directory: %s' % pathSURL
          gLogger.debug(infoStr)
          successful[pathSURL] = True
        elif urlDict['status'] == 2:
          # This is the case where the file doesn't exist.
          infoStr = 'SRM2Storage.__removeDirectory: Directory did not exist, sucessfully removed: %s' % pathSURL
          gLogger.debug(infoStr)
          successful[pathSURL] = True
        else:
          errStr = "SRM2Storage.removeDirectory: Failed to remove directory."
          errMessage = urlDict['ErrorMessage']
          gLogger.error(errStr,"%s: %s" % (pathSURL,errMessage))
          failed[pathSURL] = "%s %s" % (errStr,errMessage)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __removeDirectoryRecursive(self,directory):
    """ Recursively removes the directory and sub dirs. Repeatedly calls itself to delete recursively.
    """
    res = self.checkArgumentFormat(directory)
    if not res['OK']:
      return res
    urls = res['Value']

    successful = {}
    failed = {}
    gLogger.debug("SRM2Storage.__removeDirectory: Attempting to recursively remove %s directories." % len(urls))
    for directory in urls.keys():
      gLogger.debug("SRM2Storage.removeDirectory: Attempting to remove %s" % directory)
      res = self.__getDirectoryContents(directory)
      resDict = {'FilesRemoved':0,'SizeRemoved':0}
      if not res['OK']:
        failed[directory] = resDict
      else:
        filesToRemove = res['Value']['Files']
        subDirs = res['Value']['SubDirs']
        # Remove all the files in the directory
        res = self.__removeDirectoryFiles(filesToRemove)
        resDict['FilesRemoved'] += res['FilesRemoved']
        resDict['SizeRemoved'] += res['SizeRemoved']
        allFilesRemoved = res['AllRemoved']
        # Remove all the sub-directories
        res = self.__removeSubDirectories(subDirs)
        resDict['FilesRemoved'] += res['FilesRemoved']
        resDict['SizeRemoved'] += res['SizeRemoved']
        allSubDirsRemoved = res['AllRemoved']
        # If all the files and sub-directories are removed then remove the directory
        allRemoved = False
        if allFilesRemoved and allSubDirsRemoved:
          gLogger.debug("SRM2Storage.removeDirectory: Successfully removed all files and sub-directories.")
          res = self.__removeDirectory(directory)
          if res['OK']:
            if res['Value']['Successful'].has_key(directory):
              gLogger.debug("SRM2Storage.removeDirectory: Successfully removed the directory %s." % directory)
              allRemoved = True
        # Report the result
        if allRemoved:
          successful[directory] = resDict
        else:
          failed[directory] = resDict
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __getDirectoryContents(self,directory):
    res = self.listDirectory(directory)
    errMessage = "SRM2Storage.__getDirectoryContents: Failed to list directory."
    if not res['OK']:
      gLogger.error(errMessage,res['Message'])
      return S_ERROR(errMessage)
    if not res['Value']['Successful'].has_key(directory):
      gLogger.error(errMessage,res['Value']['Failed'][directory])
      return S_ERROR(errMessage)
    surlsDict = res['Value']['Successful'][directory]['Files']
    subDirsDict = res['Value']['Successful'][directory]['SubDirs']
    filesToRemove = {}
    for url in surlsDict.keys():
      filesToRemove[url] = surlsDict[url]['Size']
    resDict = {'Files':filesToRemove,'SubDirs':subDirsDict.keys()}
    return S_OK(resDict)

  def __removeDirectoryFiles(self,filesToRemove):
    resDict = {'FilesRemoved':0,'SizeRemoved':0,'AllRemoved':True}
    if len(filesToRemove) > 0:
      res = self.removeFile(filesToRemove.keys())
      if res['OK']:
        for removedSurl in res['Value']['Successful'].keys():
          resDict['FilesRemoved'] += 1
          resDict['SizeRemoved'] += filesToRemove[removedSurl]
        if len(res['Value']['Failed'].keys()) != 0:
          resDict['AllRemoved'] = False
    gLogger.debug("SRM2Storage.__removeDirectoryFiles: Removed %s files of size %s bytes." % (resDict['FilesRemoved'],resDict['SizeRemoved']))
    return resDict

  def __removeSubDirectories(self,subDirectories):
    resDict = {'FilesRemoved':0,'SizeRemoved':0,'AllRemoved':True}
    if len(subDirectories) > 0:
      res = self.__removeDirectoryRecursive(subDirectories)
      if res['OK']:
        for removedSubDir,removedDict in res['Value']['Successful'].items():
          resDict['FilesRemoved'] += removedDict['FilesRemoved']
          resDict['SizeRemoved'] += removedDict['SizeRemoved']
          gLogger.debug("SRM2Storage.__removeSubDirectories: Removed %s files of size %s bytes from %s." % (removedDict['FilesRemoved'],removedDict['SizeRemoved'],removedSubDir))
        for removedSubDir,removedDict in res['Value']['Failed'].items():
          resDict['FilesRemoved'] += removedDict['FilesRemoved']
          resDict['SizeRemoved'] += removedDict['SizeRemoved']
          gLogger.debug("SRM2Storage.__removeSubDirectories: Removed %s files of size %s bytes from %s." % (removedDict['FilesRemoved'],removedDict['SizeRemoved'],removedSubDir))
        if len(res['Value']['Failed'].keys()) != 0:
          resDict['AllRemoved'] = False
    return resDict

  def checkArgumentFormat(self,path):
    if type(path) in types.StringTypes:
      urls = {path:False}
    elif type(path) == types.ListType:
      urls = {}
      for url in path:
        urls[url] = False
    elif type(path) == types.DictType:
     urls = path
    else:
      return S_ERROR("SRM2Storage.checkArgumentFormat: Supplied path is not of the correct format.")
    return S_OK(urls)

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
      if urlDict.has_key('locality'):
        urlLocality = urlDict['locality']
        if re.search('ONLINE',urlLocality):
          statDict['Cached'] = 1
        else:
          statDict['Cached'] = 0
        if re.search('NEARLINE',urlLocality):
          statDict['Migrated'] = 1
        else:
          statDict['Migrated'] = 0
        statDict['Lost'] = 0
        if re.search('LOST',urlLocality):
          statDict['Lost'] = 1
        statDict['Unavailable'] = 0
        if re.search('UNAVAILABLE',urlLocality):
          statDict['Unavailable'] = 1
    return statDict

  def __getProtocols(self):
    """Returns list of protocols to use at given site.  Priority is given to a protocols list
       defined in the CS.
    """
    sections = gConfig.getSections('/Resources/StorageElements/%s/' %(self.name))
    if not sections['OK']:
      return sections

    protocolsList = []
    for section in sections['Value']:
      path = '/Resources/StorageElements/%s/%s/ProtocolName' %(self.name,section)
      if gConfig.getValue(path,'')==self.protocolName:
        protPath = '/Resources/StorageElements/%s/%s/ProtocolsList' %(self.name,section)
        siteProtocols = gConfig.getValue(protPath,[])
        if siteProtocols:
          gLogger.debug('Found SE protocols list to override defaults: %s' %(string.join(siteProtocols,', ')))
          protocolsList = siteProtocols

    if not protocolsList:
      gLogger.debug("SRM2Storage.getTransportURL: No protocols provided, using defaults.")
      protocolsList = gConfig.getValue('/Resources/StorageElements/DefaultProtocols',[])

    if not protocolsList:
      return S_ERROR("SRM2Storage.getTransportURL: No local protocols defined and no defaults found")

    return S_OK(protocolsList)

#######################################################################
#
# These methods wrap the gfal functionality with the accounting. All these are based on __gfal_operation_wrapper()
#
#######################################################################

  def __gfalls_wrapper(self,urls,depth):
    """ This is a function that can be reused everywhere to perform the gfal_ls
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_lslevels'] = depth

    allResults = []
    failed = {}
    listOfLists = breakListIntoChunks(urls.keys(),self.filesPerCall)
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] =  len(urls)
      gfalDict['timeout'] = self.fileTimeout*len(urls)
      res = self.__gfal_operation_wrapper('gfal_ls',gfalDict)
      gDataStoreClient.addRegister(res['AccountingOperation'])
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend(res['Value'])

    gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __gfalprestage_wrapper(self,urls):
    """ This is a function that can be reused everywhere to perform the gfal_prestage
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken
    gfalDict['srmv2_desiredpintime'] = 60*60*24
    gfalDict['protocols'] = self.defaultLocalProtocols
    allResults = []
    failed = {}

    listOfLists = breakListIntoChunks(urls.keys(),self.filesPerCall)
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] =  len(urls)
      gfalDict['timeout'] = self.fileTimeout*len(urls)
      res = self.__gfal_operation_wrapper('gfal_prestage',gfalDict)
      gDataStoreClient.addRegister(res['AccountingOperation'])
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend(res['Value'])

    gDataStoreClient.commit()
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
    allResults = []
    failed = {}

    listOfLists = breakListIntoChunks(urls.keys(),self.filesPerCall)
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] =  len(urls)
      gfalDict['timeout'] = self.fileTimeout*len(urls)
      res = self.__gfal_operation_wrapper('gfal_turlsfromsurls',gfalDict)
      gDataStoreClient.addRegister(res['AccountingOperation'])
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend(res['Value'])

    gDataStoreClient.commit()
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
    allResults = []
    failed = {}

    listOfLists = breakListIntoChunks(urls.keys(),self.filesPerCall)
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] =  len(urls)
      gfalDict['timeout'] = self.fileTimeout*len(urls)
      res = self.__gfal_operation_wrapper('gfal_deletesurls',gfalDict)
      gDataStoreClient.addRegister(res['AccountingOperation'])
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend(res['Value'])

    gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __gfalremovedir_wrapper(self,urls):
    """ This is a function that can be reused everywhere to perform the gfal_removedir
    """
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 1
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken
    allResults = []
    failed = {}

    listOfLists = breakListIntoChunks(urls.keys(),self.filesPerCall)
    for urls in listOfLists:
      gfalDict['surls'] = urls
      gfalDict['nbfiles'] =  len(urls)
      gfalDict['timeout'] = self.fileTimeout*len(urls)
      res = self.__gfal_operation_wrapper('gfal_removedir',gfalDict)
      gDataStoreClient.addRegister(res['AccountingOperation'])
      if not res['OK']:
        for url in urls:
          failed[url] = res['Message']
      else:
        allResults.extend(res['Value'])

    gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __gfal_pin_wrapper(self,urls,lifetime):
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 0
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken
    gfalDict['srmv2_desiredpintime'] = lifetime

    allResults = []
    failed = {}

    srmRequestFiles = {}
    for url,srmRequestID in urls.items():
      if not srmRequestFiles.has_key(srmRequestID):
        srmRequestFiles[srmRequestID] = []
      srmRequestFiles[srmRequestID].append(url)

    for srmRequestID,urls in srmRequestFiles.items():
      listOfLists = breakListIntoChunks(urls,self.filesPerCall)
      for urls in listOfLists:
        gfalDict['surls'] = urls
        gfalDict['nbfiles'] =  len(urls)
        gfalDict['timeout'] = self.fileTimeout*len(urls)
        res = self.__gfal_operation_wrapper('gfal_pin',gfalDict,srmRequestID=srmRequestID)
        gDataStoreClient.addRegister(res['AccountingOperation'])
        if not res['OK']:
          for url in urls:
            failed[url] = res['Message']
        else:
          allResults.extend(res['Value'])

    gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __gfal_prestagestatus_wrapper(self,urls):
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 0
    gfalDict['srmv2_spacetokendesc'] = self.spaceToken

    allResults = []
    failed = {}

    srmRequestFiles = {}
    for url,srmRequestID in urls.items():
      if not srmRequestFiles.has_key(srmRequestID):
        srmRequestFiles[srmRequestID] = []
      srmRequestFiles[srmRequestID].append(url)

    for srmRequestID,urls in srmRequestFiles.items():
      listOfLists = breakListIntoChunks(urls,self.filesPerCall)
      for urls in listOfLists:
        gfalDict['surls'] = urls
        gfalDict['nbfiles'] =  len(urls)
        gfalDict['timeout'] = self.fileTimeout*len(urls)
        res = self.__gfal_operation_wrapper('gfal_prestagestatus',gfalDict,srmRequestID=srmRequestID)
        gDataStoreClient.addRegister(res['AccountingOperation'])
        if not res['OK']:
          for url in urls:
            failed[url] = res['Message']
        else:
          allResults.extend(res['Value'])

    gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __gfal_release_wrapper(self,urls):
    gfalDict = {}
    gfalDict['defaultsetype'] = 'srmv2'
    gfalDict['no_bdii_check'] = 0

    allResults = []
    failed = {}

    srmRequestFiles = {}
    for url,srmRequestID in urls.items():
      if not srmRequestFiles.has_key(srmRequestID):
        srmRequestFiles[srmRequestID] = []
      srmRequestFiles[srmRequestID].append(url)

    for srmRequestID,urls in srmRequestFiles.items():
      listOfLists = breakListIntoChunks(urls,self.filesPerCall)
      for urls in listOfLists:
        gfalDict['surls'] = urls
        gfalDict['nbfiles'] =  len(urls)
        gfalDict['timeout'] = self.fileTimeout*len(urls)
        res = self.__gfal_operation_wrapper('gfal_release',gfalDict,srmRequestID=srmRequestID)
        gDataStoreClient.addRegister(res['AccountingOperation'])
        if not res['OK']:
          for url in urls:
            failed[url] = res['Message']
        else:
          allResults.extend(res['Value'])

    gDataStoreClient.commit()
    resDict = {}
    resDict['AllResults'] = allResults
    resDict['Failed'] = failed
    return S_OK(resDict)

  def __gfal_operation_wrapper(self,operation,gfalDict,srmRequestID=None):

    # Create an accounting DataOperation record for each operation
    oDataOperation = self.__initialiseAccountingObject(operation,self.name,gfalDict['nbfiles'])

    res = self.__create_gfal_object(gfalDict)
    if not res['OK']:
      oDataOperation.setValueByKey('TransferOK',0)
      oDataOperation.setValueByKey('FinalStatus','Failed')
      result = S_ERROR(res['Message'])
      result['AccountingOperation'] = oDataOperation
      return result

    gfalObject = res['Value']
    if srmRequestID:
      res = self.__gfal_set_ids(gfalObject,srmRequestID)
      if not res['OK']:
        oDataOperation.setValueByKey('TransferOK',0)
        oDataOperation.setValueByKey('FinalStatus','Failed')
        result = S_ERROR(res['Message'])
        result['AccountingOperation'] = oDataOperation
        return result

    oDataOperation.setStartTime()
    start = time.time()
    res = self.__gfal_exec(gfalObject,operation)
    end = time.time()
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey('TransferTime',end-start)
    if not res['OK']:
      oDataOperation.setValueByKey('TransferOK',0)
      oDataOperation.setValueByKey('FinalStatus','Failed')
      result = S_ERROR(res['Message'])
      result['AccountingOperation'] = oDataOperation
      return result

    gfalObject = res['Value']
    res = self.__gfal_get_ids(gfalObject)
    if not res['OK']:
      newSRMRequestID = srmRequestID
    else:
      newSRMRequestID = res['Value']

    res = self.__get_results(gfalObject)
    if not res['OK']:
      oDataOperation.setValueByKey('TransferOK',0)
      oDataOperation.setValueByKey('FinalStatus','Failed')
      result = S_ERROR(res['Message'])
      result['AccountingOperation'] = oDataOperation
      return result

    resultList = []
    pfnRes = res['Value']
    for dict in pfnRes:
      dict['SRMReqID'] = newSRMRequestID
      resultList.append(dict)

    self.__destroy_gfal_object(gfalObject)
    result = S_OK(resultList)
    result['AccountingOperation'] = oDataOperation
    return result

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

#######################################################################
#
# The following methods provide the interaction with gfal functionality
#
#######################################################################

  # These methods are for the creation of the gfal object

  def __create_gfal_object(self,gfalDict):
    gLogger.debug("SRM2Storage.__create_gfal_object: Performing gfal_init.")
    errCode,gfalObject,errMessage = gfal.gfal_init(gfalDict)
    if not errCode == 0:
      errStr = "SRM2Storage.__create_gfal_object: Failed to perform gfal_init."
      if not errMessage:
        errMessage = os.strerror(errCode)
      gLogger.error(errStr,errMessage)
      return S_ERROR("%s%s" % (errStr,errMessage))
    else:
      gLogger.debug("SRM2Storage.__create_gfal_object: Successfully performed gfal_init.")
      return S_OK(gfalObject)

  def __gfal_set_ids(self,gfalObject,srmRequestID):
    gLogger.debug("SRM2Storage.__gfal_set_ids: Performing gfal_set_ids.")
    errCode,gfalObject,errMessage = gfal.gfal_set_ids(gfalObject,None,0,str(srmRequestID))
    if not errCode == 0:
      errStr = "SRM2Storage.__gfal_set_ids: Failed to perform gfal_set_ids."
      if not errMessage:
        errMessage = os.strerror(errCode)
      gLogger.error(errStr,errMessage)
      return S_ERROR("%s%s" % (errStr,errMessage))
    else:
      gLogger.debug("SRM2Storage.__gfal_set_ids: Successfully performed gfal_set_ids.")
      return S_OK(gfalObject)

  # These methods are for the execution of the functionality

  def __gfal_exec(self,gfalObject,method):
    gLogger.debug("SRM2Storage.__gfal_exec: Performing %s." % method)
    execString = "errCode,gfalObject,errMessage = gfal.%s(gfalObject)" % method
    try:
      exec(execString)
      if not errCode == 0:
        errStr = "SRM2Storage.__gfal_exec: Failed to perform %s." % method
        if not errMessage:
          errMessage = os.strerror(errCode)
        gLogger.error(errStr,errMessage)
        return S_ERROR("%s%s" % (errStr,errMessage))
      else:
        gLogger.debug("SRM2Storage.__gfal_exec: Successfully performed %s." % method)
        return S_OK(gfalObject)
    except AttributeError,errMessage:
      exceptStr = "SRM2Storage.__gfal_exec: Exception while perfoming %s." % method
      gLogger.exception(exceptStr,'',errMessage)
      return S_ERROR("%s%s" % (exceptStr,errMessage))

  # These methods are for retrieving output information

  def __get_results(self,gfalObject):
    gLogger.debug("SRM2Storage.__get_results: Performing gfal_get_results")
    numberOfResults,gfalObject,listOfResults = gfal.gfal_get_results(gfalObject)
    if numberOfResults <= 0:
      errStr = "SRM2Storage.__get_results: Did not obtain results with gfal_get_results."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    else:
      gLogger.debug("SRM2Storage.__get_results: Retrieved %s results from gfal_get_results." % numberOfResults)
      for result in listOfResults:
        if result['status'] != 0:
          if result['explanation']:
            errMessage = result['explanation']
          elif result['status'] > 0:
            errMessage = os.strerror(result['status'])
          result['ErrorMessage'] = errMessage
      return S_OK(listOfResults)

  def __gfal_get_ids(self,gfalObject):
    gLogger.debug("SRM2Storage.__gfal_get_ids: Performing gfal_get_ids.")
    numberOfResults,gfalObject,srm1RequestID,srm1FileIDs,srmRequestToken = gfal.gfal_get_ids(gfalObject)
    if numberOfResults <= 0:
      errStr = "SRM2Storage.__gfal_get_ids: Did not obtain SRM request ID."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    else:
      gLogger.debug("SRM2Storage.__get_gfal_ids: Retrieved SRM request ID %s." % srmRequestToken)
      return S_OK(srmRequestToken)

  # Destroy the gfal object after use

  def __destroy_gfal_object(self,gfalObject):
    gLogger.debug("SRM2Storage.__destroy_gfal_object: Performing gfal_internal_free.")
    gfal.gfal_internal_free(gfalObject)
    return S_OK()

