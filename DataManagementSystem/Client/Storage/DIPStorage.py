########################################################################
# $Id: DIPStorage.py,v 1.8 2009/02/02 21:46:56 acsmith Exp $
########################################################################

""" DIPStorage class is the client of the DIRAC Storage Element.

    The following methods are available in the Service interface

    getMetadata()
    get()
    getDir()
    put()
    putDir()
    remove()

"""

__RCSID__ = "$Id: DIPStorage.py,v 1.8 2009/02/02 21:46:56 acsmith Exp $"

from DIRAC.DataManagementSystem.Client.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import getSize
import re,os,types

class DIPStorage(StorageBase):

  def __init__(self,storageName,protocol,path,host,port,spaceToken,wspath):
    """
    """

    self.protocolName = 'DIP'
    self.name = storageName
    self.protocol = protocol
    self.path = path
    self.host = host
    self.port = port
    self.wspath = wspath
    self.spaceToken = spaceToken

    self.url = protocol+"://"+host+":"+port+path

    self.cwd = ''
    self.isok = True

  ################################################################################
  #
  # The methods below are for manipulating the client
  #

  def isOK(self):
    return self.isok

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
      errStr = "Failed to create URL %s" % x
      return S_ERROR(errStr)

  def getProtocolPfn(self,pfnDict,withPort):
    """ From the pfn dict construct the pfn to be used
    """
    pfnDict['Protocol'] = ''
    pfnDict['Host'] = ''
    pfnDict['Port'] = ''
    pfnDict['WSUrl'] = ''
    res = pfnunparse(pfnDict)
    return res

  #############################################################
  #
  # These are the methods for file manipulation
  #

  def exists(self,path):
    """ Check if the given path exists. The 'path' variable can be a string or a list of strings.
    """
    res = self.__checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    serviceClient = RPCClient(self.url)
    for url in urls:
      gLogger.debug("DIPStorage.exists: Determining existence of %s." % url)
      res = serviceClient.exists(url)
      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def putFile(self,path,sourceSize=0):
    """Put a file to the physical storage
    """
    res = self.__checkArgumentFormatDict(path)
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    transferClient = TransferClient(self.url)
    for dest_url,src_file in urls.items()
      gLogger.debug("DIPStorage.putFile: Executing transfer of %s to %s" % (src_file, dest_url))
      res = self.__putFile(src_file,dest_url)
      if res['OK']:
        successful[dest_url] = res['Value']
      else:
        failed[dest_url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __putFile(self,src_file,dest_url):
    if not os.path.exists(src_file):
      errStr = "DIPStorage.__putFile: The source local file does not exist."
      gLogger.error(errStr,src_file)
      return S_ERROR(errStr)
    sourceSize = getSize(src_file)
    if sourceSize == -1:
      errStr = "DIPStorage.__putFile: Failed to get file size."
      gLogger.error(errStr,src_file)
      return S_ERROR(errStr)
    res = transferClient.sendFile(src_file,dest_url)
    if res['OK']:
      return S_OK(sourceSize)
    else:
      return res

  def getFile(self,fileTuple):
    """Get a local copy in the current directory of a physical file specified by its path
    """
    res = self.__checkArgumentFormatDict(path)
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    transferClient = TransferClient(self.url)
    for src_url,dest_file in urls.items():
      gLogger.debug("DIPStorage.putFile: Executing transfer of %s to %s" % (src_url, dest_file))
      res = self.__getFile(src_url,dest_file)
      if res['OK']:
        successful[src_url] = res['Value']
      else:
        failed[src_url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __getFile(self,src_url,dest_file)
    res = transferClient.receiveFile(src_url,dest_file)
    if not res['OK']:
      return res
    if not os.path.exists(dest_file):
      errStr = "DIPStorage.__getFile: The destination local file does not exist."
      gLogger.error(errStr,dest_file)  
      return S_ERROR(errStr)
    destSize = getSize(src_file)
    if destSize == -1:
      errStr = "DIPStorage.__getFile: Failed to get the local file size."
      gLogger.error(errStr,dest_file)
      return S_ERROR(errStr)
    return S_OK(destSize)

  def removeFile(self,path):
    """Remove physically the file specified by its path
    """
    res = self.__checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value']
    if not len(urls) > 0:
      return S_ERROR("DIPStorage.removeFile: No surls supplied.")
    successful = {}
    failed = {}
    serviceClient = RPCClient(self.url)
    for url in urls:
      gLogger.debug("DIPStorage.removeFile: Attempting to remove %s." % url)
      res = serviceClient.remove(url,'')
      if res['OK']:
        successful[url] = True
      else:
        failed[url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileMetadata(self,path):
    """  Get metadata associated to the file
    """
    res = self.__checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value'] 
    successful = {}
    failed = {}
    gLogger.debug("DIPStorage.getFileMetadata: Attempting to obtain metadata for %s files." % len(urls))
    serviceClient = RPCClient(self.url)
    for url in urls:
      res = serviceClient.getMetadata(url)
      if res['OK']:
        gLogger.debug("DIPStorage.getFileMetadata: Successfully obtained metadata for %s." % url)
        successful[url] = res['Value']
      else:
        gLogger.error("DIPStorage.getFileMetadata: Failed to get metdata for %s." % url,res['Message'])
        failed[url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  def createDirectory(self,path):
    """ Create the remote directory
    """
    res = self.__checkArgumentFormat(path)
    if not res['OK']:
      return res
    urls = res['Value'] 
    successful = {}
    failed = {}
    gLogger.debug("DIPStorage.createDirectory: Attempting to create %s directories." % len(urls))
    serviceClient = RPCClient(self.url)
    for url in urls:
      strippedUrl = url.rstrip('/')
      res = serviceClient.createDirectory(url)
      if res['OK']:
        gLogger.debug("DIPStorage.createDirectory: Successfully created directory on storage: %s" % url)
        successful[url] = True
      else:
        gLogger.error("DIPStorage.createDirectory: Failed to create directory on storage.", "%s: %s" % (url,res['Message']))
        failed[url] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def putDirectory(self, directoryTuple):
    """ Put a local directory to the physical storage together with all its files and subdirectories.
    """
    res = self.__checkArgumentFormatDict(path)
    if not res['OK']:
      return res
    urls = res['Value']
    successful = {}
    failed = {}
    gLogger.debug("DIPStorage.putDirectory: Attemping to put %s directories to remote storage." % len(urls))
    transferClient = TransferClient(self.url)
    for destDir,sourceDir in urls.items():
      res = transferClient.sendBulk([sourceDir],destDir)
      if res['OK']:
        successful[destDir] = True
      else:
         failed[destDir] = res['Message']
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDir(self,dname):
    """ Get file directory dname from the storage
    """
    return S_OK()

  def __checkArgumentFormat(self,path):
    if type(path) in types.StringTypes:
      urls = [path]
    elif type(path) == types.ListType:
      urls = path
    elif type(path) == types.DictType:
      urls = path.keys()
    else:
      return S_ERROR("RFIOStorage.__checkArgumentFormat: Supplied path is not of the correct format.")
    return S_OK(urls)

  def __checkArgumentFormatDict(self,path):   
    if type(path) in types.StringTypes:
      urls = {path:False}
    elif type(path) == types.ListType:
      urls = {}
      for url in path:
        urls[url] = False
    elif type(path) == types.DictType:
     urls = path
    else:
      return S_ERROR("RFIOStorage.checkArgumentFormat: Supplied path is not of the correct format.")
    return S_OK(urls)

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
      exceptStr = "RFIOStorage.__executeOperation: Exception while perfoming %s." % method
      gLogger.exception(exceptStr,'',errMessage)
      return S_ERROR("%s%s" % (exceptStr,errMessage))   

