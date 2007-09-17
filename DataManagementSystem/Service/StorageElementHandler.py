########################################################################
# $Id: StorageElementHandler.py,v 1.1 2007/09/17 20:54:08 atsareg Exp $
########################################################################

""" StorageElementHandler is the implementation of a simple StorageElement
    service in the DISET framework

    The following methods are available in the Service interface

    getMetadata()
    get()
    put()

"""

__RCSID__ = "$Id: StorageElementHandler.py,v 1.1 2007/09/17 20:54:08 atsareg Exp $"

import os
from stat import *
from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Utilities.Os import getDiskSpace

base_path = ''
use_tokens_flag = False

def initializeStorageElementHandler(serviceInfo):

  global base_path
  global use_tokens_flag
  cfgPath = serviceInfo['serviceSectionPath']
  result = gConfig.getOption( "%s/BasePath" % cfgPath )
  if result['OK']:
    base_path =  result['Value']
  else:
    gLogger.error('Failed to get the base path')
    return S_ERROR('Failed to get the base path')
  result = gConfig.getOption( "%s/UseTokens" % cfgPath )
  if result['OK']:
    use_tokens_flag =  result['Value']
  gLogger.info('Starting DIRAC Storage Element')
  gLogger.info('Base Path: %s' % base_path)
  gLogger.info('Use access control tokens: ' + str(use_tokens_flag))
  return S_OK()

class StorageElementHandler(RequestHandler):

  def __confirmToken(self,token,path,mode):
    """ Confirm the access rights for the path in a given mode
    """

    return True

  def __checkForDiskSpace(self,dpath,size):
    """ Check if the directory dpath can accomodate 'size' volume of data
    """

    dsize = (getDiskSpace(dpath)-1)*1024
    return (dsize > size)

  types_getMetadata = [StringType]
  def export_getMetadata(self,fileID):
    """ Get metadata for the file or directory specified by fileID
    """

    file_path = base_path+fileID
    resultDict = {}
    try:
      statTuple = os.stat(file_path)
    except OSError, x:
      if str(x).find('No such file') >= 0:
        resultDict['Exists'] = False
        return S_OK(resultDict)
      else:
        return S_ERROR('Failed to get metadata for %s' % file_path)

    resultDict['Exists'] = True
    mode = statTuple[ST_MODE]
    resultDict['Type'] = "File"
    if S_ISDIR(mode):
      resultDict['Type'] = "Directory"
    resultDict['Size'] = statTuple[ST_SIZE]
    resultDict['TimeStamps'] = (statTuple[ST_ATIME],statTuple[ST_MTIME],statTuple[ST_CTIME])

    return S_OK(resultDict)

  def transfer_fromClient( self, fileID, token, fileSize, fileHelper ):
    """ Method to receive file from clients.
        fileID is the local file name in the SE.
        fileSize can be Xbytes or -1 if unknown.
        token is used for access rights confirmation.
    """

    if not self.__checkDiskSpace(base_path,fileSize):
      return S_ERROR('Not enough disk space')

    file_path = base_path+fileID
    if not os.path.exists(os.path.dirname(file_path)):
      os.makedirs(os.path.dirname(file_path))
    result = fileHelper.getFileDescriptor(file_path,'w')
    if not result['OK']:
      return S_ERROR('Failed to get file descriptor')

    fileDescriptor = result['Value']
    result = fileHelper.networkToFD(fileDescriptor)
    if not result['OK']:
      return S_ERROR('Failed to put file '+fileID)
    else:
      return result

  def transfer_toClient( self, fileID, token, fileHelper ):
    """ Method to send files to clients.
        fileID is the local file name in the SE.
        token is used for access rights confirmation.
    """

    file_path = base_path+'/'+fileID
    result = fileHelper.getFileDescriptor(file_path,'r')
    if not result['OK']:
      return S_ERROR('Failed to get file descriptor')

    fileDescriptor = result['Value']
    result = fileHelper.FDToNetwork(fileDescriptor)
    print result
    if not result['OK']:
      return S_ERROR('Failed to get file '+fileID)
    else:
      return result

  def transfer_bulkFromClient( self, fileID, token, fileHelper ):
    """ Receive a directory with files.
        token is used for access rights confirmation.
    """

    print fileID, token
    dirName = fileID.replace('.bz2','').replace('.tar','')
    dir_path = os.path.dirname(base_path+'/'+dirName)
    result = fileHelper.networkToBulk(dir_path)
    print result

    return S_OK()

  def transfer_bulkToClient( self, fileID, token, fileSize, fileHelper ):
    """ Send a directory with files.
        token is used for access rights confirmation.
    """

    return S_OK('Not yet implemented')

  types_remove = [StringType,StringType]
  def export_remove(self,fileID,token):
    """ Remove fileID from the storage.
        token is used for access rights confirmation.
    """

    file_path = base_path+'/'+fileID
    if self.__confirmToken(token,fileID,'x'):
      try:
        os.remove(file_path)
        return S_OK()
      except OSError, x:
        if str(x).find('No such file') >= 0:
          # File does not exist anyway
          return S_OK()
        else:
          return S_ERROR('Failed to remove file %s' % fileID)
    else:
      return S_ERROR('Removal of %s not authorized' % fileID)



