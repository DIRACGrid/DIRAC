""" This is the LHCb Online storage """

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.Utilities.File import getSize
from stat import *
import types,re,os,xmlrpclib

ISOK = True

class LHCbOnlineStorage(StorageBase):

  def __init__(self,storageName,protocol,path,host,port,spaceToken,wspath):
    self.isok = ISOK

    self.protocolName = 'LHCbOnline'
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

    serverString = "%s://%s:%s" % (protocol,host,port)
    self.server = xmlrpclib.Server(serverString)

  def isOK(self):
    return self.isok

  def getFile(self,fileTuple):
    """ Tell the Online system that the migration failed and we want to get the request again
    """
    if type(fileTuple) == types.TupleType:
      urls = [fileTuple]
    elif type(fileTuple) == types.ListType:
      urls = fileTuple
    else:
      return S_ERROR("LHCbOnline.getFile: Supplied file information must be tuple of list of tuples")
    successful = {}
    failed = {}
    for lfn,ignored in urls:
      try:
        res = self.server.errorMigratingFile(lfn)
        if res:
          successful[lfn] = True
          gLogger.info("LHCbOnline.getFile: Successfully requested file from Online storage.")
        else:
          errStr = "LHCbOnline.getFile: Failed to request file from Online storage."
          failed[lfn] = errStr
          gLogger.error(errStr,lfn)
      except Exception,x:
        errStr = "LHCbOnline.getFile: Exception while requesting file from Online storage."
        gLogger.exception(errStr,str(x))
        failed[lfn] = errStr
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
      return S_ERROR("LHCbOnline.removeFile: Supplied path must be string or list of strings")
    if not len(path) > 0:
      return S_ERROR("LHCbOnline.removeFile: No surls supplied.")
    successful = {}
    failed = {}
    for lfn,ignored in urls:
      try:
        res = self.server.endMigratingFile(lfn)
        if res:
          successful[lfn] = True
          gLogger.info("LHCbOnline.getFile: Successfully requested file from Online storage.")
        else:
          errStr = "LHCbOnline.getFile: Failed to request file from Online storage."
          failed[lfn] = errStr
          gLogger.error(errStr,lfn)
      except Exception,x:
        errStr = "LHCbOnline.getFile: Exception while requesting file from Online storage."
        gLogger.exception(errStr,str(x))
        failed[lfn] = errStr
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)