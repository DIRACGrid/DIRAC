""" Client plug-in for the RAWIntegrity catalogue.
    This exposes a single method to add files to the RAW IntegrityDB.
"""
import DIRAC
from DIRAC                                          import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.ConfigurationSystem.Client               import PathFinder
from DIRAC.Resources.Catalog.FileCatalogueBase      import FileCatalogueBase
import types

class RAWIntegrityClient(FileCatalogueBase):

  def __init__(self):
    try:
      self.url = PathFinder.getServiceURL('DataManagement/RAWIntegrity')
      self.valid = True
    except Exception,x:
      errStr = "RAWIntegrityClient.__init__: Exception while generating server url."
      gLogger.exception(errStr,lException=x)
      self.valid = False

  def isOK(self):
    return self.valid

  def exists(self,lfn):
    """ LFN may be a string or list of strings
    """
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    successful = {}
    failed = {}
    for lfn in lfns.keys():
      successful[lfn] = False
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def addFile(self, lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    failed = {}
    successful = {}
    for lfn,info in res['Value'].items():
      server = RPCClient(self.url,timeout=120)
      pfn = str(info['PFN'])
      size = int(info['Size'])
      se = str(info['SE'])
      guid = str(info['GUID'])
      checksum = str(info['Checksum'])      
      res = server.addFile(lfn,pfn,size,se,guid,checksum)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = True
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

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
      return S_ERROR("RAWIntegrityClient.__checkArgumentFormat: Supplied path is not of the correct format.")
    return S_OK(urls)
  
  def getPathPermissions( self, path ):
    """ Determine the VOMs based ACL information for a supplied path
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    lfns = res['Value']
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      successful[path] = lfn
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )
