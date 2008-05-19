""" Client plug-in for the RAWIntegrity catalogue.
    This exposes a single method to add files to the RAW IntegrityDB.
"""
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase import FileCatalogueBase
import types

class RAWIntegrityClient(FileCatalogueBase):

  def __init__(self):
    try:
      self.url = PathFinder.getServiceURL('DataManagement/RAWIntegrity')
      self.valid = True
    except Exception,x:
      errStr = "RAWIntegrityClient.__init__: Exception while generating server url."
      gLogger.exception(errStr,str(x))
      self.valid = False

  def isOK(self):
    return self.valid

  def exists(self,lfn):
    """ LFN may be a string or list of strings
    """
    if type(lfn) in types.StringTypes:
      lfns = [lfn]
    elif type(lfn) == types.ListType:
      lfns = lfn
    else:
      return S_ERROR('RAWIntegrityClient.exists: Must supply a path or list of paths.')
    successful = {}
    failed = {}
    for lfn in lfns:
      successful[lfn] = False
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def addFile(self, fileTuple):
    """ A tuple should be supplied to this method which contains: (lfn,pfn,size,se,guid)
        A list of tuples may also be supplied.
    """
    if type(fileTuple) == types.TupleType:
      files = [fileTuple]
    elif type(fileTuple) == types.ListType:
      files = fileTuple
    else:
      return S_ERROR('RAWIntegrityClient.addFile: Must supply a file tuple of list of tuples')
    failed = {}
    successful = {}
    for lfn,pfn,size,se,guid,checksum in files:
      server = RPCClient(self.url)
      res = server.addFile(str(lfn),str(pfn),int(size),str(se),str(guid),str(checksum))
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = True
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)
