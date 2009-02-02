""" File catalog client for LCG File Catalog proxy service
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder

class LcgFileCatalogProxyClient:
  """ File catalog client for LCG File Catalog proxy service
  """

  def __init__(self, url = False, useCertificates = False):
    """ Constructor of the LCGFileCatalogProxy client class
    """
    self.name = 'LFCProxy'
    self.valid = False
    try:
      if url:
        self.url = url
      else:
        url = PathFinder.getServiceURL('DataManagement/LcgFileCatalogProxy')
        if not url:
          return
        self.url = url
      self.server  = RPCClient(self.url,timeout=120,useCertificates=useCertificates)
      if not self.server: 
        return
      else:
        self.valid = True
    except Exception,x:
      gLogger.exception('Exception while creating connection to LcgFileCatalog proxy server','',x)
      return

  def isOK(self):
    return self.valid

  def getName(self,DN=''):
    """ Get the file catalog type name
    """
    return self.name

  def __getattr__(self, name):
    self.call = name
    return self.execute

  def execute(self, *parms, **kws):
    """ Magic method dispatcher """
    try:
      result = self.server.callProxyMethod(self.call,parms,kws)
    except Exception,x:
      return S_ERROR('Exception while calling the server '+str(x))
    return result
