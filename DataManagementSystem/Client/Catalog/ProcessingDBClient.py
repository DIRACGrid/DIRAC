""" Client for ProcessingDB file catalog tables
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder

class ProcessingDBClient:
  """ File catalog client for processing DB
  """

  def __init__(self, url=False, useCertificates=False):
    """ Constructor of the ProcessingDB catalogue client
    """
    self.name = 'ProcDB'
    if not url:
      self.server = RPCClient("ProductionManagement/ProcessingDB",useCertificates,timeout=120)
    else:
      self.server = RPCClient(self.url,useCertificates,timeout = 120)

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
      return self.server.callProxyMethod(self.call,parms,kws)
    except Exception,x:
      return S_ERROR("ProcessingDBClient.execute: Exception while calling the server.",str(x))
