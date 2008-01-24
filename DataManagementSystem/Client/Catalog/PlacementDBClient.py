""" Client for PlacementDB file catalog tables
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder

class PlacementDBClient:
  """ File catalog client for placement DB
  """

  def __init__(self, url=False, useCertificates=False):
    """ Constructor of the PlacementDB catalogue client
    """
    self.name = 'PlaceDB'
    if not url:
      self.server = RPCClient("DataManagement/PlacementDB",useCertificates,timeout=120)
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
      inputParameter = parms[0]
      if not type(inputParameter) == ListType:
        inputParameters = [inputParameter]
      else:
        inputParameters = inputParameter
      return self.server.callProxyMethod(self.call,parms,kws)
    except Exception,x:
      return S_ERROR("PlacementDBClient.execute: Exception while calling the server.",str(x))
