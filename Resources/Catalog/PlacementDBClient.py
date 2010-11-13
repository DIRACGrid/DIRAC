""" Client for PlacementDB file catalog tables
"""
from DIRAC                                                    import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC.ConfigurationSystem.Client                         import PathFinder
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
import types

class PlacementDBClient(TransformationClient):
  """ File catalog client for placement DB
  """
  def __init__(self, url=False, useCertificates=False):
    """ Constructor of the PlacementDB catalogue client
    """
    self.name = 'PlacementDB'
    self.valid = True
    try:
      if not url:
        url = PathFinder.getServiceURL("DataManagement/PlacementDB")
      self.setServer(url)
    except:
      self.valid = False
