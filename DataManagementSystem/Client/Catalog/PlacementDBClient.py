""" Client for PlacementDB file catalog tables
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Transformation.TransformationDBClient import TransformationDBClient
import types

class PlacementDBClient(TransformationDBClient):
  """ File catalog client for placement DB
  """
  def __init__(self, url=False, useCertificates=False):
    """ Constructor of the PlacementDB catalogue client
    """
    self.name = 'PlacementDB'
    self.valid = True
    try:
      if not url:
        oServer = RPCClient("DataManagement/PlacementDB")
      else:
        oServer = RPCClient(self.url)
      self.setServer(oServer)
    except:
      self.valid = False
