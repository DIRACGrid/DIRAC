""" Client for ProcductionDB file catalog tables
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Transformation.TransformationDBClient import TransformationDBClient
import types

class ProductionDBClient:
  """ File catalog client for production DB
  """

  def __init__(self, url=False, useCertificates=False):
    """ Constructor of the ProductionDB catalogue client
    """
    self.name = 'ProductionDB'
    self.valid = True
    try:
      if not url:
        oServer = RPCClient("ProductionManagement/ProductionManager")
      else:
        oServer = RPCClient(url)
      self.setServer(oServer)
    except:
      self.valid = False
