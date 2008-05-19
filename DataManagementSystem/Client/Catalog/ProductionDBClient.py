""" Client for ProcductionDB file catalog tables
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Transformation.TransformationDBClient import TransformationDBClient
from DIRAC.ConfigurationSystem.Client import PathFinder
import types

class ProductionDBClient(TransformationDBClient):
  """ File catalog client for Production DB
  """

  def __init__(self, url=False, useCertificates=False):
    """ Constructor of the ProductionDB catalogue client
    """
    self.name = 'ProductionDB'
    self.valid = True
    try:
      if not url:
        url = PathFinder.getServiceURL("ProductionManagement/ProductionManager")
      self.setServer(url)
    except:
      self.valid = False
