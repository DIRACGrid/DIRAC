""" Client for ProcessingDB file catalog tables
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Transformation.TransformationDBClient import TransformationDBClient
import types

class ProcessingDBClient:
  """ File catalog client for processing DB
  """

  def __init__(self, url=False, useCertificates=False):
    """ Constructor of the ProcessingDB catalogue client
    """
    self.name = 'ProcessingDB'
    self.valid = True
    try:
      if not url:
        oServer = RPCClient("ProductionManagement/ProcessingDB")
      else:
        oServer = RPCClient(self.url)
      self.setServer(oServer)
    except:
      self.valid = False