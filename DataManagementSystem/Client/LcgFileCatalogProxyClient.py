""" File catalog client for LCG File Catalog proxy service
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

class LcgFileCatalogProxyClient:
  """ File catalog client for LCG File Catalog proxy service
  """

  def __init__(self, url = False, useCertificates = False):
    """ Constructor of the LCGFileCatalogProxy client class
    """
    self.name = 'LFCProxy'
    if not url:
      result = gConfig.getOption('/DIRAC/Setup')
      if not result['OK']:
        gLogger.fatal('Failed to get the /DIRAC/Setup')
        return
      setup = result['Value']
      configPath = '/DIRAC/Setups/%s/DataManagement' % setup

      dmConfig = gConfig.getValue(configPath)
      configPath = '/Systems/DataManagement/%s/URLs/LcgFileCatalog/LcgFileCatalogProxy' % dmConfig
      self.url = gConfig.getValue(configPath)
    else:
      self.url = url
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
      result = self.server.callProxyMethod(self.call,parms,kws)
    except Exception,x:
      return S_ERROR('Exception while calling the server '+str(x))
    return result
