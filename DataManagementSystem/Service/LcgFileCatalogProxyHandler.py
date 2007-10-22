"""
This is a service which represents a DISET proxy to the LCG File Catalog
"""
from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.FileCatalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient

lfcCombinedClient = False

def initializeLcgFileCatalogProxyHandler(serviceInfo):
  global lfcCombinedClient
  lfcCombinedClient = LcgFileCatalogCombinedClient()
  return S_OK()

class LcgFileCatalogProxyHandler(RequestHandler):

  types_callProxyMethod = [StringType,ListType,DictionaryType]
  def export_callProxyMethod(self, name, args, kargs):
    """ A generic method to call methods of the LCG FileCatalog client
    """
    try:
      method = getattr(lfcCombinedClient,name)
    except AttributeError, x:
      error = "Exception: no method named "+name
      return S_ERROR(error)

    kargs["DN"] = self.sDN
    try:
      result = method(*args,**kargs)
      return result
    except Exception,x:
      print str(x)
      return S_ERROR('Exception while calling method '+name)
