"""
This is a service which represents a DISET proxy to the LCG File Catalog
"""
from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

fileCatalog = False

def initializeLcgFileCatalogProxyHandler(serviceInfo):
  global fileCatalog
  fileCatalog = FileCatalog(['LcgFileCatalogCombined'])
  result = setupShifterProxyInEnv("DataManager")
  if not result[ 'OK' ]:
    self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
    return result

  return S_OK()

class LcgFileCatalogProxyHandler(RequestHandler):

  types_callProxyMethod = [StringType,TupleType,DictionaryType]
  def export_callProxyMethod(self, name, args, kargs):
    """ A generic method to call methods of the LCG FileCatalog client
    """
    try:
      method = getattr(fileCatalog,name)
    except AttributeError, x:
      error = "Exception: no method named "+name
      return S_ERROR(error)

    try:
      result = method(*args,**kargs)
      return result
    except Exception,x:
      print str(x)
      return S_ERROR('Exception while calling method '+name)
