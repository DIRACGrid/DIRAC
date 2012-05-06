""" File catalog client for LCG File Catalog proxy service """

from DIRAC                                      import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Client                     import Client

class LcgFileCatalogProxyClient( Client ):
  """ File catalog client for LCG File Catalog proxy service
  """

  def __init__( self, url = False ):
    """ Constructor of the LCGFileCatalogProxy client class
    """
    Client.__init__( self )
    self.name = 'LFCProxy'
    self.valid = False
    self.setServer( 'DataManagement/LcgFileCatalogProxy' )
    if url:
      self.setServer( url )
    self.setTimeout( 120 )
    self.valid = self.ping()['OK']

  def isOK( self ):
    return self.valid

  def getName( self, DN = '' ):
    """ Get the file catalog type name
    """
    return self.name

  def __getattr__( self, name ):
    self.method = name
    return self.execute

  def execute( self, *parms, **kws ):
    """ Magic method dispatcher """
    self.call = callProxyMethod
    return self.executeRPC( self.method, parms, kws )
