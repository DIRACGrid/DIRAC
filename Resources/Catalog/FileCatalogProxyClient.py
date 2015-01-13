########################################################################
# $HeadURL $
# File: FileCatalogProxyClient.py
########################################################################
""" File catalog client for the File Catalog proxy service """

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient import RPCClient

class FileCatalogProxyClient( object ):
  """ File catalog client for the File Catalog proxy service
  """

  def __init__( self, fcName, **kwargs ):
    """ Constructor of the LCGFileCatalogProxy client class
    """
    self.method = None
    self.fcName = fcName
    self.rpc = RPCClient( 'DataManagement/FileCatalogProxy', timeout=120 )
    self.valid = False
    self.valid = self.rpc.ping()['OK']

  def isOK( self ):
    """ Is the Catalog available?
    """
    return self.valid

  def getName( self ):
    """ Get the file catalog name
    """
    return self.fcName

  def __getattr__( self, name ):
    self.method = name
    return self.execute

  def execute( self, *parms, **kws ):
    """ Magic method dispatcher """
    return self.rpc.callProxyMethod( self.fcName, self.method, parms, kws )
