"""
    FileCatalogClientBase is a base class for the clients of file catalog-like
    services built within the DIRAC framework.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client

class FileCatalogClientBase( Client ):
  """ Client code to the DIRAC File Catalogue
  """
  def __init__( self, url = None, **kwargs ):
    """ Constructor function.
    """
    super( FileCatalogClientBase, self ).__init__( **kwargs )
    if url:
      self.serverURL = url
    self.available = False

  def isOK( self, timeout = 120 ):
    """ Check that the service is OK
    """
    if not self.available:
      rpcClient = self._getRPC( timeout = timeout )
      res = rpcClient.isOK()
      if not res['OK']:
        self.available = False
      else:
        self.available = True
    return S_OK( self.available )

#######################################################################################
#  The following methods must be implemented in derived classes
#######################################################################################

  def getInterfaceMethods( self ):
    """ Get the methods implemented by the File Catalog client

    :return tuple: ( read_methods_list, write_methods_list, nolfn_methods_list )
    """
    raise AttributeError( "getInterfaceMethods must be implemented in the FC derived class" )

  def hasCatalogMethod( self, methodName ):
    """ Check of a method with the given name is implemented
    :param str methodName: the name of the method to check
    :return: boolean Flag True if the method is implemented
    """
    raise AttributeError( "hasCatalogMethod must be implemented in the FC derived class" )