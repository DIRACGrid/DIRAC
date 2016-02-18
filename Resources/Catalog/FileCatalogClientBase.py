"""
    FileCatalogClientBase is a base class for the clients of file catalog-like
    services built within the DIRAC framework.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Utilities.ReturnValues import S_OK
from DIRAC.Resources.Catalog.Utilities import checkCatalogArguments

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

  @staticmethod
  def getInterfaceMethods():
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

########################################################################################
#  Some default methods to always return successfully if we do not care
########################################################################################
  @checkCatalogArguments
  def hasAccess( self, paths, _opType ):
    """ Default method: returns True for all paths and all actions

      :param lfn paths: has to be formatted this way :
                  { lfn : { se1 : pfn1, se2 : pfn2, ...}, ...}
      :param str _opType: dummy string

      :return: { successful : { lfn : [ ses ] } : failed : { lfn : { se : msg } } }
    """
    lfns = paths.keys()
    return S_OK( {'Failed' : {}, 'Successful' : dict.fromkeys( lfns, True )} )

  @checkCatalogArguments
  def exists( self, lfns ):
    """ Default method: returns False for all paths

      :param lfn paths: has to be formatted this way :
                  { lfn : { se1 : pfn1, se2 : pfn2, ...}, ...}

      :return: { successful : { lfn : [ ses ] } : failed : { lfn : { se : msg } } }

    """
    return S_OK( {'Failed' : {}, 'Successful' : dict.fromkeys( lfns, False )} )

  @checkCatalogArguments
  def getPathPermissions( self, lfns ):
    """ Default method: returns Read & Write permission for all the paths

      :param lfn paths: has to be formatted this way :
                  { lfn : { se1 : pfn1, se2 : pfn2, ...}, ...}

      :return: { successful : { lfn : [ ses ] } : failed : { lfn : { se : msg } } }

    """
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      successful[lfn] = { 'Write': True, "Read": True }
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )