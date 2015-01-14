from types import StringType, ListType
import errno
import gfal2
# from DIRAC
from DIRAC.Resources.Storage.GFAL2StorageBase import GFAL2StorageBase
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Utilities import checkArgumentFormat




__RCSID__ = "$Id$"

class SRM2V2Storage( GFAL2StorageBase ):
  """ SRM2 SE class that inherits from GFAL2StorageBase
  """
  
  def __init__( self, storageName, parameters ):
    """ """
    self.log = gLogger.getSubLogger( "SRM2V2Storage", True )
    self.log.debug( "SRM2V2Storage.__init__: Initializing object" )
    GFAL2StorageBase.__init__( self, storageName, parameters )
    self.pluginName = 'SRM2V2'

    # ##
    #    Setting the default SRM parameters here. For methods where this
    #    is not the default there is a method defined in this class, setting
    #    the proper values and then calling the base class method.
    # ##
    self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
    self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
    self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", self.defaultLocalProtocols )


  def _getExtendedAttributes( self, path, protocols = False ):
    if protocols:
      self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", protocols )
    return GFAL2StorageBase._getExtendedAttributes( self, path, protocols = protocols )



  def getTransportURL( self, path, protocols = False ):
    """ obtain the tURLs for the supplied path and protocols

    :param self: self reference
    :param str path: path on storage
    :param mixed protocols: protocols to use
    :returns Failed dict {path : error message}
             Successful dict {path : transport url}
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( 'SRM2V2Storage.getTransportURL: Attempting to retrieve tURL for %s paths' % len( urls ) )

    failed = {}
    successful = {}

    if not protocols:
      protocols = self.__getProtocols()
      if not protocols['OK']:
        return protocols
      listProtocols = protocols['Value']
    elif type( protocols ) == StringType:
      listProtocols = [protocols]
    elif type( protocols ) == ListType:
      listProtocols = protocols
    else:
      return S_ERROR( "getTransportURL: Must supply desired protocols to this plug-in." )

    for url in urls:
      res = self.__getSingleTransportURL( url, listProtocols )
      self.log.debug( 'res = %s' % res )

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )



  def __getSingleTransportURL( self, path, protocols = False ):
    """ Get the tURL from path with getxattr from gfal2

    :param self: self reference
    :param str path: path on the storage
    :returns S_OK( Transport_URL ) in case of success
             S_ERROR( errStr ) in case of a failure
    """
    self.log.debug( 'SRM2V2Storage.__getSingleTransportURL: trying to retrieve tURL for %s' % path )
    if protocols:
      res = self.__getExtendedAttributes( path, protocols )
    else:
      res = self.__getExtendedAttributes( path )
    if res['OK']:
      attributeDict = res['Value']
      # 'user.replicas' is the extended attribute we are interested in
      if 'user.replicas' in attributeDict.keys():
        turl = attributeDict['user.replicas']
        return S_OK( turl )
      else:
        errStr = 'SRM2V2Storage.__getSingleTransportURL: Extended attribute tURL is not set.'
        self.log.debug( errStr )
        return S_ERROR( errStr )
    else:
      errStr = 'SRM2V2Storage.__getSingleTransportURL: %s' % res['Message']
      return S_ERROR( errStr )



  def __getProtocols( self ):
    """ returns list of protocols to use at a given site

    :warn: priority is given to a protocols list defined in the CS

    :param self: self reference
    """
    sections = gConfig.getSections( '/Resources/StorageElements/%s/' % ( self.name ) )
    self.log.debug( "SRM2V2Storage.__getProtocols: Trying to get protocols for storage %s." % self.name )
    if not sections['OK']:
      return sections

    protocolsList = []
    for section in sections['Value']:
      path = '/Resources/StorageElements/%s/%s/ProtocolName' % ( self.name, section )
      if gConfig.getValue( path, '' ) == self.protocol:
        protPath = '/Resources/StorageElements/%s/%s/ProtocolsList' % ( self.name, section )
        siteProtocols = gConfig.getValue( protPath, [] )
        if siteProtocols:
          self.log.debug( 'SRM2V2Storage.__getProtocols: Found SE protocols list to override defaults:', ', '.join( siteProtocols, ) )
          protocolsList = siteProtocols

    if not protocolsList:
      self.log.debug( "SRM2V2Storage.__getProtocols: No protocols provided, using the default protocols." )
      protocolsList = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )

    # if there is even no default protocol
    if not protocolsList:
      return S_ERROR( "SRM2V2Storage.__getProtocols: No local protocols defined and no defaults found." )

    return S_OK( protocolsList )

  def __getExtendedAttributes( self, path, protocols = False ):
    """ Get all the available extended attributes of path

    :param self: self reference
    :param str path: path of which we wan't extended attributes
    :return S_OK( attributeDict ) if successful. Where the keys of the dict are the attributes and values the respective values
    """
    attributeDict = {}

    # get all the extended attributes from path
    try:
      self.gfal2.set_opt_boolean( "BDII", "ENABLE", False )
      self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
      self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
      if protocols:
        self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", protocols )
      else:
        self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", self.defaultLocalProtocols )
      attributes = self.gfal2.listxattr( path )

      # get all the respective values of the extended attributes of path
      for attribute in attributes:
        attributeDict[attribute] = self.gfal2.getxattr( path, attribute )

      return S_OK( attributeDict )
    # simple error messages, the method that is calling them adds the source of error.
    except gfal2.GError, e:
      if e.code == errno.ENOENT:
        errStr = 'Path does not exist.'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )
      else:
        errStr = 'Something went wrong while checking for extended attributes. Please see error log for more information.'
        self.log.error( errStr, e.message )
        return S_ERROR( errStr )


