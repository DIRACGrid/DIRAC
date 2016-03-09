""" :mod: GFAL2_SRM2Storage
    =================

    .. module: python
    :synopsis: SRM2 module based on the GFAL2_StorageBase class.
"""

# from DIRAC
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat


__RCSID__ = "$Id$"

class GFAL2_SRM2Storage( GFAL2_StorageBase ):
  """ SRM2 SE class that inherits from GFAL2StorageBase
  """

  def __init__( self, storageName, parameters ):
    """ """
    super( GFAL2_SRM2Storage, self ).__init__( storageName, parameters )
    self.log = gLogger.getSubLogger( "GFAL2_SRM2Storage", True )
    self.log.debug( "GFAL2_SRM2Storage.__init__: Initializing object" )
    self.pluginName = 'GFAL2_SRM2'

    # ##
    #    Setting the default SRM parameters here. For methods where this
    #    is not the default there is a method defined in this class, setting
    #    the proper values and then calling the base class method.
    # ##

    self.gfal2requestLifetime = gConfig.getValue( '/Resources/StorageElements/RequestLifeTime', 100 )

    self.__setSRMOptionsToDefault()

    if self.checksumType:
      self.gfal2.set_opt_string( "SRM PLUGIN", "COPY_CHECKSUM_TYPE", self.checksumType )


  def __setSRMOptionsToDefault( self ):
    ''' Resetting the SRM options back to default

    '''
    self.gfal2.set_opt_integer( "SRM PLUGIN", "OPERATION_TIMEOUT", self.gfal2Timeout )
    self.gfal2.set_opt_string( "SRM PLUGIN", "SPACETOKENDESC", self.spaceToken )
    self.gfal2.set_opt_integer( "SRM PLUGIN", "REQUEST_LIFETIME", self.gfal2requestLifetime )
    # Setting the TURL protocol to gsiftp because with other protocols we have authorisation problems
#    self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", self.defaultLocalProtocols )
    self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", ['gsiftp'] )


  def _getExtendedAttributes( self, path, protocols = False, attributes = None ):
    ''' Changing the TURL_PROTOCOLS option for SRM in case we ask for a specific
        protocol

        :param self: self reference
        :param str path: path on the storage
        :param str protocols: a list of protocols
        :param list attributes: a list of extended attributes that we are interested in,
                                default is None so we retrieve the list with listxattr via
                                gfal2 and get them all.
        :return: S_OK( attributeDict ) if successful. Where the keys of the dict are the attributes
                                      and values the respective values
    '''
    if protocols:
      self.gfal2.set_opt_string_list( "SRM PLUGIN", "TURL_PROTOCOLS", protocols )
    res = super( GFAL2_SRM2Storage, self )._getExtendedAttributes( path, attributes = attributes )
    self.__setSRMOptionsToDefault()
    return res

  def _updateMetadataDict( self, metadataDict, attributeDict ):
    """ Updating the metadata dictionary with srm specific attributes

    :param self: self reference
    :param dict: metadataDict we want add the SRM specific attributes to
    :param dict: attributeDict contains 'user.status' which we then fill in the metadataDict

    """
    # 'user.status' is the extended attribute we are interested in
    user_status = attributeDict.get( 'user.status', '' )
    metadataDict['Cached'] = int( 'ONLINE' in user_status )
    metadataDict['Migrated'] = int( 'NEARLINE' in user_status )
    metadataDict['Lost'] = int( user_status == 'LOST' )
    metadataDict['Unavailable'] = int( user_status == 'UNAVAILABLE' )
    metadataDict['Accessible'] = not metadataDict['Lost'] and metadataDict['Cached'] and not metadataDict['Unavailable']

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

    self.log.debug( 'GFAL2_SRM2Storage.getTransportURL: Attempting to retrieve tURL for %s paths' % len( urls ) )

    failed = {}
    successful = {}
    if not protocols:
      protocols = self.__getProtocols()
      if not protocols['OK']:
        return protocols
      listProtocols = protocols['Value']
    elif isinstance( protocols, basestring ):
      listProtocols = [protocols]
    elif isinstance( protocols, list ):
      listProtocols = protocols
    else:
      return S_ERROR( "getTransportURL: Must supply desired protocols to this plug-in." )


    # I doubt this can happen... 'srm' is not in the listProtocols,
    # it is normally, gsiftp, root, etc
    if self.protocolParameters['Protocol'] in listProtocols:
      successful = {}
      failed = {}
      for url in urls:
        if self.isURL( url )['Value']:
          successful[url] = url
        else:
          failed[url] = 'getTransportURL: Failed to obtain turls.'

      return S_OK( {'Successful' : successful, 'Failed' : failed} )

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
    :returns: S_OK( Transport_URL ) in case of success
              S_ERROR( errStr ) in case of a failure
    """
    self.log.debug( 'GFAL2_SRM2Storage.__getSingleTransportURL: trying to retrieve tURL for %s' % path )
    res = self._getExtendedAttributes( path, protocols = protocols, attributes = ['user.replicas'] )
    if res['OK']:
      attributeDict = res['Value']
      # 'user.replicas' is the extended attribute we are interested in
      # It should always be in it I guess !
      if 'user.replicas' in attributeDict:
        turl = attributeDict['user.replicas']
        return S_OK( turl )
      else:
        errStr = 'GFAL2_SRM2Storage.__getSingleTransportURL: Extended attribute tURL is not set.'
        self.log.debug( errStr )
        return S_ERROR( errStr )
    else:
      return res



  def __getProtocols( self ):
    """ returns list of protocols to use at a given site

    :warn: priority is given to a protocols list defined in the CS

    :param self: self reference
    """
    sections = gConfig.getSections( '/Resources/StorageElements/%s/' % ( self.name ) )
    self.log.debug( "GFAL2_SRM2Storage.__getProtocols: Trying to get protocols for storage %s." % self.name )
    if not sections['OK']:
      return sections

    protocolsList = []
    for section in sections['Value']:
      path = '/Resources/StorageElements/%s/%s/PluginName' % ( self.name, section )
      if gConfig.getValue( path, '' ) == self.pluginName:
        protPath = '/Resources/StorageElements/%s/%s/ProtocolsList' % ( self.name, section )
        siteProtocols = gConfig.getValue( protPath, [] )
        if siteProtocols:
          self.log.debug( 'GFAL2_SRM2Storage.__getProtocols: Found SE protocols list to override defaults:', ', '.join( siteProtocols, ) )
          protocolsList = siteProtocols

    if not protocolsList:
      self.log.debug( "GFAL2_SRM2Storage.__getProtocols: No protocols provided, using the default protocols." )
      protocolsList = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )
      self.log.debug( 'GFAL2_SRM2Storage.__getProtocols: protocolList = %s' % protocolsList )

    # if there is even no default protocol
    if not protocolsList:
      return S_ERROR( "GFAL2_SRM2Storage.__getProtocols: No local protocols defined and no defaults found." )

    return S_OK( protocolsList )



