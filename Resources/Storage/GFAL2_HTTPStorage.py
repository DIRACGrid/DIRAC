""" :mod: GFAL2_HTTPStorage
    =================

    .. module: python
    :synopsis: HTTP module based on the GFAL2_StorageBase class.
"""

from types import StringType, ListType
# from DIRAC
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Utilities import checkArgumentFormat
from UnitTests.HTTP_Test.GFAL2_HTTPStorage import GFAL2_HTTPStorage


class GFAL2_HTTPStorage( GFAL2_StorageBase ):

  """ .. class:: GFAL2_HTTPStorage

  HTTP interface to StorageElement using gfal2
  """

  def __init__( self, storageName, parameters ):
    """ c'tor

    :param self: self reference
    :param str storageName: SE name
    :param str protocol: protocol to use
    :param str rootdir: base path for vo files
    :param str host: SE host
    :param int port: port to use to communicate with :host:
    :param str spaceToken: space token
    :param str wspath: location of HTTP on :host:
    """
    self.log = gLogger.getSubLogger( "GFAL2_HTTPStorage", True )
    # # init base class
    super( GFAL2_HTTPStorage, self ).__init__( storageName, parameters )

    # TODO: test if HTTP can handle checksums.
    self.checksumType = None

#     self.log.setLevel( "DEBUG" )

    self.pluginName = 'GFAL2_HTTP'
    self.protocol = self.protocolParameters['Protocol']
    self.host = self.protocolParameters['Host']


    self.protocolParameters['Port'] = 0
    self.protocolParameters['WSUrl'] = 0
    self.protocolParameters['SpaceToken'] = 0

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

    self.log.debug( 'GFAL2_HTTPStorage.getTransportURL: Attempting to retrieve tURL for %s paths' % len( urls ) )

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
    :returns S_OK( Transport_URL ) in case of success
             S_ERROR( errStr ) in case of a failure
    """
    self.log.debug( 'GFAL2_HTTPStorage.__getSingleTransportURL: trying to retrieve tURL for %s' % path )
    if protocols:
      res = self._getExtendedAttributes( path, protocols )
    else:
      res = self._getExtendedAttributes( path )
    if res['OK']:
      attributeDict = res['Value']
      # 'user.replicas' is the extended attribute we are interested in
      if 'user.replicas' in attributeDict.keys():
        turl = attributeDict['user.replicas']
        return S_OK( turl )
      else:
        errStr = 'GFAL2_HTTPStorage.__getSingleTransportURL: Extended attribute tURL is not set.'
        self.log.debug( errStr )
        return S_ERROR( errStr )
    else:
      errStr = 'GFAL2_HTTPStorage.__getSingleTransportURL: %s' % res['Message']
      return S_ERROR( errStr )



  def __getProtocols( self ):
    """ returns list of protocols to use at a given site

    :warn: priority is given to a protocols list defined in the CS

    :param self: self reference
    """
    sections = gConfig.getSections( '/Resources/StorageElements/%s/' % ( self.name ) )
    self.log.debug( "GFAL2_HTTPStorage.__getProtocols: Trying to get protocols for storage %s." % self.name )
    if not sections['OK']:
      return sections

    protocolsList = []
    for section in sections['Value']:
      path = '/Resources/StorageElements/%s/%s/ProtocolName' % ( self.name, section )
      if gConfig.getValue( path, '' ) == self.protocol:
        protPath = '/Resources/StorageElements/%s/%s/ProtocolsList' % ( self.name, section )
        siteProtocols = gConfig.getValue( protPath, [] )
        if siteProtocols:
          self.log.debug( 'GFAL2_HTTPStorage.__getProtocols: Found SE protocols list to override defaults:', ', '.join( siteProtocols, ) )
          protocolsList = siteProtocols

    if not protocolsList:
      self.log.debug( "GFAL2_HTTPStorage.__getProtocols: No protocols provided, using the default protocols." )
      protocolsList = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )
      self.log.debug( 'GFAL2_HTTPStorage.__getProtocols: protocolList = %s' % protocolsList )

    # if there is even no default protocol
    if not protocolsList:
      return S_ERROR( "GFAL2_HTTPStorage.__getProtocols: No local protocols defined and no defaults found." )

    return S_OK( protocolsList )
