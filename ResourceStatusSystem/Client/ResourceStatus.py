""" ResourceStatus

  Module use to switch between the CS and the RSS.

"""

import datetime
import math
from time import sleep

from DIRAC                                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton                    import DIRACSingleton
from DIRAC.ConfigurationSystem.Client.CSAPI                 import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities.RSSCacheNoThread  import RSSCache
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration  import RssConfiguration
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter        import InfoGetter
from DIRAC.Core.Utilities                                   import DErrno

class ResourceStatus( object ):
  """
  ResourceStatus helper that connects to CS if RSS flag is not Active. It keeps
  the connection to the db / server as an object member, to avoid creating a new
  one massively.
  """

  __metaclass__ = DIRACSingleton

  def __init__( self ):
    """
    Constructor, initializes the rssClient.
    """

    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self.rssConfig = RssConfiguration()
    self.__opHelper = Operations()
    self.rssClient = None
    self.infoGetter = InfoGetter()

    # We can set CacheLifetime and CacheHistory from CS, so that we can tune them.
    cacheLifeTime = int( self.rssConfig.getConfigCache() )

    # RSSCache only affects the calls directed to RSS, if using the CS it is not
    # used.
    self.seCache = RSSCache( 'StorageElement', cacheLifeTime, self.__updateSECache )
    self.ceCache = RSSCache( 'ComputingElement', cacheLifeTime, self.__updateCECache )
    self.catalogCache = RSSCache( 'Catalog', cacheLifeTime, self.__updateCatalogCache )
    self.ftsCache = RSSCache( 'FTS', cacheLifeTime, self.__updateFTSCache )

  def getComputingElementStatus( self, elementName, statusType = None, default = None ):
    """
    Helper with dual access, tries to get information from the RSS for the given
    ComputingElement.


    example:
      >>> getComputingElementStatus( 'CERN-USER' )
          S_OK( { 'CERN-USER' : { 'all': 'Active' } } )
      >>> getComputingElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' )
          S_ERROR( ... )

    """

    if self.__getMode():
      # We do not apply defaults. If is not on the cache, S_ERROR is returned.
      return self.__getRSSComputingElementStatus( elementName, statusType )
    else:
      return S_OK( { 'all' : 'Active' } )

  def setComputingElementStatus( self, elementName, statusType, status, reason = None,
                               tokenOwner = None ):

    """
    Helper with dual access, tries set information in RSS.

    example:
      >>> setComputingElementStatus( 'CERN-USER', 'all' )
          S_OK( ... )
      >>> setComputingElementStatus( None )
          S_ERROR( ... )
    """

    if self.__getMode():
      return self.__setRSSComputingElementStatus( elementName, statusType, status, reason, tokenOwner )
    else:
      return S_OK()


  def getFTSStatus( self, elementName, statusType = None, default = None ):
    """
    Helper with dual access, tries to get information from the RSS for the given FTS.

    example:
      >>> getFTSStatus( 'CERN-USER' )
          S_OK( { 'CERN-USER' : { 'all': 'Active' } } )
      >>> getFTSStatus( 'CERN-USER', 'ThisIsAWrongStatusType' )
          S_ERROR( ... )

    """

    if self.__getMode():
      # We do not apply defaults. If is not on the cache, S_ERROR is returned.
      return self.__getRSSftsStatus( elementName, statusType )
    else:
      return S_OK( { 'all' : 'Active' } )

  def setFTSStatus( self, elementName, statusType, status, reason = None,
                               tokenOwner = None ):

    """
    Helper with dual access, tries set information in RSS.

    example:
      >>> setFTSStatus( 'CERN-USER', 'all' )
          S_OK( ... )
      >>> setFTSStatus( None )
          S_ERROR( ... )
    """

    if self.__getMode():
      return self.__setRSSftsStatus( elementName, statusType, status, reason, tokenOwner )
    else:
      return S_OK()


  def getCatalogStatus( self, elementName, statusType = None, default = None ):
    """
    Helper with dual access, tries to get information from the RSS for the given
    Catalog, otherwise, it gets it from the CS.

    example:
      >>> getStorageElementStatus( 'CERN-USER', 'ReadAccess' )
          S_OK( { 'CERN-USER' : { 'ReadAccess': 'Active' } } )
      >>> getStorageElementStatus( 'CERN-USER', 'WriteAccess' )
          S_OK( { 'CERN-USER' : {'ReadAccess': 'Active', 'WriteAccess': 'Active',
                                 'CheckAccess': 'Banned', 'RemoveAccess': 'Banned'}} )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' )
          S_ERROR( xyz.. )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType', 'Unknown' )
          S_OK( 'Unknown' )

    """

    if self.__getMode():
      # We do not apply defaults. If is not on the cache, S_ERROR is returned.
      return self.__getRSSCatalogStatus( elementName, statusType )
    else:
      return self.__getCSCatalogStatus( elementName, statusType, default )


  def setCatalogStatus( self, elementName, statusType, status, reason = None,
                               tokenOwner = None ):

    """
    Helper with dual access, tries set information in RSS and in CS.

    example:
      >>> getStorageElementStatus( 'CERN-USER', 'ReadAccess' )
          S_OK( { 'ReadAccess': 'Active' } )
      >>> getStorageElementStatus( 'CERN-USER', 'WriteAccess' )
          S_OK( {'ReadAccess': 'Active', 'WriteAccess': 'Active', 'CheckAccess': 'Banned', 'RemoveAccess': 'Banned'} )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' )
          S_ERROR( xyz.. )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType', 'Unknown' )
          S_OK( 'Unknown' )
    """

    if self.__getMode():
      return self.__setRSSCatalogStatus( elementName, statusType, status, reason, tokenOwner )
    else:
      return self.__setCSCatalogStatus( elementName, statusType, status )


  def getStorageElementStatus( self, elementName, statusType = None, default = None ):
    """
    Helper with dual access, tries to get information from the RSS for the given
    StorageElement, otherwise, it gets it from the CS.

    example:
      >>> getStorageElementStatus( 'CERN-USER', 'ReadAccess' )
          S_OK( { 'CERN-USER' : { 'ReadAccess': 'Active' } } )
      >>> getStorageElementStatus( 'CERN-USER', 'Write' )
          S_OK( { 'CERN-USER' : {'ReadAccess': 'Active', 'WriteAccess': 'Active',
                                 'CheckAccess': 'Banned', 'RemoveAccess': 'Banned'}} )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' )
          S_ERROR( xyz.. )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType', 'Unknown' )
          S_OK( 'Unknown' )

    """

    if self.__getMode():
      # We do not apply defaults. If is not on the cache, S_ERROR is returned.
      return self.__getRSSStorageElementStatus( elementName, statusType )
    else:
      return self.__getCSStorageElementStatus( elementName, statusType, default )

  def setStorageElementStatus( self, elementName, statusType, status, reason = None,
                               tokenOwner = None ):

    """
    Helper with dual access, tries set information in RSS and in CS.

    example:
      >>> getStorageElementStatus( 'CERN-USER', 'ReadAccess' )
          S_OK( { 'ReadAccess': 'Active' } )
      >>> getStorageElementStatus( 'CERN-USER', 'Write' )
          S_OK( {'ReadAccess': 'Active', 'WriteAccess': 'Active', 'CheckAccess': 'Banned', 'RemoveAccess': 'Banned'} )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' )
          S_ERROR( xyz.. )
      >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType', 'Unknown' )
          S_OK( 'Unknown' )
    """

    if self.__getMode():
      return self.__setRSSStorageElementStatus( elementName, statusType, status, reason, tokenOwner )
    else:
      return self.__setCSStorageElementStatus( elementName, statusType, status )

################################################################################

  def __updateSECache( self ):
    """ Method used to update the StorageElementCache.

        It will try 5 times to contact the RSS before giving up
    """

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }

    for ti in range( 5 ):
      rawCache = self.rssClient.selectStatusElement( 'Resource', 'Status',
                                                     elementType = 'StorageElement',
                                                     meta = meta )
      if rawCache['OK']:
        break
      self.log.warn( "Can't get SE status", rawCache['Message'] + "; trial %d" % ti )
      sleep( math.pow( ti, 2 ) )
      self.rssClient = ResourceStatusClient()

    if not rawCache[ 'OK' ]:
      return rawCache
    return S_OK( getCacheDictFromRawData( rawCache[ 'Value' ] ) )

  def __updateCECache( self ):
    """ Method used to update the ComputingElementCache.

        It will try 5 times to contact the RSS before giving up
    """

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }

    for ti in range( 5 ):
      rawCache = self.rssClient.selectStatusElement( 'Resource', 'Status',
                                                     elementType = 'ComputingElement',
                                                     meta = meta )
      if rawCache['OK']:
        break
      self.log.warn( "Can't get CE status", rawCache['Message'] + "; trial %d" % ti )
      sleep( math.pow( ti, 2 ) )
      self.rssClient = ResourceStatusClient()

    if not rawCache[ 'OK' ]:
      return rawCache
    return S_OK( getCacheDictFromRawData( rawCache[ 'Value' ] ) )

  def __updateCatalogCache( self ):
    """ Method used to update the catalogCache.

        It will try 5 times to contact the RSS before giving up
    """

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }

    for ti in range( 5 ):
      rawCache = self.rssClient.selectStatusElement( 'Resource', 'Status',
                                                     elementType = 'Catalog',
                                                     meta = meta )
      if rawCache['OK']:
        break
      self.log.warn( "Can't get catalog status", rawCache['Message'] + "; trial %d" % ti )
      sleep( math.pow( ti, 2 ) )
      self.rssClient = ResourceStatusClient()

    if not rawCache[ 'OK' ]:
      return rawCache
    return S_OK( getCacheDictFromRawData( rawCache[ 'Value' ] ) )

  def __updateFTSCache( self ):
    """ Method used to update the FTS.

        It will try 5 times to contact the RSS before giving up
    """

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }

    for ti in range( 5 ):
      rawCache = self.rssClient.selectStatusElement( 'Resource', 'Status',
                                                     elementType = 'FTS',
                                                     meta = meta )
      if rawCache['OK']:
        break
      self.log.warn( "Can't get FTS status", rawCache['Message'] + "; trial %d" % ti )
      sleep( math.pow( ti, 2 ) )
      self.rssClient = ResourceStatusClient()

    if not rawCache[ 'OK' ]:
      return rawCache
    return S_OK( getCacheDictFromRawData( rawCache[ 'Value' ] ) )


################################################################################

  def __getRSSComputingElementStatus( self, elementName, statusType ):
    """
    Gets from the cache or the RSS the ComputingElements status. The cache is a
    copy of the DB table. If it is not on the cache, most likely is not going
    to be on the DB.

    There is one exception: item just added to the CS, e.g. new ComputingElement.
    The period between it is added to the DB and the changes are propagated
    to the cache will be inconsistent, but not dangerous. Just wait <cacheLifeTime>
    minutes.
    """

    cacheMatch = self.ceCache.match( elementName, statusType )

    self.log.debug( '__getRSSComputingElementStatus' )
    self.log.debug( cacheMatch )

    return cacheMatch

  def __setRSSComputingElementStatus( self, elementName, statusType, status, reason, tokenOwner ):
    """
    Sets on the RSS the ComputingElements status
    """

    expiration = datetime.datetime.utcnow() + datetime.timedelta( days = 1 )

    self.ceCache.acquireLock()
    try:
      res = self.rssClient.modifyStatusElement( 'Resource', 'Status', name = elementName,
                                                statusType = statusType, status = status,
                                                reason = reason, tokenOwner = tokenOwner,
                                                tokenExpiration = expiration )
      if res[ 'OK' ]:
        self.ceCache.refreshCache()
      else:
        _msg = 'Error updating ComputingElement (%s,%s,%s)' % ( elementName, statusType, status )
        gLogger.warn( 'RSS: %s' % _msg )

      return res

    finally:
      # Release lock, no matter what.
      self.ceCache.releaseLock()

  def __getRSSftsStatus( self, elementName, statusType ):
    """
    Gets from the cache or the RSS the FTS status. The cache is a
    copy of the DB table. If it is not on the cache, most likely is not going
    to be on the DB.

    There is one exception: item just added to the CS, e.g. new FTS.
    The period between it is added to the DB and the changes are propagated
    to the cache will be inconsistent, but not dangerous. Just wait <cacheLifeTime>
    minutes.
    """

    cacheMatch = self.ftsCache.match( elementName, statusType )

    self.log.debug( '__getRSSftsStatus' )
    self.log.debug( cacheMatch )

    return cacheMatch

  def __setRSSftsStatus( self, elementName, statusType, status, reason, tokenOwner ):
    """
    Sets on the RSS the FTS status
    """

    expiration = datetime.datetime.utcnow() + datetime.timedelta( days = 1 )

    self.ftsCache.acquireLock()
    try:
      res = self.rssClient.modifyStatusElement( 'Resource', 'Status', name = elementName,
                                                statusType = statusType, status = status,
                                                reason = reason, tokenOwner = tokenOwner,
                                                tokenExpiration = expiration )
      if res[ 'OK' ]:
        self.ftsCache.refreshCache()
      else:
        _msg = 'Error updating FTS (%s,%s,%s)' % ( elementName, statusType, status )
        gLogger.warn( 'RSS: %s' % _msg )

      return res

    finally:
      # Release lock, no matter what.
      self.ftsCache.releaseLock()

  def __getRSSCatalogStatus( self, elementName, statusType ):
    """
    Gets from the cache or the RSS the Catalog status. The cache is a
    copy of the DB table. If it is not on the cache, most likely is not going
    to be on the DB.

    There is one exception: item just added to the CS, e.g. new Catalog.
    The period between it is added to the DB and the changes are propagated
    to the cache will be inconsistent, but not dangerous. Just wait <cacheLifeTime>
    minutes.
    """

    cacheMatch = self.catalogCache.match( elementName, statusType )

    self.log.debug( '__getRSSCatalogStatus' )
    self.log.debug( cacheMatch )

    return cacheMatch


  def __getCSCatalogStatus( self, elementName, statusType, default ):
    """
    Gets from the CS the Catalog status
    """

    cs_path = "/Resources/FileCatalogs"

    if not isinstance( elementName, list ):
      elementName = [ elementName ]

    statuses = self.rssConfig.getConfigStatusType( 'Catalog' )

    result = {}
    for element in elementName:

      if statusType is not None:
        # Added Active by default
        res = gConfig.getValue( "%s/%s/%s" % ( cs_path, element, statusType ), 'Active' )
        result[element] = {statusType: res}

      else:
        res = gConfig.getOptionsDict( "%s/%s" % ( cs_path, element ) )
        if res[ 'OK' ] and res[ 'Value' ]:
          elementStatuses = {}
          for elementStatusType, value in res[ 'Value' ].items():
            if elementStatusType in statuses:
              elementStatuses[ elementStatusType ] = value

          # If there is no status defined in the CS, we add by default Read and
          # Write as Active.
          if elementStatuses == {}:
            elementStatuses = { 'ReadAccess' : 'Active', 'WriteAccess' : 'Active' }

          result[ element ] = elementStatuses

    if result:
      return S_OK( result )

    if default is not None:

      # sec check
      if statusType is None:
        statusType = 'none'

      defList = [ [ el, statusType, default ] for el in elementName ]
      return S_OK( getDictFromList( defList ) )

    _msg = "Catalog '%s', with statusType '%s' is unknown for CS."
    return S_ERROR(DErrno.ERESUNK, _msg % ( elementName, statusType ) )


  def __setRSSCatalogStatus( self, elementName, statusType, status, reason, tokenOwner ):
    """
    Sets on the RSS the Catalog status
    """

    expiration = datetime.datetime.utcnow() + datetime.timedelta( days = 1 )

    self.catalogCache.acquireLock()
    try:
      res = self.rssClient.modifyStatusElement( 'Resource', 'Status', name = elementName,
                                                statusType = statusType, status = status,
                                                reason = reason, tokenOwner = tokenOwner,
                                                tokenExpiration = expiration )
      if res[ 'OK' ]:
        self.catalogCache.refreshCache()

      if not res[ 'OK' ]:
        _msg = 'Error updating Catalog (%s,%s,%s)' % ( elementName, statusType, status )
        gLogger.warn( 'RSS: %s' % _msg )

      return res

    finally:
      # Release lock, no matter what.
      self.catalogCache.releaseLock()

  def __setCSCatalogStatus( self, elementName, statusType, status ):
    """
    Sets on the CS the Catalog status
    """

    statuses = self.rssConfig.getConfigStatusType( 'Catalog' )
    if not statusType in statuses:
      gLogger.error( "%s is not a valid statusType" % statusType )
      return S_ERROR( "%s is not a valid statusType: %s" % ( statusType, statuses ) )

    csAPI = CSAPI()

    cs_path = "/Resources/FileCatalogs"

    csAPI.setOption( "%s/%s/%s" % ( cs_path, elementName, statusType ), status )

    res = csAPI.commitChanges()
    if not res[ 'OK' ]:
      gLogger.warn( 'CS: %s' % res[ 'Message' ] )

    return res



  def __getRSSStorageElementStatus( self, elementName, statusType ):
    """
    Gets from the cache or the RSS the StorageElements status. The cache is a
    copy of the DB table. If it is not on the cache, most likely is not going
    to be on the DB.

    There is one exception: item just added to the CS, e.g. new StorageElement.
    The period between it is added to the DB and the changes are propagated
    to the cache will be inconsistent, but not dangerous. Just wait <cacheLifeTime>
    minutes.
    """

    cacheMatch = self.seCache.match( elementName, statusType )

    self.log.debug( '__getRSSStorageElementStatus' )
    self.log.debug( cacheMatch )

    return cacheMatch

  def __getCSStorageElementStatus( self, elementName, statusType, default ):
    """
    Gets from the CS the StorageElements status
    """

    cs_path = "/Resources/StorageElements"

    if not isinstance( elementName, list ):
      elementName = [ elementName ]

    statuses = self.rssConfig.getConfigStatusType( 'StorageElement' )

    result = {}
    for element in elementName:

      if statusType is not None:
        # Added Active by default
        res = gConfig.getValue( "%s/%s/%s" % ( cs_path, element, statusType ), 'Active' )
        result[element] = {statusType: res}

      else:
        res = gConfig.getOptionsDict( "%s/%s" % ( cs_path, element ) )
        if res[ 'OK' ] and res[ 'Value' ]:
          elementStatuses = {}
          for elementStatusType, value in res[ 'Value' ].items():
            if elementStatusType in statuses:
              elementStatuses[ elementStatusType ] = value

          # If there is no status defined in the CS, we add by default Read and
          # Write as Active.
          if elementStatuses == {}:
            elementStatuses = { 'ReadAccess' : 'Active', 'WriteAccess' : 'Active' }

          result[ element ] = elementStatuses

    if result:
      return S_OK( result )

    if default is not None:

      # sec check
      if statusType is None:
        statusType = 'none'

      defList = [ [ el, statusType, default ] for el in elementName ]
      return S_OK( getDictFromList( defList ) )

    _msg = "StorageElement '%s', with statusType '%s' is unknown for CS."
    return S_ERROR(DErrno.ERESUNK, _msg % ( elementName, statusType ) )

  def __setRSSStorageElementStatus( self, elementName, statusType, status, reason, tokenOwner ):
    """
    Sets on the RSS the StorageElements status
    """

    expiration = datetime.datetime.utcnow() + datetime.timedelta( days = 1 )

    self.seCache.acquireLock()
    try:
      res = self.rssClient.modifyStatusElement( 'Resource', 'Status', name = elementName,
                                                statusType = statusType, status = status,
                                                reason = reason, tokenOwner = tokenOwner,
                                                tokenExpiration = expiration )
      if res[ 'OK' ]:
        self.seCache.refreshCache()

      if not res[ 'OK' ]:
        _msg = 'Error updating StorageElement (%s,%s,%s)' % ( elementName, statusType, status )
        gLogger.warn( 'RSS: %s' % _msg )

      return res

    finally:
      # Release lock, no matter what.
      self.seCache.releaseLock()

  def __setCSStorageElementStatus( self, elementName, statusType, status ):
    """
    Sets on the CS the StorageElements status
    """

    statuses = self.rssConfig.getConfigStatusType( 'StorageElement' )
    if not statusType in statuses:
      gLogger.error( "%s is not a valid statusType" % statusType )
      return S_ERROR( "%s is not a valid statusType: %s" % ( statusType, statuses ) )

    csAPI = CSAPI()

    cs_path = "/Resources/StorageElements"

    csAPI.setOption( "%s/%s/%s" % ( cs_path, elementName, statusType ), status )

    res = csAPI.commitChanges()
    if not res[ 'OK' ]:
      gLogger.warn( 'CS: %s' % res[ 'Message' ] )

    return res

  def __getMode( self ):
    """
      Get's flag defined ( or not ) on the RSSConfiguration. If defined as 1,
      we use RSS, if not, we use CS.
    """

    res = self.rssConfig.getConfigState()

    if res == 'Active':

      if self.rssClient is None:
        self.rssClient = ResourceStatusClient()
      return True

    self.rssClient = None
    return False

  def isStorageElementAlwaysBanned( self, seName, statusType ):
    """ Checks if the AlwaysBanned policy is applied to the SE
        given as parameter

        :param seName : string, name of the SE
        :param statusType : ReadAcces, WriteAccess, RemoveAccess, CheckAccess

        :returns: S_OK(True/False)
    """

    res = self.infoGetter.getPoliciesThatApply( {'name' : seName, 'statusType' : statusType} )
    if not res['OK']:
      self.log.error( "isStorageElementAlwaysBanned: unable to get the information", res['Message'] )
      return res

    isAlwaysBanned = 'AlwaysBanned' in [policy['type'] for policy in res['Value']]

    return S_OK( isAlwaysBanned )

################################################################################

def getDictFromList( fromList ):
  """
  Auxiliar method that given a list returns a dictionary of dictionaries:
  { site1 : { statusType1 : st1, statusType2 : st2 }, ... }
  """

  res = {}
  for listElement in fromList:
    site, sType, status = listElement
    if not res.has_key( site ):
      res[ site ] = {}
    res[ site ][ sType ] = status
  return res

def getCacheDictFromRawData( rawList ):
  """
  Formats the raw data list, which we know it must have tuples of three elements.
  ( element1, element2, element3 ) into a list of tuples with the format
  ( ( element1, element2 ), element3 ). Then, it is converted to a dictionary,
  which will be the new Cache.

  It happens that element1 is elementName, element2 is statusType and element3
  is status.

  :Parameters:
    **rawList** - `list`
      list of three element tuples [( element1, element2, element3 ),... ]

  :return: dict of the form { ( elementName, statusType ) : status, ... }
  """

  res = [ ( ( name, sType ), status ) for name, sType, status in rawList ]
  return dict( res )

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
