""" SiteStatus helper

  Provides methods to easily interact with the RSS

"""

import math
from time import sleep
from DIRAC                                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton                    import DIRACSingleton
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.ResourceStatusSystem.Client.ResourceStatus       import getCacheDictFromRawData
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities.RSSCacheNoThread  import RSSCache
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration  import RssConfiguration
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter        import InfoGetter
from DIRAC.ConfigurationSystem.Client.Helpers.Path          import cfgPath
from DIRAC.Core.Utilities                                   import DErrno

__RCSID__ = '$Id: $'

class SiteStatus( object ):
  """
  RSS helper to interact with the 'Site' family on the DB. It provides the most
  demanded functions and a cache to avoid hitting the server too often.

  It provides four methods to interact with the site statuses:
  * getSiteStatuses
  * getSiteStatus
  * isUsableSite
  * getUsableSites
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
    self.rsClient = ResourceStatusClient()

    # TODO: The RSSCache isn't working right now for sites
    # We can set CacheLifetime and CacheHistory from CS, so that we can tune them.
    # cacheLifeTime = int( self.rssConfig.getConfigCache() )

    # RSSCache initialization
    # self.siteCache  = RSSCache( 'Site', cacheLifeTime, self.__updateSiteCache )

  def getSiteStatuses( self, siteNamesList ):
    """
    Method that queries the database for status of the sites in a given list.
    If the input is None, it is interpreted as * ( all ).

    If match is positive, the output looks like:
    {
     'test1.test1.org': 'Active',
     'test2.test2.org': 'Banned',
    }

    examples
      >>> siteStatus.getSiteStatuses( [ 'test1.test1.uk', 'test2.test2.net', 'test3.test3.org' ] )
          S_OK( { 'test1.test1.org': 'Active', 'test2.test2.net': 'Banned', 'test3.test3.org': 'Active' }  )
      >>> siteStatus.getSiteStatuses( 'NotExists')
          S_ERROR( ... ))
      >>> siteStatus.getSiteStatuses( None )
          S_OK( { 'test1.test1.org': 'Active',
                  'test2.test2.net': 'Banned', },
                  ...
                }
              )

    :Parameters:
      **siteNamesList** - `list`
        name(s) of the sites to be matched

    :return: S_OK() || S_ERROR()
    """

    if siteNamesList is None:
     siteStatusDict = self.rsClient.selectStatusElement( 'Site', 'Status', meta = { 'columns' : [ 'Name', 'Status' ] } )

     if not siteStatusDict['OK']:
       return S_ERROR(DErrno.ERESGEN, 'selectStatusElement failed')
     else:
       siteStatusDict = siteStatusDict['Value']

     return S_OK( dict(siteStatusDict) )

    siteStatusDict = {}

    for siteName in siteNamesList:
      print siteName
      result = self.rsClient.selectStatusElement( 'Site', 'Status', name = siteName, meta = { 'columns' : [ 'Status' ] } )

      if not result['OK']:
        return S_ERROR(DErrno.ERESGEN, 'selectStatusElement failed')
      elif result['Value'] == ():
        #if one of the listed elements does not exist continue
        continue
      else:
        siteStatusDict[siteName] = result['Value'][0][0]

    return S_OK( siteStatusDict )


  def getSiteStatus( self, siteName ):
    """
    Given a site, it returns its status by querying the database.

    examples
      >>> siteStatus.getSiteStatus( 'test1.test1.org' )
          S_OK( 'Active' )
      >>> siteStatus.getSiteStatus( 'NotExists' )
          S_ERROR( ... )
      >>> siteStatus.getSiteStatus( None )
          S_ERROR( ... )

    :Parameters:
      **siteName** - `string`
        name of the site to be matched

    :return: S_OK() || S_ERROR()
    """

    if siteName is None:
      return S_ERROR(DErrno.ERESUNK, 'Site does not exists')

    siteStatus = self.rsClient.selectStatusElement( 'Site', 'Status', name = siteName, meta = { 'columns' : [ 'Status' ] } )

    if not siteStatus['OK']:
      return S_ERROR(DErrno.ERESGEN, 'selectStatusElement failed')
    else:
      siteStatus = siteStatus['Value'][0][0]

    return S_OK( siteStatus )


  def isUsableSite( self, siteName ):
    """
    Similar method to getSiteStatus. The difference is the output.
    Given a site name, returns a bool if the site is usable:
    status is Active or Degraded outputs True
    anything else outputs False

    examples
      >>> siteStatus.isUsableSite( 'test1.test1.org' )
          True
      >>> siteStatus.isUsableSite( 'test2.test2.org' )
          False # May be banned
      >>> siteStatus.isUsableSite( None )
          False
      >>> siteStatus.isUsableSite( 'NotExists' )
          False

    :Parameters:
      **siteName** - `string`
        name of the site to be matched

    :return: S_OK() || S_ERROR()
    """

    siteStatus = self.rsClient.selectStatusElement( 'Site', 'Status', name = siteName, meta = { 'columns' : [ 'Status' ] } )

    if not siteStatus['OK']:
      return S_ERROR(DErrno.ERESGEN, 'selectStatusElement failed')

    if siteStatus['Value'] == ():
      #Site does not exist, so it is not usable
      return S_OK(False)

    if siteStatus['Value'][0][0] == 'Active' or siteStatus['Value'][0][0] == 'Degraded':
      return S_OK(True)
    else:
      return S_OK(False)


  def getUsableSites( self, siteNamesList ):
    """
    Returns all sites that are usable if their
    statusType is either Active or Degraded; in a list.

    examples
      >>> siteStatus.getUsableSites( [ 'test1.test1.uk', 'test2.test2.net', 'test3.test3.org' ] )
          S_OK( ['test1.test1.uk', 'test3.test3.org'] )
      >>> siteStatus.getUsableSites( None )
          S_ERROR( ... )
      >>> siteStatus.getUsableSites( 'NotExists' )
          S_ERROR( ... )

    :Parameters:
      **siteNamesList** - `List`
        name(s) of the sites to be matched

    :return: S_OK() || S_ERROR()
    """

    if siteNamesList is None:
      return S_ERROR(DErrno.ERESUNK, 'siteNamesList is empty')

    siteStatusList = []

    for siteName in siteNamesList:
      siteStatus = self.rsClient.selectStatusElement( 'Site', 'Status', name = siteName, meta = { 'columns' : [ 'Status' ] } )

      if not siteStatus['OK']:
        return S_ERROR(DErrno.ERESGEN, 'selectStatusElement failed')
      elif siteStatus['Value'] == ():
        #if one of the listed elements does not exist continue
        continue
      else:
        siteStatus = siteStatus['Value'][0][0]

      if siteStatus == 'Active' or siteStatus == 'Degraded':
        siteStatusList.append(siteName)

    return S_OK( siteStatusList )


 ################################################################################

  def __updateSiteCache( self ):
    """ Method used to update the Site Cache.

        It will try 5 times to contact the RSS before giving up
    """

    meta = { 'columns' : [ 'Name', 'StatusType', 'Status' ] }

    for ti in range( 5 ):
      rawCache = self.rssClient.selectStatusElement( 'Site', 'Status', meta = meta )
      if rawCache['OK']:
        break
      self.log.warn( "Can't get Site status", rawCache['Message'] + "; trial %d" % ti )
      sleep( math.pow( ti, 2 ) )
      self.rssClient = ResourceStatusClient()

    if not rawCache[ 'OK' ]:
      return rawCache
    return S_OK( getCacheDictFromRawData( rawCache[ 'Value' ] ) )

#################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF