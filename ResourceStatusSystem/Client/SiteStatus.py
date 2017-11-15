""" SiteStatus helper

  Provides methods to easily interact with the CS and/or RSS to get the site status

"""

import errno
import math
from time import sleep
from datetime import datetime, timedelta

from DIRAC                                                  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton                    import DIRACSingleton
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.Core.Utilities                                   import DErrno
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatus       import ResourceStatus
from DIRAC.ResourceStatusSystem.Utilities.RSSCacheNoThread  import RSSCache
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration  import RssConfiguration

__RCSID__ = '$Id: $'

class SiteStatus( object ):
  """
  RSS helper to interact with the 'Site' family on the DB. It provides the most
  demanded functions and a cache to avoid hitting the server too often.

  It provides four methods to interact with the site statuses:
  * getSiteStatuses
  * isUsableSite
  * getUsableSites
  * getSites
  """

  __metaclass__ = DIRACSingleton

  def __init__( self ):
    """
    Constructor, initializes the rssClient.
    """

    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self.rssConfig = RssConfiguration()
    self.__opHelper = Operations()
    self.rssFlag = ResourceStatus().rssFlag
    self.rsClient = ResourceStatusClient()

    # We can set CacheLifetime and CacheHistory from CS, so that we can tune them.
    cacheLifeTime = int( self.rssConfig.getConfigCache() )

    # RSSCache only affects the calls directed to RSS, if using the CS it is not used.
    self.rssCache = RSSCache( cacheLifeTime, self.__updateRssCache )

  def __updateRssCache( self ):
    """ Method used to update the rssCache.

        It will try 5 times to contact the RSS before giving up
    """

    meta = {'columns' : ['Name', 'Status']}

    for ti in range(5):
      rawCache = self.rsClient.selectStatusElement('Site', 'Status', meta = meta)
      if rawCache['OK']:
        break
      self.log.warn("Can't get resource's status", rawCache['Message'] + "; trial %d" % ti)
      sleep(math.pow(ti, 2))
      self.rsClient = ResourceStatusClient()

    if not rawCache['OK']:
      return rawCache
    return S_OK(getCacheDictFromRawData(rawCache['Value']))


  def getSiteStatuses( self, siteNamesList = None ):
    """
    Method that queries the database for status of the sites in a given list.
    If the input is None, it is interpreted as * ( all ).

    If match is positive, the output looks like:
    {
     'test1.test1.org': 'Active',
     'test2.test2.org': 'Banned',
    }

    examples
      >>> siteStatus.getSiteStatuses( ['test1.test1.uk', 'test2.test2.net', 'test3.test3.org'] )
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

    if self.rssFlag:
      return self.__getRSSSiteStatus(siteNamesList)
    else:
      siteStatusDict = {}
      if siteNamesList:
        for siteName in siteNamesList:
          result = RPCClient('WorkloadManagement/WMSAdministrator').getSiteMaskStatus(siteName)
          if not result['OK']:
            return result
          else:
            siteStatusDict[siteName] = result['Value']
      else:
        result = RPCClient('WorkloadManagement/WMSAdministrator').getSiteMaskStatus()
        if not result['OK']:
          return result
        else:
          siteStatusDict = result['Value']

      return S_OK( siteStatusDict )

  def __getRSSSiteStatus(self, siteName = None):
    """ Gets from the cache or the RSS the Sites status. The cache is a
        copy of the DB table. If it is not on the cache, most likely is not going
        to be on the DB.

        There is one exception: item just added to the CS, e.g. new Element.
        The period between it is added to the DB and the changes are propagated
        to the cache will be inconsistent, but not dangerous. Just wait <cacheLifeTime>
        minutes.

    :param siteName: name of the site
    :type siteName: str
    """

    cacheMatch = self.rssCache.match(siteName, '', '')

    self.log.debug( '__getRSSSiteStatus' )
    self.log.debug( cacheMatch )

    return cacheMatch


  #FIXME: useless?
  def isUsableSite( self, siteName ):
    """
    Similar method to getSiteStatuses. The difference is the output.
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

    if self.rssFlag:
      siteStatus = self.rsClient.selectStatusElement( 'Site', 'Status',
                                                      name = siteName,
                                                      meta = { 'columns' : ['Status'] } )
    else:
      siteStatus = RPCClient( 'WorkloadManagement/WMSAdministrator' ).getSiteMaskStatus(siteName)

    if not siteStatus['OK']:
      return siteStatus

    if not siteStatus['Value']:
      # Site does not exist, so it is not usable
      return S_OK(False)

    if self.rssFlag:
      status = siteStatus['Value'][0][0]
    else:
      status = siteStatus['Value']

    if status in ('Active', 'Degraded'):
      return S_OK(True)
    else:
      return S_OK(False)


  #FIXME: use getSitesStatuses?
  def getUsableSites( self, siteNamesList = None ):
    """
    Returns all sites that are usable if their
    statusType is either Active or Degraded; in a list.

    examples
      >>> siteStatus.getUsableSites( ['test1.test1.uk', 'test2.test2.net', 'test3.test3.org'] )
          S_OK( ['test1.test1.uk', 'test3.test3.org'] )
      >>> siteStatus.getUsableSites( None )
          S_OK( ['test1.test1.uk', 'test3.test3.org', 'test4.test4.org', 'test5.test5.org', ...] )
      >>> siteStatus.getUsableSites( 'NotExists' )
          S_ERROR( ... )

    :Parameters:
      **siteNamesList** - `List`
        name(s) of the sites to be matched

    :return: S_OK() || S_ERROR()
    """

    if not siteNamesList:
      if self.rssFlag:
        result = self.rsClient.selectStatusElement( 'Site', 'Status',
                                                    status = 'Active',
                                                    meta = { 'columns' : ['Name'] } )
        if not result['OK']:
          return result

        activeSites = [ x[0] for x in result['Value'] ]

        result = self.rsClient.selectStatusElement( 'Site', 'Status',
                                                    status = 'Degraded',
                                                    meta = { 'columns' : ['Name'] } )
        if not result['OK']:
          return result

        degradedSites = [ x[0] for x in result['Value'] ]

        return S_OK( activeSites + degradedSites )

      else:
        activeSites = RPCClient( 'WorkloadManagement/WMSAdministrator' ).getSiteMask()
        if not activeSites['OK']:
          return activeSites

        return S_OK( activeSites['Value'] )

    siteStatusList = []

    for siteName in siteNamesList:

      if self.rssFlag:
        siteStatus = self.rsClient.selectStatusElement( 'Site', 'Status',
                                                        name = siteName,
                                                        meta = { 'columns' : ['Status'] } )
      else:
        siteStatus = RPCClient( 'WorkloadManagement/WMSAdministrator' ).getSiteMaskStatus(siteName)

      if not siteStatus['OK']:
        return siteStatus
      elif not siteStatus['Value']:
        #if one of the listed elements does not exist continue
        continue
      else:

        if self.rssFlag:
          siteStatus = siteStatus['Value'][0][0]
        else:
          siteStatus = siteStatus['Value']

      if siteStatus in ('Active', 'Degraded'):
        siteStatusList.append(siteName)

    return S_OK( siteStatusList )


  #FIXME: use getSitesStatuses?
  def getSites( self, siteState = 'Active' ):
    """
    By default, it gets the currently active site list

    examples
      >>> siteStatus.getSites()
          S_OK( ['test1.test1.uk', 'test3.test3.org'] )
      >>> siteStatus.getSites( 'Active' )
          S_OK( ['test1.test1.uk', 'test3.test3.org'] )
      >>> siteStatus.getSites( 'Banned' )
          S_OK( ['test0.test0.uk', ... ] )
      >>> siteStatus.getSites( 'All' )
          S_OK( ['test1.test1.uk', 'test3.test3.org', 'test4.test4.org', 'test5.test5.org'...] )
      >>> siteStatus.getSites( None )
          S_ERROR( ... )

    :Parameters:
      **siteState** - `String`
        state of the sites to be matched

    :return: S_OK() || S_ERROR()
    """

    if not siteState:
      return S_ERROR(DErrno.ERESUNK, 'siteState parameter is empty')

    elif siteState.capitalize() == 'All':
      # if no siteState is set return everything
      if self.rssFlag:
        siteStatus = self.rsClient.selectStatusElement( 'Site',
                                                        'Status',
                                                        meta = { 'columns' : ['Name'] } )
      else:
        siteStatus = RPCClient( 'WorkloadManagement/WMSAdministrator' ).getSiteMask( 'All' )

    else:
      # fix case sensitive string
      siteState = siteState.capitalize()
      allowedStateList = ['Active', 'Banned', 'Degraded', 'Probing', 'Error', 'Unknown']
      if siteState not in allowedStateList:
        return S_ERROR(errno.EINVAL, 'Not a valid status, parameter rejected')

      if self.rssFlag:
        siteStatus = self.rsClient.selectStatusElement( 'Site',
                                                        'Status',
                                                        status = siteState,
                                                        meta = { 'columns' : ['Name'] } )
      else:
        siteStatus = RPCClient( 'WorkloadManagement/WMSAdministrator' ).getSiteMask( siteState )

    if not siteStatus['OK']:
      return siteStatus
    else:

      if not self.rssFlag:
        return S_OK( siteStatus['Value'] )

      siteList = []
      for site in siteStatus['Value']:
        siteList.append(site[0])

      return S_OK( siteList )

  def setSiteStatus( self, site, status, comment = 'No comment' ):
    """
    Set the status of a site in the 'SiteStatus' table of RSS

    examples
      >>> siteStatus.banSite( 'site1.test.test' )
          S_OK()
      >>> siteStatus.banSite( None )
          S_ERROR( ... )

    :Parameters:
      **site** - `String`
        the site that is going to be banned
      **comment** - `String`
        reason for banning

    :return: S_OK() || S_ERROR()
    """

    if not status:
      return S_ERROR(DErrno.ERESUNK, 'status parameter is empty')

    # fix case sensitive string
    status = status.capitalize()
    allowedStateList = ['Active', 'Banned', 'Degraded', 'Probing', 'Error', 'Unknown']
    if status not in allowedStateList:
      return S_ERROR(errno.EINVAL, 'Not a valid status, parameter rejected')

    result = getProxyInfo()
    if result['OK']:
      tokenOwner = result['Value']['username']
    else:
      return S_ERROR( "Unable to get user proxy info %s " % result['Message'] )

    tokenExpiration = datetime.utcnow() + timedelta( days = 1 )

    result = self.rsClient.modifyStatusElement( 'Site', 'Status', status = status, name = site,
                                                tokenExpiration = tokenExpiration, reason = comment,
                                                tokenOwner = tokenOwner )

    if not result['OK']:
      return result

    return S_OK()




def getCacheDictFromRawData( rawList ): #FIXME: to remove?
  """
  Formats the raw data list, which we know it must have tuples of four elements.
  ( element1, element2 ) into a dictionary of tuples with the format
  { ( element1 ): element2 )}.
  The resulting dictionary will be the new Cache.

  It happens that element1 is elementName,
                  element4 is status.

  :Parameters:
    **rawList** - `list`
      list of three element tuples [( element1, element2 ),... ]

  :return: dict of the form { ( elementName ) : status, ... }
  """

  res = {}
  for entry in rawList:
    res.update( { (entry[0]) : entry[1] } )

  return res

#################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
