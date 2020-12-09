""" SiteStatus helper

Module that acts as a helper for knowing the status of a site.
It takes care of switching between the CS and the RSS.
The status is kept in the RSSCache object, which is a small wrapper on top of DictCache

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

import six
import errno
import math
from time import sleep
from datetime import datetime, timedelta

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ResourceStatusSystem.Utilities.RSSCacheNoThread import RSSCache
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration import RssConfiguration


@six.add_metaclass(DIRACSingleton)
class SiteStatus(object):
  """
  RSS helper to interact with the 'Site' family on the DB. It provides the most
  demanded functions and a cache to avoid hitting the server too often.

  It provides four methods to interact with the site statuses:
  * getSiteStatuses
  * isUsableSite
  * getUsableSites
  * getSites
  """
  def __init__(self):
    """
    Constructor, initializes the rssClient.
    """

    self.log = gLogger.getSubLogger(self.__class__.__name__)
    self.rssConfig = RssConfiguration()
    self.__opHelper = Operations()
    self.rssFlag = ResourceStatus().rssFlag
    self.rsClient = ResourceStatusClient()

    cacheLifeTime = int(self.rssConfig.getConfigCache())

    # RSSCache only affects the calls directed to RSS, if using the CS it is not used.
    self.rssCache = RSSCache(cacheLifeTime, self.__updateRssCache)

  def __updateRssCache(self):
    """ Method used to update the rssCache.

        It will try 5 times to contact the RSS before giving up
    """

    meta = {'columns': ['Name', 'Status']}

    for ti in range(5):
      rawCache = self.rsClient.selectStatusElement('Site', 'Status', meta=meta)
      if rawCache['OK']:
        break
      self.log.warn("Can't get resource's status", rawCache['Message'] + "; trial %d" % ti)
      sleep(math.pow(ti, 2))
      self.rsClient = ResourceStatusClient()

    if not rawCache['OK']:
      return rawCache
    return S_OK(getCacheDictFromRawData(rawCache['Value']))

  def getSiteStatuses(self, siteNames=None):
    """
    Method that queries the database for status of the sites in a given list.
    A single string site name may also be provides as "siteNames"
    If the input is None, it is interpreted as * ( all ).

    If match is positive, the output looks like::

      {
       'test1.test1.org': 'Active',
       'test2.test2.org': 'Banned',
      }

    Examples::

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

    :param siteNames: name(s) of the sites to be matched
    :type siteNames: list, str
    :return: S_OK() || S_ERROR()
    """

    if self.rssFlag:
      return self.__getRSSSiteStatus(siteNames)
    else:
      siteStatusDict = {}
      wmsAdmin = WMSAdministratorClient()
      if siteNames:
        if isinstance(siteNames, six.string_types):
          siteNames = [siteNames]
        for siteName in siteNames:
          result = wmsAdmin.getSiteMaskStatus(siteName)
          if not result['OK']:
            return result
          else:
            siteStatusDict[siteName] = result['Value']
      else:
        result = wmsAdmin.getSiteMaskStatus()
        if not result['OK']:
          return result
        else:
          siteStatusDict = result['Value']

      return S_OK(siteStatusDict)

  def __getRSSSiteStatus(self, siteName=None):
    """ Gets from the cache or the RSS the Sites status. The cache is a
        copy of the DB table. If it is not on the cache, most likely is not going
        to be on the DB.

        There is one exception: item just added to the CS, e.g. new Element.
        The period between it is added to the DB and the changes are propagated
        to the cache will be inconsistent, but not dangerous. Just wait <cacheLifeTime>
        minutes.

    :param siteName: name of the site
    :type siteName: str

    :return: dict
    """

    cacheMatch = self.rssCache.match(siteName, '', '')

    self.log.debug('__getRSSSiteStatus')
    self.log.debug(cacheMatch)

    return cacheMatch

  def getUsableSites(self, siteNames=None):
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
      **siteNames** - `List` or `str`
        name(s) of the sites to be matched

    :return: S_OK() || S_ERROR()
    """

    siteStatusDictRes = self.getSiteStatuses(siteNames)
    if not siteStatusDictRes['OK']:
      return siteStatusDictRes
    siteStatusList = [x[0] for x in siteStatusDictRes['Value'].items() if x[1] in ['Active', 'Degraded']]

    return S_OK(siteStatusList)

  def getSites(self, siteState='Active'):
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

    siteStatusDictRes = self.getSiteStatuses()
    if not siteStatusDictRes['OK']:
      return siteStatusDictRes

    if siteState.capitalize() == 'All':
      # if no siteState is set return everything
      siteList = list(siteStatusDictRes['Value'])

    else:
      # fix case sensitive string
      siteState = siteState.capitalize()
      allowedStateList = ['Active', 'Banned', 'Degraded', 'Probing', 'Error', 'Unknown']
      if siteState not in allowedStateList:
        return S_ERROR(errno.EINVAL, 'Not a valid status, parameter rejected')

      siteList = [x[0] for x in siteStatusDictRes['Value'].items() if x[1] == siteState]

    return S_OK(siteList)

  def setSiteStatus(self, site, status, comment='No comment'):
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

    if self.rssFlag:
      result = getProxyInfo()
      if result['OK']:
        tokenOwner = result['Value']['username']
      else:
        return S_ERROR("Unable to get user proxy info %s " % result['Message'])

      tokenExpiration = datetime.utcnow() + timedelta(days=1)

      self.rssCache.acquireLock()
      try:
        result = self.rsClient.modifyStatusElement('Site', 'Status', status=status, name=site,
                                                   tokenExpiration=tokenExpiration, reason=comment,
                                                   tokenOwner=tokenOwner)
        if result['OK']:
          self.rssCache.refreshCache()
        else:
          _msg = 'Error updating status of site %s to %s' % (site, status)
          gLogger.warn('RSS: %s' % _msg)

      # Release lock, no matter what.
      finally:
        self.rssCache.releaseLock()

    else:
      if status in ['Active', 'Degraded']:
        result = WMSAdministratorClient().allowSite()
      else:
        result = WMSAdministratorClient().banSite()

    return result


def getCacheDictFromRawData(rawList):  # FIXME: to remove?
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
    res.update({(entry[0]): entry[1]})

  return res
