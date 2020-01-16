""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id$"

import os
import glob
import time
from six.moves.urllib.request import urlopen
import random
import hashlib
import commands

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security.MyProxy import MyProxy
from DIRAC.Core.Security.X509Request import X509Request  # pylint: disable=import-error
from DIRAC.Core.Security.X509Chain import X509Chain, isPUSPdn  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Resources.ProxyProvider.ProxyProviderFactory import ProxyProviderFactory


class ProxyDB(DB):

  NOTIFICATION_TIMES = [2592000, 1296000]

  def __init__(self,
               useMyProxy=False):
    DB.__init__(self, 'ProxyDB', 'Framework/ProxyDB')
    random.seed()
    self.__version = 1
    self.__defaultRequestLifetime = 300  # 5min
    self.__defaultTokenLifetime = 86400 * 7  # 1 week
    self.__defaultTokenMaxUses = 50
    self.__useMyProxy = useMyProxy
    self._minSecsToAllowStore = 3600
    self.__notifClient = NotificationClient()
    retVal = self.__initializeDB()
    if not retVal['OK']:
      raise Exception("Can't create tables: %s" % retVal['Message'])
    self.purgeExpiredProxies(sendNotifications=False)
    self.__checkDBVersion()

  def getMyProxyServer(self):
    """ Get MyProxy server from configuration

        :return: str
    """
    return gConfig.getValue("/DIRAC/VOPolicy/MyProxyServer", "myproxy.cern.ch")

  def getMyProxyMaxLifeTime(self):
    """ Get a maximum of the proxy lifetime delegated by MyProxy

        :return: int -- time in a seconds
    """
    return gConfig.getValue("/DIRAC/VOPolicy/MyProxyMaxDelegationTime", 168) * 3600

  def getFromAddr(self):
    """ Get the From address to use in proxy expiry e-mails.

        :return: str
    """
    cs_path = getDatabaseSection(self.fullname)
    opt_path = "/%s/%s" % (cs_path, "FromAddr")
    return gConfig.getValue(opt_path, "proxymanager@diracgrid.org")

  def __initializeDB(self):
    """ Create the tables

        :result: S_OK()/S_ERROR()
    """
    retVal = self._query("show tables")
    if not retVal['OK']:
      return retVal

    tablesInDB = [t[0] for t in retVal['Value']]
    tablesD = {}

    if 'ProxyDB_Requests' not in tablesInDB:
      tablesD['ProxyDB_Requests'] = {'Fields': {'Id': 'INTEGER AUTO_INCREMENT NOT NULL',
                                                'UserDN': 'VARCHAR(255) NOT NULL',
                                                'Pem': 'BLOB',
                                                'ExpirationTime': 'DATETIME'
                                                },
                                     'PrimaryKey': 'Id'
                                     }

    if 'ProxyDB_CleanProxies' not in tablesInDB:
      tablesD['ProxyDB_CleanProxies'] = {'Fields': {'UserDN': 'VARCHAR(255) NOT NULL',
                                                    'Pem': 'BLOB',
                                                    'ExpirationTime': 'DATETIME',
                                                    },
                                         'PrimaryKey': 'UserDN'
                                         }
    # WARN: Now proxies upload only in ProxyDB_CleanProxies, so this table will not be needed in some future
    if 'ProxyDB_Proxies' not in tablesInDB:
      tablesD['ProxyDB_Proxies'] = {'Fields': {'UserName': 'VARCHAR(64) NOT NULL',
                                               'UserDN': 'VARCHAR(255) NOT NULL',
                                               'UserGroup': 'VARCHAR(255) NOT NULL',
                                               'Pem': 'BLOB',
                                               'ExpirationTime': 'DATETIME',
                                               'PersistentFlag': 'ENUM ("True","False") NOT NULL DEFAULT "True"',
                                               },
                                    'PrimaryKey': ['UserDN', 'UserGroup']
                                    }

    if 'ProxyDB_VOMSProxies' not in tablesInDB:
      tablesD['ProxyDB_VOMSProxies'] = {'Fields': {'UserName': 'VARCHAR(64) NOT NULL',
                                                   'UserDN': 'VARCHAR(255) NOT NULL',
                                                   'UserGroup': 'VARCHAR(255) NOT NULL',
                                                   'VOMSAttr': 'VARCHAR(255) NOT NULL',
                                                   'Pem': 'BLOB',
                                                   'ExpirationTime': 'DATETIME',
                                                   },
                                        'PrimaryKey': ['UserDN', 'UserGroup', 'vomsAttr']
                                        }

    if 'ProxyDB_Log' not in tablesInDB:
      tablesD['ProxyDB_Log'] = {'Fields': {'ID': 'BIGINT NOT NULL AUTO_INCREMENT',
                                           'IssuerUsername': 'VARCHAR(255) NOT NULL',
                                           'IssuerGroup': 'VARCHAR(255) NOT NULL',
                                           'TargetUsername': 'VARCHAR(255) NOT NULL',
                                           'TargetGroup': 'VARCHAR(255) NOT NULL',
                                           'Action': 'VARCHAR(128) NOT NULL',
                                           'Timestamp': 'DATETIME',
                                           },
                                'PrimaryKey': 'ID',
                                'Indexes': {'Timestamp': ['Timestamp']}
                                }

    if 'ProxyDB_Tokens' not in tablesInDB:
      tablesD['ProxyDB_Tokens'] = {'Fields': {'Token': 'VARCHAR(64) NOT NULL',
                                              'RequesterUsername': 'VARCHAR(255) NOT NULL',
                                              'RequesterGroup': 'VARCHAR(255) NOT NULL',
                                              'ExpirationTime': 'DATETIME NOT NULL',
                                              'UsesLeft': 'SMALLINT UNSIGNED DEFAULT 1',
                                              },
                                   'PrimaryKey': 'Token'
                                   }

    if 'ProxyDB_ExpNotifs' not in tablesInDB:
      tablesD['ProxyDB_ExpNotifs'] = {'Fields': {'UserDN': 'VARCHAR(255) NOT NULL',
                                                 'UserGroup': 'VARCHAR(255) NOT NULL',
                                                 'LifeLimit': 'INTEGER UNSIGNED DEFAULT 0',
                                                 'ExpirationTime': 'DATETIME NOT NULL',
                                                 },
                                      'PrimaryKey': ['UserDN', 'UserGroup']
                                      }

    return self._createTables(tablesD)

  def __addUserNameToTable(self, tableName):
    """ Add user name to the table

        :param str tableName: table name

        :return: S_OK()/S_ERROR()
    """
    result = self._update("ALTER TABLE `%s` ADD COLUMN UserName VARCHAR(64) NOT NULL" % tableName)
    if not result['OK']:
      return result
    result = self._query("SELECT DISTINCT UserName, UserDN FROM `%s`" % tableName)
    if not result['OK']:
      return result
    data = result['Value']
    for userName, userDN in data:
      if not userName:
        result = Registry.getUsernameForDN(userDN)
        if not result['OK']:
          self.log.error("Could not retrieve username for DN", userDN)
          continue
        userName = result['Value']
        try:
          userName = self._escapeString(userName)['Value']
          userDN = self._escapeString(userDN)['Value']
        except KeyError:
          self.log.error("Could not escape username or DN", "%s %s" % (userName, userDN))
          continue
        userName = result['Value']
        result = self._update("UPDATE `%s` SET UserName=%s WHERE UserDN=%s" % (tableName, userName, userDN))
        if not result['OK']:
          self.log.error("Could update username for DN", "%s: %s" % (userDN, result['Message']))
          continue
        self.log.info("UserDN %s has user %s" % (userDN, userName))
    return S_OK()

  def __checkDBVersion(self):
    """ Check DB tables for empty UserName option

        :return: S_OK()/S_ERROR()
    """
    if self.versionDB == self.__version:
      return S_OK()
    if self.versionDB > self.__version:
      return S_ERROR('Already installed newer DB version "%s".' % self.versionDB)

    for tableName in ("ProxyDB_Proxies", "ProxyDB_VOMSProxies"):
      result = self._query("describe `%s`" % tableName)
      if not result['OK']:
        return result
      if 'UserName' not in [row[0] for row in result['Value']]:
        self.log.notice("Username missing in table %s schema. Adding it" % tableName)
        result = self.__addUserNameToTable(tableName)
        if not result['OK']:
          return result

    if self.versionDB < 1 and self.versionDB < self.__version:
      for tb, oldColumn, newColumn in [('ProxyDB_Log', 'IssuerDN', 'IssuerUsername'),
                                       ('ProxyDB_Log', 'TargetDN', 'TargetUsername'),
                                       ('ProxyDB_Tokens', 'RequesterDN', 'RequesterUsername')]:
        result = self._query("SHOW COLUMNS FROM `%s` LIKE '%s'" % (tb, oldColumn))
        if result['OK'] and len(result['Value']) > 0:
          result = self._query('ALTER TABLE %s CHANGE COLUMN %s %s VARCHAR(255) NOT NULL' % (tb, oldColumn, newColumn))
        if not result['OK']:
          return result
      result = self.updateDBVersion(1)
      if not result['OK']:
        return result
    
    return S_OK()

  def generateDelegationRequest(self, credDict):
    """ Generate a request and store it for a given proxy Chain

        :param dict credDict: dictionary that contain proxy as chain

        :return: S_OK(dict)/S_ERROR() -- dict contain id and proxy as string of the request
    """
    retVal = self._getConnection()
    if not retVal['OK']:
      return retVal
    connObj = retVal['Value']
    retVal = credDict['x509Chain'].generateProxyRequest()
    if not retVal['OK']:
      return retVal
    request = retVal['Value']
    retVal = request.dumpRequest()
    if not retVal['OK']:
      return retVal
    reqStr = retVal['Value']
    retVal = request.dumpPKey()
    if not retVal['OK']:
      return retVal
    allStr = reqStr + retVal['Value']
    try:
      sDN = self._escapeString(credDict['DN'])['Value']
      sAllStr = self._escapeString(allStr)['Value']
    except KeyError:
      return S_ERROR("Cannot escape DN")
    cmd = "INSERT INTO `ProxyDB_Requests` (UserDN, Pem, ExpirationTime) VALUES "
    cmd += "(%s, %s, TIMESTAMPADD(SECOND, %d, UTC_TIMESTAMP()))" % (sDN, sAllStr,
                                                                    int(self.__defaultRequestLifetime))
    retVal = self._update(cmd, conn=connObj)
    if not retVal['OK']:
      return retVal
    # 99% of the times we will stop here
    if 'lastRowId' in retVal:
      return S_OK({'id': retVal['lastRowId'], 'request': reqStr})
    # If the lastRowId hack does not work. Get it by hand
    retVal = self._query("SELECT Id FROM `ProxyDB_Requests` WHERE Pem='%s'" % reqStr)
    if not retVal['OK']:
      return retVal
    data = retVal['Value']
    if len(data) == 0:
      return S_ERROR("Insertion of the request in the db didn't work as expected")
    self.logAction("request upload", credDict['username'], credDict['group'], credDict['username'], "any")
    # Here we go!
    return S_OK({'id': data[0][0], 'request': reqStr})

  def __retrieveDelegationRequest(self, requestId, userDN):
    """ Retrieve a request from the DB

        :param int requestId: id of the request
        :param str userDN: user DN

        :return: S_OK(str)/S_ERROR()
    """
    try:
      sUserDN = self._escapeString(userDN)['Value']
    except KeyError:
      return S_ERROR("Cannot escape DN")
    cmd = "SELECT Pem FROM `ProxyDB_Requests` WHERE Id = %s AND UserDN = %s" % (requestId, sUserDN)
    retVal = self._query(cmd)
    if not retVal['OK']:
      return retVal
    data = retVal['Value']
    if len(data) == 0:
      return S_ERROR("No requests with id %s" % requestId)
    request = X509Request()
    retVal = request.loadAllFromString(data[0][0])
    if not retVal['OK']:
      return retVal
    return S_OK(request)

  def purgeExpiredRequests(self):
    """ Purge expired requests from the db

        :return: S_OK()/S_ERROR()
    """
    cmd = "DELETE FROM `ProxyDB_Requests` WHERE ExpirationTime < UTC_TIMESTAMP()"
    return self._update(cmd)

  def deleteRequest(self, requestId):
    """ Delete a request from the db

        :param int requestId: id of the request

        :return: S_OK()/S_ERROR()
    """
    cmd = "DELETE FROM `ProxyDB_Requests` WHERE Id=%s" % requestId
    return self._update(cmd)

  def completeDelegation(self, requestId, userDN, delegatedPem):
    """ Complete a delegation and store it in the db

        :param int requestId: id of the request
        :param str userDN: DN
        :param str delegatedPem: delegated proxy as string

        :return: S_OK()/S_ERROR()
    """
    retVal = self.__retrieveDelegationRequest(requestId, userDN)
    if not retVal['OK']:
      return retVal
    request = retVal['Value']
    chain = X509Chain(keyObj=request.getPKey())
    retVal = chain.loadChainFromString(delegatedPem)
    if not retVal['OK']:
      return retVal
    retVal = chain.isValidProxy()
    if not retVal['OK']:
      return retVal

    result = chain.isVOMS()
    if result['OK'] and result.get('Value'):
      return S_ERROR("Proxies with VOMS extensions are not allowed to be uploaded")

    retVal = chain.getDIRACGroup(ignoreDefault=True)
    if not retVal['OK']:
      return retVal
    if retVal['Value']:
      # WARN: Since v7r1, DIRAC has implemented proxyProviders and ProxyDB_CleanProxies,
      # WARN:   which has helped implement the ability to store only one proxy and
      # WARN:   dynamically add a group at the request of a proxy. This means that group extensions
      # WARN:   doesn't need for storing proxies, but for compatibility with older versions,
      # WARN:   the following block has been added to store proxies with group extensions.
      self.log.warn("Proxies with DIRAC group extensions must be not allowed to be uploaded:", retVal['Value'])
      retVal = chain.getDIRACGroup()
      if not retVal['OK']:
        return retVal
      userGroup = retVal['Value'] or Registry.getDefaultUserGroup()
      result = Registry.getGroupsForDN(userDN, researchedGroup=userGroup)
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR("%s group is not valid for %s" % (userGroup, userDN))
      retVal = chain.isValidProxy(ignoreDefault=True)
      if not retVal['OK'] and DErrno.cmpError(retVal, DErrno.ENOGROUP):
        retVal = self.deleteProxy(userDN)
        if not retVal['OK']:
          return retVal
      retVal = self.__storeProxyOld(userDN, userGroup, chain)
      # WARN: End of compatibility block
    else:
      retVal = self.__storeProxy(userDN, chain)

    if not retVal['OK']:
      return retVal
    return self.deleteRequest(requestId)

  def __storeProxy(self, userDN, chain):
    """ Store user proxy into the Proxy repository for a user specified by his
        DN and group or proxy provider.

        :param str userDN: user DN from proxy
        :param X509Chain() chain: proxy chain
        :param str proxyProvider: proxy provider name

        :return: S_OK()/S_ERROR()
    """
    # Get remaining secs
    retVal = chain.getRemainingSecs()
    if not retVal['OK']:
      return retVal
    remainingSecs = retVal['Value']
    if remainingSecs < self._minSecsToAllowStore:
      return S_ERROR("Cannot store proxy, remaining secs %s is less than %s" %
                     (remainingSecs, self._minSecsToAllowStore))

    # Compare the DNs
    retVal = chain.getIssuerCert()
    if not retVal['OK']:
      return retVal
    proxyIdentityDN = retVal['Value'].getSubjectDN()['Value']
    if userDN != proxyIdentityDN:
      msg = "Mismatch in the user DN"
      vMsg = "Proxy says %s and credentials are %s" % (proxyIdentityDN, userDN)
      self.log.error(msg, vMsg)
      return S_ERROR("%s. %s" % (msg, vMsg))

    # Check if its limited
    if chain.isLimitedProxy()['Value']:
      return S_ERROR("Limited proxies are not allowed to be stored")
    dLeft = remainingSecs / 86400
    hLeft = remainingSecs / 3600 - dLeft * 24
    mLeft = remainingSecs / 60 - hLeft * 60 - dLeft * 1440
    sLeft = remainingSecs - hLeft * 3600 - mLeft * 60 - dLeft * 86400
    self.log.info("Storing proxy for credentials %s (%d:%02d:%02d:%02d left)" %
                  (proxyIdentityDN, dLeft, hLeft, mLeft, sLeft))

    try:
      sUserDN = self._escapeString(userDN)['Value']
    except KeyError:
      return S_ERROR("Cannot escape DN")
    # Check what we have already got in the repository
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ), Pem "
    cmd += "FROM `ProxyDB_CleanProxies` WHERE UserDN=%s " % sUserDN
    result = self._query(cmd)
    if not result['OK']:
      return result
    data = result['Value']

    # Check if there is a previous ticket for the DN
    pemChain = chain.dumpAllToString()['Value']
    dValues = {'UserDN': sUserDN, 'Pem': self._escapeString(pemChain)['Value'],
               'ExpirationTime': 'TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() )' % int(remainingSecs)}
    cmd = "INSERT INTO `ProxyDB_CleanProxies` (%s) VALUES (%s)" % (", ".join(dValues.keys()),
                                                                   ", ".join(dValues.values()))
    if len(data) > 0:
      cmd = 'UPDATE `ProxyDB_CleanProxies` SET %s WHERE UserDN = %s' % (", ".join(["%s = %s" % (k, v) for k, v in dValues.items()]), sUserDN)
      pem = data[0][1]
      if pem:
        remainingSecsInDB = data[0][0]
        if remainingSecs <= remainingSecsInDB:
          self.log.info("Proxy stored is longer than uploaded, omitting.",
                        "%s in uploaded, %s in db" % (remainingSecs, remainingSecsInDB))
          return S_OK()

    retVal = Registry.getUsernameForDN(userDN)
    if not retVal['OK']:
      return retVal
    userName = retVal['Value']

    self.logAction("store proxy", userName, 'any', userName, 'any')
    return self._update(cmd)
  
  def __getPemAndTimeLeft(self, userDN, userGroup, requiredLifeTime=None, vomsAttr=None):
    """ Get proxy from DB and add group

        :param str userDN: user DN
        :param str userGroup: required DIRAC group
        :param int requiredLifeTime: required proxy live time in a seconds
        :param str vomsAttr: if need search VOMS proxy first

        :return: S_OK(tuple)/S_ERROR() -- tuple with proxy as chain and proxy live time in a seconds
    """
    cmd = 'SELECT Pem, TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), ExpirationTime) FROM '
    cmd += '`%%s` WHERE UserDN="%s" AND TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), ExpirationTime) > 0' % userDN
    if vomsAttr:
      # Search VOMS proxy first
      result = self._query(cmd % 'ProxyDB_VOMSProxies' + " AND VOMSAttr=%s AND UserGroup=%s" % (vomsAttr, userGroup))
      if not result['OK']:
        result = self._query(cmd % 'ProxyDB_CleanProxies') 
    else:
      result = self._query(cmd % 'ProxyDB_CleanProxies')
    err = "%s@%s proxy" % (userDN, userGroup)
    if not result['OK']:
      return S_ERROR("%s getting error: %s" % (err, result['Message']))
    data = result['Value']
    if len(data) > 0 and data[0][0]:
      if requiredLifeTime and data[0][1] <= requiredLifeTime:
        return S_ERROR("%s stored in DB, but with less live time that required " % err)
      chain = X509Chain()
      result = chain.loadProxyFromString(data[0][0])
      if result['OK']:
        result = chain.generateProxyToString(requiredLifeTime or min(3600 * 12, data[0][1]), diracGroup=userGroup, rfc=True)
      if not result['OK']:
        return S_ERROR("%s exist in DB, but %s" % (err, result['Message']))
      return S_OK((result['Value'], requiredLifeTime))
    return S_ERROR("%s with %s group is absent in DB" % (userDN, userGroup))

  def __generateProxyForDNGroup(self, userDN, userGroup, requiredLifeTime):
    """ Generate proxy from proxy provider and store it to DB

        :param str userDN: user DN
        :param str userGroup: required DIRAC group
        :param int requiredLifeTime: required proxy live time in a seconds

        :return: S_OK(tuple)/S_ERROR() -- tuple with proxy as chain and proxy live time in a seconds
    """
    # Try to get proxy
    proxyProvider = Registry.getProxyProviderForDN(userDN)
    if not proxyProvider:
      return result
    result = ProxyProviderFactory().getProxyProvider(proxyProvider)
    if not result['OK']:
      return result
    providerObj = result['Value']
    result = providerObj.getProxy(userDN)
    if not result['OK']:
      return result
    proxyStr = result['Value']['proxy']

    # Research proxy
    chain = X509Chain()
    result = chain.loadProxyFromString(proxyStr)
    if not result['OK']:
      return result
    result = chain.getRemainingSecs()
    if not result['OK']:
      return result
    remainingSecs = result['Value']

    # Store proxy
    result = self.__storeProxy(userDN, chain)
    if not result['OK']:
      return result
    
    # Add group
    result = chain.generateProxyToString(requiredLifeTime, diracGroup=userGroup, rfc=True)
    if not result['OK']:
      return S_ERROR("Cannot generate proxy: %s" % result['Message'])
    return S_OK((result['Value'], requiredLifeTime))

  def purgeExpiredProxies(self, sendNotifications=True):
    """ Purge expired requests from the db

        :param boolean sendNotifications: if need to send notification

        :return: S_OK(int)/S_ERROR() -- int is number of purged expired proxies
    """
    purged = 0
    for tableName in ("ProxyDB_CleanProxies", "ProxyDB_Proxies", "ProxyDB_VOMSProxies"):
      cmd = "DELETE FROM `%s` WHERE ExpirationTime < UTC_TIMESTAMP()" % tableName
      result = self._update(cmd)
      if not result['OK']:
        return result
      purged += result['Value']
      self.log.info("Purged %s expired proxies from %s" % (result['Value'], tableName))
    if sendNotifications:
      result = self.sendExpirationNotifications()
      if not result['OK']:
        return result
    return S_OK(purged)

  # WARN: Here userGroup for compatibility
  def deleteProxy(self, userDN, userGroup=None):
    """ Remove proxy of the given user from the repository

        :param str userDN: user DN
        :param str userGroup: DIRAC group

        :return: S_OK()/S_ERROR()
    """
    tables = ['ProxyDB_Proxies', 'ProxyDB_VOMSProxies']
    try:
      userDN = self._escapeString(userDN)['Value']
      if userGroup:
        userGroup = self._escapeString(userGroup)['Value']
        tables.append('ProxyDB_CleanProxies')
    except KeyError:
      return S_ERROR("Invalid DN or group or proxy provider")
    errMsgs = []
    req = "DELETE FROM `%%s` WHERE UserDN=%s" % userDN
    for table in tables:
      result = self._update('%s %s' % (req % table,
                                       'AND UserGroup=%s' % userGroup if userGroup else ''))
      if not result['OK']:
        if result['Message'] not in errMsgs:
          errMsgs.append(result['Message'])
    if errMsgs:
      return S_ERROR(', '.join(errMsgs))
    return result

  # WARN: Old method for compatibility with older versions v7r0-
  @deprecated("Only for DIRAC v6")
  def __getPemAndTimeLeftOld(self, userDN, userGroup, vomsAttr=None):
    """ Get proxy from database

        :param str userDN: user DN
        :param str userGroup: requested DIRAC group
        :param str vomsAttr: VOMS name
        :param str proxyProvider: proxy provider name

        :return: S_OK(tuple)/S_ERROR() -- tuple contain proxy as string and remaining seconds
    """
    try:
      sUserDN = self._escapeString(userDN)['Value']
      if userGroup:
        sUserGroup = self._escapeString(userGroup)['Value']
      if vomsAttr:
        sVomsAttr = self._escapeString(vomsAttr)['Value']
    except KeyError:
      return S_ERROR("Invalid DN or Group")
    if not vomsAttr:
      sTable = "`ProxyDB_Proxies`"
    else:
      sTable = "`ProxyDB_VOMSProxies`"
    cmd = "SELECT Pem, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) from %s " % sTable
    cmd += "WHERE UserDN=%s AND TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > 0" % (sUserDN)
    if userGroup:
      cmd += " AND UserGroup=%s" % sUserGroup
    if vomsAttr:
        cmd += " AND VOMSAttr=%s" % sVomsAttr
    retVal = self._query(cmd)
    if not retVal['OK']:
      return retVal
    data = retVal['Value']
    for record in data:
      if record[0]:
        return S_OK((record[0], record[1]))
    userMask = "%s@%s" % (userDN, userGroup)
    return S_ERROR("%s has no proxy registered" % userMask)

  # WARN: This proxy manager work as myproxy, it seems we no use an external myproxy anymore
  def renewFromMyProxy(self, userDN, userGroup, lifeTime=None, chain=None):
    """ Renew proxy from MyProxy

        :param str userDN: user DN
        :param str userGroup: user group
        :param int lifeTime: needed proxy live time in a seconds
        :param object chain: proxy as X509Chain

        :return: S_OK(X509Chain)/S_ERROR()
    """
    if not lifeTime:
      lifeTime = 43200
    if not self.__useMyProxy:
      return S_ERROR("myproxy is disabled")
    # Get the chain
    if not chain:
      retVal = self.__getPemAndTimeLeft(userDN, userGroup)
      if not retVal['OK']:
        return retVal
      pemData = retVal['Value'][0]
      chain = X509Chain()
      retVal = chain.loadProxyFromString(pemData)
      if not retVal['OK']:
        return retVal

    originChainLifeTime = chain.getRemainingSecs()['Value']
    maxMyProxyLifeTime = self.getMyProxyMaxLifeTime()
    # If we have a chain that's 0.8 of max mplifetime don't ask to mp
    if originChainLifeTime > maxMyProxyLifeTime * 0.8:
      self.log.error("Skipping myproxy download",
                     "user %s %s  chain has %s secs and requested %s secs" % (userDN,
                                                                              userGroup,
                                                                              originChainLifeTime,
                                                                              maxMyProxyLifeTime))
      return S_OK(chain)

    lifeTime *= 1.3
    if lifeTime > maxMyProxyLifeTime:
      lifeTime = maxMyProxyLifeTime
    self.log.info("Renewing proxy from myproxy", "user %s %s for %s secs" % (userDN, userGroup, lifeTime))

    myProxy = MyProxy(server=self.getMyProxyServer())
    retVal = myProxy.getDelegatedProxy(chain, lifeTime)
    if not retVal['OK']:
      return retVal
    mpChain = retVal['Value']
    retVal = mpChain.getRemainingSecs()
    if not retVal['OK']:
      return S_ERROR("Can't retrieve remaining secs from renewed proxy: %s" % retVal['Message'])
    mpChainSecsLeft = retVal['Value']
    if mpChainSecsLeft < originChainLifeTime:
      self.log.info("Chain downloaded from myproxy has less lifetime than the one stored in the db",
                    "\n Downloaded from myproxy: %s secs\n Stored in DB: %s secs" % (mpChainSecsLeft,
                                                                                     originChainLifeTime))
      return S_OK(chain)
    retVal = mpChain.getDIRACGroup()
    if not retVal['OK']:
      return S_ERROR("Can't retrieve DIRAC Group from renewed proxy: %s" % retVal['Message'])
    chainGroup = retVal['Value']
    if chainGroup != userGroup:
      return S_ERROR("Mismatch between renewed proxy group and expected: %s vs %s" % (userGroup, chainGroup))
    retVal = self.__storeProxy(userDN, userGroup, mpChain)
    if not retVal['OK']:
      self.log.error("Cannot store proxy after renewal", retVal['Message'])
    retVal = myProxy.getServiceDN()
    if not retVal['OK']:
      hostDN = userDN
    else:
      hostDN = retVal['Value']
    self.logAction("myproxy renewal", hostDN, "host", userDN, userGroup)
    return S_OK(mpChain)

  def getProxy(self, userName, userGroup, requiredLifeTime=None, voms=False):
    """ Get proxy string from the Proxy Repository for use with userName in the userGroup

        :param str userName: user DN
        :param str userGroup: required DIRAC group
        :param int requiredLifeTime: required proxy live time in a seconds
        :param bool voms: if need VOMS attribute

        :return: S_OK(tuple)/S_ERROR() -- tuple with proxy as chain and proxy live time in a seconds
    """
    # Found DN
    result = Registry.getDNForUsernameInGroup(userName, userGroup)
    if not result['OK']:
      return result
    userDN = result['Value']
    vomsAttr = Registry.getVOMSAttributeForGroup(userGroup)
    if not vomsAttr and voms:
      return S_ERROR("No mapping defined for group %s in the CS" % userGroup)

    # Standard proxy is requested
    self.log.verbose('Try to get proxy from ProxyDB_CleanProxies')
    result = self.__getPemAndTimeLeft(userDN, userGroup, requiredLifeTime, voms and vomsAttr)
    if not result['OK']:

      # WARN: for compatibility
      result = self.__getPemAndTimeLeftOld(userDN, userGroup, voms and vomsAttr)
      if not result['OK'] or requiredLifeTime and timeLeft < requiredLifeTime:

        errMsg = result['Message']
        result = self.__generateProxyForDNGroup(userDN, userGroup, requiredLifeTime)
        if not result['OK']:
          return S_ERROR('%s; %s' % (errMsg, result['Message']))

    pemData, timeLeft = result['Value']
    
    chain = X509Chain()
    result = chain.loadProxyFromString(pemData)
    if not result['OK']:
      return S_ERROR("Checking %s@%s proxy failed: %s" % (userDN, userGroup, result['Message']))

    # Proxy is invalid for some reason, let's delete it
    if not chain.isValidProxy()['OK']:
      self.deleteProxy(userDN, userGroup)
      return S_ERROR("%s@%s has no proxy registered" % (userDN, userGroup))
    
    if voms:
      vomsMgr = VOMS()
      attrs = vomsMgr.getVOMSAttributes(chain).get('Value')
      if attrs and attrs[0]:
        if vomsAttr != attrs[0]:
          return S_ERROR("Stored proxy has already a different VOMS attribute %s than requested %s" %
                          (attrs[0], vomsAttr))
      else:
        retVal = vomsMgr.setVOMSAttributes(chain, vomsAttr, vo=Registry.getVOMSVOForGroup(userGroup))
        if not retVal['OK']:
          return S_ERROR("Cannot append voms extension: %s" % retVal['Message'])
        chain = retVal['Value']
      # We have got the VOMS proxy, store it into the cache
      result = self.__storeVOMSProxy(userDN, userGroup, vomsAttr, chain)
      if not result['OK']:
        return result
      timeLeft = result['Value']

    return S_OK((chain, timeLeft)) 

  def __storeVOMSProxy(self, userDN, userGroup, vomsAttr, chain):
    """ Store VOMS proxy

        :param str userDN: user DN
        :param str userGroup: DIRAC group
        :param str vomsAttr: VOMS attribute
        :param object chain: proxy as X509Chain

        :return: S_OK(str)/S_ERROR()
    """
    retVal = self._getConnection()
    if not retVal['OK']:
      return retVal
    connObj = retVal['Value']
    retVal1 = VOMS().getVOMSProxyInfo(chain, 'actimeleft')
    retVal2 = VOMS().getVOMSProxyInfo(chain, 'timeleft')
    if not retVal1['OK']:
      return retVal1
    if not retVal2['OK']:
      return retVal2
    try:
      vomsSecsLeft1 = int(retVal1['Value'].strip())
      vomsSecsLeft2 = int(retVal2['Value'].strip())
      vomsSecsLeft = min(vomsSecsLeft1, vomsSecsLeft2)
    except Exception as e:
      return S_ERROR("Can't parse VOMS time left: %s" % str(e))
    secsLeft = min(vomsSecsLeft, chain.getRemainingSecs()['Value'])
    pemData = chain.dumpAllToString()['Value']
    result = Registry.getUsernameForDN(userDN)
    if not result['OK']:
      userName = ""
    else:
      userName = result['Value']
    try:
      sUserName = self._escapeString(userName)['Value']
      sUserDN = self._escapeString(userDN)['Value']
      sUserGroup = self._escapeString(userGroup)['Value']
      sVomsAttr = self._escapeString(vomsAttr)['Value']
      sPemData = self._escapeString(pemData)['Value']
    except KeyError:
      return S_ERROR("Could not escape some data")
    cmd = "REPLACE INTO `ProxyDB_VOMSProxies` ( UserName, UserDN, UserGroup, VOMSAttr, Pem, ExpirationTime ) VALUES "
    cmd += "( %s, %s, %s, %s, %s, TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() ) )" % (sUserName, sUserDN, sUserGroup,
                                                                                    sVomsAttr, sPemData, secsLeft)
    result = self._update(cmd, conn=connObj)
    if not result['OK']:
      return result
    return S_OK(secsLeft)

  def getUsers(self, validSecondsLeft=0, userMask=None):
    """ Get all the distinct users from the Proxy Repository. Optionally, only users
        with valid proxies within the given validity period expressed in seconds

        :param int validSecondsLeft: validity period expressed in seconds
        :param str userMask: user name that need to add to search filter

        :return: S_OK(list)/S_ERROR() -- list contain dicts with DN, group, expiration time
    """
    data = []
    sqlCond = []
    if validSecondsLeft:
      try:
        validSecondsLeft = int(validSecondsLeft)
      except ValueError:
        return S_ERROR("Seconds left has to be an integer")
      sqlCond.append("TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > %d" % validSecondsLeft)

    if userMask:
      result = Registry.getDNsForUsername(userMask)
      if not result['OK']:
        return result
      sqlCond.append('UserDN IN ("%s")' % '", "'.join(result['Value']))

    for table, fields in [('ProxyDB_CleanProxies', ("UserDN", "ExpirationTime")),
                          ('ProxyDB_Proxies', ("UserDN", "UserGroup", "ExpirationTime", "PersistentFlag"))]:
      cmd = "SELECT %s FROM `%s`" % (", ".join(fields), table)
      if sqlCond:
        cmd += " WHERE %s" % " AND ".join(sqlCond)
      retVal = self._query(cmd)
      if not retVal['OK']:
        return retVal
      for record in retVal['Value']:
        record = list(record)
        if table == 'ProxyDB_CleanProxies':
          record.insert(1, '')
          record.insert(3, False)
        result = Registry.getUsernameForDN(record[0])
        if not result['OK']:
          gLogger.error("Stored proxy %s has no owner:" % record[0], result['Message'])
          continue
        user = result['Value']
        result = Registry.getGroupsForDN(record[0])
        if not result['OK']:
          gLogger.error("Stored proxy %s has no groups:" % record[0], result['Message'])
          continue
        groups = result['Value']
        data.append({'DN': record[0],
                     'user': user,
                     'group': record[1],  # for compatibility with -v7r0
                     'groups': groups,
                     'expirationtime': record[2],
                     'persistent': record[3] == 'True'})
    return S_OK(data)

  def getCredentialsAboutToExpire(self, requiredSecondsLeft, onlyPersistent=True):
    """ Get credentials about to expire for MyProxy

        :param int requiredSecondsLeft: required seconds left
        :param boolean onlyPersistent: look records only with persistent flag

        :return: S_OK()/S_ERROR()
    """
    cmd = "SELECT UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    cmd += " WHERE TIMESTAMPDIFF( SECOND, ExpirationTime, UTC_TIMESTAMP() ) < %d and " % requiredSecondsLeft
    cmd += "TIMESTAMPDIFF( SECOND, ExpirationTime, UTC_TIMESTAMP() ) > 0"
    if onlyPersistent:
      cmd += " AND PersistentFlag = 'True'"
    return self._query(cmd)

  def setPersistencyFlag(self, userDN, userGroup, persistent=True):
    """ Set the proxy PersistentFlag to the flag value

        :param str userDN: user DN
        :param str userGroup: group name
        :param boolean persistent: enable persistent flag

        :return: S_OK()/S_ERROR()
    """

    try:
      sUserDN = self._escapeString(userDN)['Value']
      sUserGroup = self._escapeString(userGroup)['Value']
    except KeyError:
      return S_ERROR("Can't escape something")
    if persistent:
      sqlFlag = "True"
    else:
      sqlFlag = "False"
    retVal = self._query(
        "SELECT PersistentFlag FROM `ProxyDB_Proxies` WHERE UserDN=%s AND UserGroup=%s" %
        (sUserDN, sUserGroup))
    sqlInsert = True
    if retVal['OK']:
      data = retVal['Value']
      if len(data) > 0:
        sqlInsert = False
        if data[0][0] == sqlFlag:
          return S_OK()
    if sqlInsert:
      # If it's not in the db and we're removing the persistency then do nothing
      if not persistent:
        return S_OK()
      result = Registry.getUsernameForDN(userDN)
      if not result['OK']:
        self.log.error("setPersistencyFlag: Can not retrieve username for DN", userDN)
        return result
      userName = result['Value']
      cmd = "INSERT INTO `ProxyDB_Proxies` ( UserName, UserDN, UserGroup, Pem, ExpirationTime, PersistentFlag ) VALUES "
      cmd += "( %s, %s, %s, '', UTC_TIMESTAMP(), 'True' )" % (userName, sUserDN, sUserGroup)
    else:
      cmd = "UPDATE `ProxyDB_Proxies` SET PersistentFlag='%s' WHERE UserDN=%s AND UserGroup=%s" % (sqlFlag,
                                                                                                   sUserDN,
                                                                                                   sUserGroup)
    retVal = self._update(cmd)
    if not retVal['OK']:
      return retVal
    return S_OK()
  
  def getProxiesContent(self, user=None, group=None):
    """ Get the contents of the db for user/group

        :param str user: user name
        :param str group: group name

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
    """
    where = "WHERE Pem is not NULL"
    if group and user:
      result = Registry.getDNForUserInGroup(user, group)
      if not result['OK']:
        return result
      where += " AND UserDN in (%s)" % self._escapeString(str(result['Value']))['Value']
    elif user:
      result = Registry.getDNsForUsername(username)
      if not result['OK']:
        return result
      where += " AND UserDN in (%s)" % ", ".join([self._escapeString(str(v))['Value'] for v in result['Value']])
    elif group:
      result = Registry.getUsersForGroup(username)
      if not result['OK']:
        return result
      for user in result['Value']:
        result = Registry.getDNsForUsername(username)
        if not result['OK']:
          return result
        where += " AND UserDN in (%s)" % ", ".join([self._escapeString(str(v))['Value'] for v in result['Value']])

    data = []
    for table, fields in [('ProxyDB_CleanProxies', ("UserDN", "ExpirationTime")),
                          ('ProxyDB_Proxies', ("UserDN", "UserGroup", "ExpirationTime", "PersistentFlag"))]:
      result = self._query("SELECT %s FROM `%s` %s ORDER BY UserDN DESC" % (", ".join(fields), table, where))
      if not result['OK']:
        return result
      for record in result['Value']:
        record = list(record)
        if table == 'ProxyDB_CleanProxies':
          record.insert(1, '')
          record.insert(3, False)
        record[3] = record[3] == 'True'
        result = Registry.getUsernameForDN(record[0])
        if not result['OK']:
          self.log.error(result['Message'])
          continue
        record.insert(0, result['Value'])
        data.append(record)
    totalRecords = len(data)
    return S_OK({'ParameterNames': fields, 'Records': data, 'TotalRecords': totalRecords})

  def getProxiesContentOld(self, selDict, sortList, start=0, limit=0):
    """ Get the contents of the db, parameters are a filter to the db

        :param dict selDict: selection dict that contain fields and their posible values
        :param dict sortList: dict with sorting fields
        :param int,long start: search limit start
        :param int,long start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
    """
    if "UserName" in selDict:
      if not selDict.get("UserDN"):
        selDict["UserDN"] = []
      for username in selDict["UserName"]:
        result = Registry.getDNsForUsername(username)
        if result['OK']:
          selDict["UserDN"] += result['Value']
      del selDict["UserName"]
    data = []
    sqlWhere = ["Pem is not NULL"]
    for table, fields in [('ProxyDB_CleanProxies', ("UserDN", "ExpirationTime")),
                          ('ProxyDB_Proxies', ("UserDN", "UserGroup", "ExpirationTime", "PersistentFlag"))]:
      cmd = "SELECT %s FROM `%s`" % (", ".join(fields), table)
      for field in selDict:
        if field not in fields:
          continue
        fVal = selDict[field]
        if isinstance(fVal, (dict, tuple, list)):
          if fVal:
            sqlWhere.append("%s in (%s)" %
                            (field, ", ".join([self._escapeString(str(value))['Value'] for value in fVal])))
        else:
          sqlWhere.append("%s = %s" % (field, self._escapeString(str(fVal))['Value']))
      sqlOrder = []
      if sortList:
        for sort in sortList:
          if len(sort) == 1:
            sort = (sort, "DESC")
          elif len(sort) > 2:
            return S_ERROR("Invalid sort %s" % sort)
          if sort[0] not in fields:
            if sort[0] == 'UserName':
              continue
            if table == 'ProxyDB_CleanProxies' and sort[0] in ['UserGroup', 'PersistentFlag']:
              continue
            return S_ERROR("Invalid sorting field %s" % sort[0])
          if sort[1].upper() not in ("ASC", "DESC"):
            return S_ERROR("Invalid sorting order %s" % sort[1])
          sqlOrder.append("%s %s" % (sort[0], sort[1]))
      if sqlWhere:
        cmd = "%s WHERE %s" % (cmd, " AND ".join(sqlWhere))
      if sqlOrder:
        cmd = "%s ORDER BY %s" % (cmd, ", ".join(sqlOrder))
      if limit:
        try:
          start = int(start)
          limit = int(limit)
        except ValueError:
          return S_ERROR("start and limit have to be integers")
        cmd += " LIMIT %d,%d" % (start, limit)
      retVal = self._query(cmd)
      if not retVal['OK']:
        return retVal
      for record in retVal['Value']:
        record = list(record)
        if table == 'ProxyDB_CleanProxies':
          record.insert(1, '')
          record.insert(3, False)
        record[3] = record[3] == 'True'
        result = Registry.getUsernameForDN(record[0])
        if not result['OK']:
          self.log.error(result['Message'])
          continue
        record.insert(0, result['Value'])
        data.append(record)
    totalRecords = len(data)
    return S_OK({'ParameterNames': fields, 'Records': data, 'TotalRecords': totalRecords})

  def logAction(self, action, issuerUsername, issuerGroup, targetUsername, targetGroup):
    """ Add an action to the log

        :param str action: proxy action
        :param str issuerUsername: user DN of issuer
        :param str issuerGroup: DIRAC group of issuer
        :param str targetUsername: user DN of target
        :param str targetGroup: DIRAC group of target

        :return: S_ERROR()
    """
    try:
      sAction = self._escapeString(action)['Value']
      sIssuerUsername = self._escapeString(issuerUsername)['Value']
      sIssuerGroup = self._escapeString(issuerGroup)['Value']
      sTargetUsername = self._escapeString(targetUsername)['Value']
      sTargetGroup = self._escapeString(targetGroup)['Value']
    except KeyError:
      return S_ERROR("Can't escape from death")
    cmd = "INSERT INTO `ProxyDB_Log` (Action, IssuerUsername, IssuerGroup, TargetUsername, TargetGroup, Timestamp) VALUES "
    cmd += "(%s, %s, %s, %s, %s, UTC_TIMESTAMP())" % (sAction, sIssuerUsername, sIssuerGroup, sTargetUsername, sTargetGroup)
    retVal = self._update(cmd)
    if not retVal['OK']:
      self.log.error("Can't add a proxy action log: ", retVal['Message'])

  def purgeLogs(self):
    """ Purge expired requests from the db

        :return: S_OK()/S_ERROR()
    """
    cmd = "DELETE FROM `ProxyDB_Log` WHERE TIMESTAMPDIFF( SECOND, Timestamp, UTC_TIMESTAMP() ) > 15552000"
    return self._update(cmd)

  def getLogsContent(self, selDict, sortList, start=0, limit=0):
    """ Function to get the contents of the logs table parameters are a filter to the db

        :param dict selDict: filters
        :param list sortList: sort list
        :param int start: start number
        :param int limit: limit

        :return: S_OK()/S_ERROR()
    """
    fields = ("Action", "IssuerUsername", "IssuerGroup", "TargetUsername", "TargetGroup", "Timestamp")
    cmd = "SELECT %s FROM `ProxyDB_Log`" % ", ".join(fields)
    if selDict:
      qr = []
      if 'beforeDate' in selDict:
        qr.append("Timestamp < %s" % self._escapeString(selDict['beforeDate'])['Value'])
        del selDict['beforeDate']
      if 'afterDate' in selDict:
        qr.append("Timestamp > %s" % self._escapeString(selDict['afterDate'])['Value'])
        del selDict['afterDate']
      for field in selDict:
        qr.append("(%s)" %
                  " OR ".join(["%s=%s" %
                               (field, self._escapeString(str(value))['Value']) for value in selDict[field]]))
      whereStr = " WHERE %s" % " AND ".join(qr)
      cmd += whereStr
    else:
      whereStr = ""
    if sortList:
      cmd += " ORDER BY %s" % ", ".join(["%s %s" % (sort[0], sort[1]) for sort in sortList])
    if limit:
      cmd += " LIMIT %d,%d" % (start, limit)
    retVal = self._query(cmd)
    if not retVal['OK']:
      return retVal
    data = retVal['Value']
    totalRecords = len(data)
    cmd = "SELECT COUNT( Timestamp ) FROM `ProxyDB_Log`"
    cmd += whereStr
    retVal = self._query(cmd)
    if retVal['OK']:
      totalRecords = retVal['Value'][0][0]
    return S_OK({'ParameterNames': fields, 'Records': data, 'TotalRecords': totalRecords})

  def generateToken(self, requesterUsername, requesterGroup, numUses=1, lifeTime=0, retries=10):
    """ Generate and return a token and the number of uses for the token

        :param str requesterUsername: DN of requester
        :param str requesterGroup: DIRAC group of requester
        :param int numUses: number of uses
        :param int lifeTime: proxy live time in a seconds
        :param int retries: number of retries

        :return: S_OK(tuple)/S_ERROR() -- tuple with token and number of uses
    """
    if not lifeTime:
      lifeTime = gConfig.getValue("/DIRAC/VOPolicy/TokenLifeTime", self.__defaultTokenLifetime)
    maxUses = gConfig.getValue("/DIRAC/VOPolicy/TokenMaxUses", self.__defaultTokenMaxUses)
    numUses = max(1, min(numUses, maxUses))
    m = hashlib.md5()
    rndData = "%s.%s.%s.%s" % (time.time(), random.random(), numUses, lifeTime)
    m.update(rndData)
    token = m.hexdigest()
    fieldsSQL = ", ".join(("Token", "RequesterUsername", "RequesterGroup", "ExpirationTime", "UsesLeft"))
    valuesSQL = ", ".join((self._escapeString(token)['Value'],
                           self._escapeString(requesterUsername)['Value'],
                           self._escapeString(requesterGroup)['Value'],
                           "TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() )" % int(lifeTime),
                           str(numUses)))

    insertSQL = "INSERT INTO `ProxyDB_Tokens` ( %s ) VALUES ( %s )" % (fieldsSQL, valuesSQL)
    result = self._update(insertSQL)
    if result['OK']:
      return S_OK((token, numUses))
    if result['Message'].find("uplicate entry") > -1:
      if retries:
        return self.generateToken(numUses, lifeTime, retries - 1)
      return S_ERROR("Max retries reached for token generation. Aborting")
    return result

  def purgeExpiredTokens(self):
    """ Purge expired tokens from the db

        :return: S_OK(boolean)/S_ERROR()
    """
    delSQL = "DELETE FROM `ProxyDB_Tokens` WHERE ExpirationTime < UTC_TIMESTAMP() OR UsesLeft < 1"
    return self._update(delSQL)

  def useToken(self, token, requesterUsername, requesterGroup):
    """ Uses of token count

        :param str token: token
        :param str requesterUsername: user name of requester
        :param str requesterGroup: DIRAC group of requester

        :return: S_OK(boolean)/S_ERROR()
    """
    sqlCond = " AND ".join(("UsesLeft > 0",
                            "Token=%s" % self._escapeString(token)['Value'],
                            "RequesterUsername=%s" % self._escapeString(requesterUsername)['Value'],
                            "RequesterGroup=%s" % self._escapeString(requesterGroup)['Value'],
                            "ExpirationTime >= UTC_TIMESTAMP()"))
    updateSQL = "UPDATE `ProxyDB_Tokens` SET UsesLeft = UsesLeft - 1 WHERE %s" % sqlCond
    result = self._update(updateSQL)
    if not result['OK']:
      return result
    return S_OK(result['Value'] > 0)

  def __cleanExpNotifs(self):
    """ Clean expired notifications from the db

        :return: S_OK()/S_ERROR()
    """
    cmd = "DELETE FROM `ProxyDB_ExpNotifs` WHERE ExpirationTime < UTC_TIMESTAMP()"
    return self._update(cmd)

  def sendExpirationNotifications(self):
    """ Send notification about expiration

        :return: S_OK(list)/S_ERROR() -- tuple list of user DN, group and proxy left time
    """
    result = self.__cleanExpNotifs()
    if not result['OK']:
      return result
    cmd = "SELECT UserDN, UserGroup, LifeLimit FROM `ProxyDB_ExpNotifs`"
    result = self._query(cmd)
    if not result['OK']:
      return result
    notifDone = dict([((row[0], row[1]), row[2]) for row in result['Value']])
    notifLimits = sorted([int(x) for x in self.getCSOption("NotificationTimes", ProxyDB.NOTIFICATION_TIMES)])
    sqlSel = "UserDN, UserGroup, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime )"
    sqlCond = "TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) < %d" % max(notifLimits)
    cmd = "SELECT %s FROM `ProxyDB_Proxies` WHERE %s" % (sqlSel, sqlCond)
    result = self._query(cmd)
    if not result['OK']:
      return result
    pilotProps = (Properties.GENERIC_PILOT, Properties.PILOT)
    data = result['Value']
    sent = []
    for row in data:
      userDN, group, lTime = row
      # If it's a pilot proxy, skip it
      if Registry.groupHasProperties(group, pilotProps):
        continue
      # IF it dosn't hace the auto upload proxy, skip it
      if not Registry.getGroupOption(group, "AutoUploadProxy", False):
        continue
      notKey = (userDN, group)
      for notifLimit in notifLimits:
        if notifLimit < lTime:
          # Not yet in this notification limit
          continue
        if notKey in notifDone and notifDone[notKey] <= notifLimit:
          # Already notified for this notification limit
          break
        if not self._notifyProxyAboutToExpire(userDN, group, lTime):
          # Cannot send notification, retry later
          break
        try:
          sUserDN = self._escapeString(userDN)['Value']
          sGroup = self._escapeString(group)['Value']
        except KeyError:
          return S_ERROR("OOPS")
        if notKey not in notifDone:
          values = "( %s, %s, %d, TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ) )" % (sUserDN, sGroup, notifLimit, lTime)
          cmd = "INSERT INTO `ProxyDB_ExpNotifs` ( UserDN, UserGroup, LifeLimit, ExpirationTime ) VALUES %s" % values
          result = self._update(cmd)
          if not result['OK']:
            gLogger.error("Could not mark notification as sent", result['Message'])
        else:
          values = "LifeLimit = %d, ExpirationTime = TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() )" % (notifLimit, lTime)
          cmd = "UPDATE `ProxyDB_ExpNotifs` SET %s WHERE UserDN = %s AND UserGroup = %s" % (values, sUserDN, sGroup)
          result = self._update(cmd)
          if not result['OK']:
            gLogger.error("Could not mark notification as sent", result['Message'])
        sent.append((userDN, group, lTime))
        notifDone[notKey] = notifLimit
    return S_OK(sent)

  def _notifyProxyAboutToExpire(self, userDN, userGroup, lTime):
    """ Send notification mail about to expire

        :param str userDN: user DN
        :param str userGroup: DIRAC group
        :param int lTime: left proxy live time in a seconds

        :return: boolean
    """
    result = Registry.getUsernameForDN(userDN)
    if not result['OK']:
      return False
    userName = result['Value']
    userEMail = Registry.getUserOption(userName, "Email", "")
    if not userEMail:
      gLogger.error("Could not discover user email", userName)
      return False
    daysLeft = int(lTime / 86400)
    msgSubject = "Your proxy uploaded to DIRAC will expire in %d days" % daysLeft
    msgBody = """\
Dear %s,

  The proxy you uploaded to DIRAC will expire in aproximately %d days. The proxy
  information is:

  DN:    %s
  Group: %s

  If you plan on keep using this credentials please upload a newer proxy to
  DIRAC by executing:

  $ dirac-proxy-init -P -g %s --rfc

  If you have been issued different certificate, please make sure you have a
  proxy uploaded with that certificate.

Cheers,
 DIRAC's Proxy Manager
""" % (userName, daysLeft, userDN, userGroup, userGroup)
    fromAddr = self.getFromAddr()
    result = self.__notifClient.sendMail(userEMail, msgSubject, msgBody, fromAddress=fromAddr)
    if not result['OK']:
      gLogger.error("Could not send email", result['Message'])
      return False
    return True

  # WARN: Old method for compatibility with older versions v7r0-
  @deprecated("Only for DIRAC v6")
  def __storeProxyOld(self, userDN, userGroup, chain):
    """ Store user proxy into the Proxy repository for a user specified by his
        DN and group. Old method.
    """
    retVal = Registry.getUsernameForDN(userDN)
    if not retVal['OK']:
      return retVal
    userName = retVal['Value']
    # Get remaining secs
    retVal = chain.getRemainingSecs()
    if not retVal['OK']:
      return retVal
    remainingSecs = retVal['Value']
    if remainingSecs < self._minSecsToAllowStore:
      return S_ERROR(
          "Cannot store proxy, remaining secs %s is less than %s" %
          (remainingSecs, self._minSecsToAllowStore))

    # Compare the DNs
    retVal = chain.getIssuerCert()
    if not retVal['OK']:
      return retVal
    proxyIdentityDN = retVal['Value'].getSubjectDN()['Value']
    if not userDN == proxyIdentityDN:
      msg = "Mismatch in the user DN"
      vMsg = "Proxy says %s and credentials are %s" % (proxyIdentityDN, userDN)
      self.log.error(msg, vMsg)
      return S_ERROR("%s. %s" % (msg, vMsg))
    # Check the groups
    retVal = chain.getDIRACGroup()
    if not retVal['OK']:
      return retVal
    proxyGroup = retVal['Value']
    if not proxyGroup:
      proxyGroup = Registry.getDefaultUserGroup()
    if not userGroup == proxyGroup:
      msg = "Mismatch in the user group"
      vMsg = "Proxy says %s and credentials are %s" % (proxyGroup, userGroup)
      self.log.error(msg, vMsg)
      return S_ERROR("%s. %s" % (msg, vMsg))
    # Check if its limited
    if chain.isLimitedProxy()['Value']:
      return S_ERROR("Limited proxies are not allowed to be stored")
    dLeft = remainingSecs / 86400
    hLeft = remainingSecs / 3600 - dLeft * 24
    mLeft = remainingSecs / 60 - hLeft * 60 - dLeft * 1440
    sLeft = remainingSecs - hLeft * 3600 - mLeft * 60 - dLeft * 86400
    self.log.info("Storing proxy for credentials %s (%d:%02d:%02d:%02d left)" %
                  (proxyIdentityDN, dLeft, hLeft, mLeft, sLeft))

    try:
      sUserDN = self._escapeString(userDN)['Value']
      sUserGroup = self._escapeString(userGroup)['Value']
    except KeyError:
      return S_ERROR("Cannot escape DN")
    # Check what we have already got in the repository
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ), Pem "
    cmd += "FROM `ProxyDB_Proxies` WHERE UserDN=%s AND UserGroup=%s" % (
        sUserDN, sUserGroup)
    result = self._query(cmd)
    if not result['OK']:
      return result
    # check if there is a previous ticket for the DN
    data = result['Value']
    sqlInsert = True
    if len(data) > 0:
      sqlInsert = False
      pem = data[0][1]
      if pem:
        remainingSecsInDB = data[0][0]
        if remainingSecs <= remainingSecsInDB:
          self.log.info(
              "Proxy stored is longer than uploaded, omitting.",
              "%s in uploaded, %s in db" %
              (remainingSecs,
               remainingSecsInDB))
          return S_OK()

    pemChain = chain.dumpAllToString()['Value']
    dValues = {'UserName': self._escapeString(userName)['Value'],
               'UserDN': sUserDN,
               'UserGroup': sUserGroup,
               'Pem': self._escapeString(pemChain)['Value'],
               'ExpirationTime': 'TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() )' % int(remainingSecs),
               'PersistentFlag': "'False'"}
    if sqlInsert:
      sqlFields = []
      sqlValues = []
      for key in dValues:
        sqlFields.append(key)
        sqlValues.append(dValues[key])
      cmd = "INSERT INTO `ProxyDB_Proxies` ( %s ) VALUES ( %s )" % (", ".join(sqlFields), ", ".join(sqlValues))
    else:
      sqlSet = []
      sqlWhere = []
      for k in dValues:
        if k in ('UserDN', 'UserGroup'):
          sqlWhere.append("%s = %s" % (k, dValues[k]))
        else:
          sqlSet.append("%s = %s" % (k, dValues[k]))
      cmd = "UPDATE `ProxyDB_Proxies` SET %s WHERE %s" % (", ".join(sqlSet), " AND ".join(sqlWhere))

    self.logAction("store proxy", userName, userGroup, userName, userGroup)
    return self._update(cmd)
