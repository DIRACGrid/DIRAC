""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id$"

import os
import glob
import time
import urllib
import random
import hashlib
import commands

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security.MyProxy import MyProxy
from DIRAC.Core.Security.X509Request import X509Request
from DIRAC.Core.Security.X509Chain import X509Chain, isPUSPdn
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, Resources
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
# pylint: disable=import-error,no-name-in-module
from DIRAC.Resources.ProxyProvider.ProxyProviderFactory import ProxyProviderFactory


class ProxyDB(DB):

  NOTIFICATION_TIMES = [2592000, 1296000]

  def __init__(self,
               useMyProxy=False):
    DB.__init__(self, 'ProxyDB', 'Framework/ProxyDB')
    random.seed()
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

        :return: basestring
    """
    return gConfig.getValue("/DIRAC/VOPolicy/MyProxyServer", "myproxy.cern.ch")

  def getMyProxyMaxLifeTime(self):
    """ Get a maximum of the proxy lifetime delegated by MyProxy

        :return: int -- time in a seconds
    """
    return gConfig.getValue("/DIRAC/VOPolicy/MyProxyMaxDelegationTime", 168) * 3600

  def getFromAddr(self):
    """ Get the From address to use in proxy expiry e-mails.

        :return: basestring
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
      tablesD['ProxyDB_CleanProxies'] = {'Fields': {'UserName': 'VARCHAR(64) NOT NULL',
                                                    'UserDN': 'VARCHAR(255) NOT NULL',
                                                    'ProxyProvider': 'VARCHAR(64) DEFAULT "Certificate"',
                                                    'Pem': 'BLOB',
                                                    'ExpirationTime': 'DATETIME',
                                                    },
                                         'PrimaryKey': ['UserDN', 'ProxyProvider']
                                         }
    # FIXME: Need to delete this table in v7
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
                                           'IssuerDN': 'VARCHAR(255) NOT NULL',
                                           'IssuerGroup': 'VARCHAR(255) NOT NULL',
                                           'TargetDN': 'VARCHAR(255) NOT NULL',
                                           'TargetGroup': 'VARCHAR(255) NOT NULL',
                                           'Action': 'VARCHAR(128) NOT NULL',
                                           'Timestamp': 'DATETIME',
                                           },
                                'PrimaryKey': 'ID',
                                'Indexes': {'Timestamp': ['Timestamp']}
                                }
    # FIXME: This table is needed in v7?
    if 'ProxyDB_Tokens' not in tablesInDB:
      tablesD['ProxyDB_Tokens'] = {'Fields': {'Token': 'VARCHAR(64) NOT NULL',
                                              'RequesterDN': 'VARCHAR(255) NOT NULL',
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

        :param basestring tableName: table name

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
    for tableName in ("ProxyDB_CleanProxies", "ProxyDB_Proxies", "ProxyDB_VOMSProxies"):
      result = self._query("describe `%s`" % tableName)
      if not result['OK']:
        return result
      if 'UserName' not in [row[0] for row in result['Value']]:
        self.log.notice("Username missing in table %s schema. Adding it" % tableName)
        result = self.__addUserNameToTable(tableName)
        if not result['OK']:
          return result

  def generateDelegationRequest(self, proxyChain, userDN):
    """ Generate a request and store it for a given proxy Chain

        :param X509Chain() proxyChain: proxy as chain
        :param basestring userDN: user DN

        :return: S_OK(dict)/S_ERROR() -- dict contain id and proxy as string of the request
    """
    retVal = self._getConnection()
    if not retVal['OK']:
      return retVal
    connObj = retVal['Value']
    retVal = proxyChain.generateProxyRequest()
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
      sUserDN = self._escapeString(userDN)['Value']
      sAllStr = self._escapeString(allStr)['Value']
    except KeyError:
      return S_ERROR("Cannot escape DN")
    cmd = "INSERT INTO `ProxyDB_Requests` ( Id, UserDN, Pem, ExpirationTime )"
    cmd += " VALUES ( 0, %s, %s, TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() ) )" % (sUserDN,
                                                                                   sAllStr,
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
    # retVal = proxyChain.getDIRACGroup()
    # if retVal['OK'] and retVal['Value']:
    #   userGroup = retVal['Value']
    # else:
    #   userGroup = "unset"
    userGroup = proxyChain.getDIRACGroup().get('Value') or "unset"
    self.logAction("request upload", userDN, userGroup, userDN, "any")
    # Here we go!
    return S_OK({'id': data[0][0], 'request': reqStr})

  def retrieveDelegationRequest(self, requestId, userDN):
    """ Retrieve a request from the DB

        :param int requestId: id of the request
        :param basestring userDN: user DN

        :return: S_OK(basestring)/S_ERROR()
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
        :param basestring userDN: user DN
        :param basestring delegatedPem: delegated proxy as string

        :return: S_OK()/S_ERROR()
    """
    retVal = self.retrieveDelegationRequest(requestId, userDN)
    if not retVal['OK']:
      return retVal
    request = retVal['Value']
    chain = X509Chain(keyObj=request.getPKey())
    retVal = chain.loadChainFromString(delegatedPem)
    if not retVal['OK']:
      return retVal
    retVal = chain.isValidProxy()#ignoreDefault=True)
    if not retVal['OK']:
      return retVal
    
      # if DErrno.cmpError(retVal, DErrno.ENOGROUP):
      #   # For proxies without embedded DIRAC group only one default is allowed
      #   # Cleaning all the proxies for this DN if any before uploading the new one.
      #   retVal = self.deleteProxy(userDN)
      #   if not retVal['OK']:
      #     return retVal
      # else:
      #   return retVal

    result = chain.isVOMS() # FIXME: Add exception when is group extention also
    if result['OK'] and result.get('Value'):
      return S_ERROR("Proxies with VOMS extensions are not allowed to be uploaded")

    retVal = request.checkChain(chain)
    if not retVal['OK']:
      return retVal
    if not retVal['Value']:
      return S_ERROR("Received chain does not match request: %s" % retVal['Message'])

    retVal = chain.getDIRACGroup(ignoreDefault=True)
    if not retVal['OK']:
      return retVal
    if retVal['Value']:
      return S_ERROR("Proxies with DIRAC group extensions are not allowed to be uploaded: %s" % retVal['Value'])
    #  userGroup = retVal['Value']

    # retVal = Registry.getGroupsForDN(userDN)
    # if not retVal['OK']:
    #   return retVal
    # if userGroup and userGroup not in retVal['Value']:
    #   return S_ERROR("%s group is not valid for %s" % (userGroup, userDN))

    retVal = self.storeProxy(userDN, chain)#, userGroup
    if not retVal['OK']:
      return retVal
    retVal = self.deleteRequest(requestId)
    if not retVal['OK']:
      return retVal
    return S_OK()

  # FIXME: For v7 need to delete userGroup parameter
  def storeProxy(self, userDN, chain, proxyProvider=None):#userGroup,
    """ Store user proxy into the Proxy repository for a user specified by his
        DN and group or proxy provider.

        :param basestring userDN: user DN from proxy
        :param basestring,boolean userGroup: group extension from proxy
        :param X509Chain() chain: proxy chain
        :param basestring proxyProvider: proxy provider name. In case this
               parameter set userGroup is ignored

        :return: S_OK()/S_ERROR()
    """
    retVal = Registry.getUsernameForDN(userDN)
    if not retVal['OK']:
      return retVal
    userName = retVal['Value']

    if not proxyProvider:
      result = Registry.getProxyProvidersForDN(userDN)
      if result['OK']:
        proxyProvider = result['Value'][0] 

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
    if userDN != proxyIdentityDN:
      msg = "Mismatch in the user DN"
      vMsg = "Proxy says %s and credentials are %s" % (proxyIdentityDN, userDN)
      self.log.error(msg, vMsg)
      return S_ERROR("%s. %s" % (msg, vMsg))

    # Check the groups
    # if userGroup and not proxyProvider:
    #   retVal = chain.getDIRACGroup()
    #   if not retVal['OK']:
    #     return retVal
    #   proxyGroup = retVal['Value']
    #   if not proxyGroup:
    #     proxyGroup = Registry.getDefaultUserGroup()
    #   if userGroup != proxyGroup:
    #     msg = "Mismatch in the user group"
    #     vMsg = "Proxy says %s and credentials are %s" % (proxyGroup, userGroup)
    #     self.log.error(msg, vMsg)
    #     return S_ERROR("%s. %s" % (msg, vMsg))

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
      # if userGroup and not proxyProvider:
      #   sUserGroup = self._escapeString(userGroup)['Value']
      #   sTable = 'ProxyDB_Proxies'
      # else:
      sTable = 'ProxyDB_CleanProxies'
    except KeyError:
      return S_ERROR("Cannot escape DN")

    # Check what we have already got in the repository
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ), Pem "
    cmd += "FROM `%s` WHERE UserDN=%s " % (sTable, sUserDN)
    # if proxyProvider:
    #   cmd += 'AND ProxyProvider="%s"' % proxyProvider
    # else:
    #   cmd += "AND UserGroup=%s" % sUserGroup
    result = self._query(cmd)
    if not result['OK']:
      return result

    # Check if there is a previous ticket for the DN
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
               'Pem': self._escapeString(pemChain)['Value'],
               'ExpirationTime': 'TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() )' % int(remainingSecs)}
    # if userGroup and not proxyProvider:
    #   dValues['UserGroup'] = sUserGroup
    #   dValues['PersistentFlag'] = "'False'"
    # else:
    if proxyProvider:
      dValues['ProxyProvider'] = "'%s'" % proxyProvider
    if sqlInsert:
      sqlFields = []
      sqlValues = []
      for key in dValues:
        sqlFields.append(key)
        sqlValues.append(dValues[key])
      cmd = "INSERT INTO `%s` ( %s ) VALUES ( %s )" % (sTable, ", ".join(sqlFields), ", ".join(sqlValues))
    else:
      sqlSet = []
      sqlWhere = []
      for k in dValues:
        if k in ('UserDN', 'ProxyProvider'):#, 'UserGroup'
          sqlWhere.append("%s = %s" % (k, dValues[k]))
        else:
          sqlSet.append("%s = %s" % (k, dValues[k]))
      cmd = "UPDATE `%s` SET %s WHERE %s" % (sTable, ", ".join(sqlSet), " AND ".join(sqlWhere))

    # if userGroup and not proxyProvider:
    #   self.logAction("store proxy", userDN, userGroup, userDN, userGroup)
    # else:
    #   self.logAction("store clean proxy", userDN, proxyProvider, userDN, proxyProvider)
    self.logAction("store proxy", userDN, proxyProvider, userDN, proxyProvider)
    return self._update(cmd)

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

  def deleteProxy(self, userDN, userGroup='any', proxyProvider=None):
    """ Remove proxy of the given user from the repository

        :param basestring userDN: user DN
        :param basestring userGroup: DIRAC group
        :param basestring proxyProvider: proxy provider name

        :return: S_OK()/S_ERROR()
    """
    try:
      userDN = self._escapeString(userDN)['Value']
      if userGroup != 'any':
        userGroup = self._escapeString(userGroup)['Value']
      if proxyProvider:
        proxyProvider = self._escapeString(proxyProvider)['Value']
    except KeyError:
      return S_ERROR("Invalid DN or group or proxy provider")
    errMsgs = []
    req = "DELETE FROM `%%s` WHERE UserDN=%s" % userDN
    if proxyProvider:
      ppReq = '%s AND ProxyProvider=%s' % (req, proxyProvider)
      result = self._update(ppReq % 'ProxyDB_CleanProxies')
      if not result['OK']:
        errMsgs += result['Message']
    if userGroup != 'any':
      req += " AND UserGroup=%s" % userGroup
    for db in ['ProxyDB_Proxies', 'ProxyDB_VOMSProxies']:
      result = self._update(req % db)
      if not result['OK']:
        errMsgs += result['Message']
    if errMsgs:
      return S_ERROR(', '.join(errMsgs))
    return result

  def __getPemAndTimeLeft(self, userDN, userGroup=False, vomsAttr=False, proxyProvider=False):
    """ Get proxy from database

        :param basestring userDN: user DN
        :param basestring userGroup: requested DIRAC group
        :param basestring vomsAttr: VOMS name
        :param basestring proxyProvider: proxy provider name

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
    if proxyProvider:
      sTable = "`ProxyDB_CleanProxies`"
    elif not vomsAttr:
      sTable = "`ProxyDB_Proxies`"
    else:
      sTable = "`ProxyDB_VOMSProxies`"
    cmd = "SELECT Pem, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) from %s " % sTable
    cmd += "WHERE UserDN=%s AND TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > 0" % (sUserDN)
    if proxyProvider:
      cmd += ' AND ProxyProvider="%s"' % proxyProvider
    else:
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
        if proxyProvider:
          chain = X509Chain()
          result = chain.loadProxyFromString(record[0])
          if not result['OK']:
            return result
          result = chain.generateProxyToString(record[1], diracGroup=userGroup, rfc=True)
          if not result['OK']:
            return result
          return S_OK((result['Value'], record[1]))
        return S_OK((record[0], record[1]))
    if userGroup:
      userMask = "%s@%s" % (userDN, userGroup)
    else:
      userMask = userDN
    return S_ERROR("%s has no proxy registered" % userMask)

  def renewFromMyProxy(self, userDN, userGroup, lifeTime=False, chain=False):
    """ Renew proxy from MyProxy

        :param basestring userDN: user DN
        :param basestring userGroup: user group
        :param int lifeTime: needed proxy live time in a seconds
        :param X509Chain() chain: proxy as chain

        :return: S_OK(X509Chain())/S_ERROR()
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
    retVal = self.storeProxy(userDN, userGroup, mpChain)
    if not retVal['OK']:
      self.log.error("Cannot store proxy after renewal", retVal['Message'])
    retVal = myProxy.getServiceDN()
    if not retVal['OK']:
      hostDN = userDN
    else:
      hostDN = retVal['Value']
    self.logAction("myproxy renewal", hostDN, "host", userDN, userGroup)
    return S_OK(mpChain)

  # FIXME: this method not need if DIRAC setup use DNProperties section in configuration
  def __getPUSProxy(self, userDN, userGroup, requiredLifetime, requestedVOMSAttr=None):
    
    result = Registry.getGroupsForDN(userDN)
    if not result['OK']:
      return result

    validGroups = result['Value']
    if userGroup not in validGroups:
      return S_ERROR('Invalid group %s for user' % userGroup)

    voName = Registry.getVOForGroup(userGroup)
    if not voName:
      return S_ERROR('Can not determine VO for group %s' % userGroup)

    retVal = self.__getVOMSAttribute(userGroup, requestedVOMSAttr)
    if not retVal['OK']:
      return retVal
    vomsAttribute = retVal['Value']['attribute']
    vomsVO = retVal['Value']['VOMSVO']

    puspServiceURL = Registry.getVOOption(voName, 'PUSPServiceURL')
    if not puspServiceURL:
      return S_ERROR('Can not determine PUSP service URL for VO %s' % voName)

    user = userDN.split(":")[-1]

    puspURL = "%s?voms=%s:%s&proxy-renewal=false&disable-voms-proxy=false" \
              "&rfc-proxy=true&cn-label=user:%s" % (puspServiceURL, vomsVO, vomsAttribute, user)

    try:
      proxy = urllib.urlopen(puspURL).read()
    except Exception as e:
      return S_ERROR('Failed to get proxy from the PUSP server')

    chain = X509Chain()
    chain.loadChainFromString(proxy)
    chain.loadKeyFromString(proxy)

    result = chain.getCredentials()
    if not result['OK']:
      return S_ERROR('Failed to get a valid PUSP proxy')
    credDict = result['Value']
    if credDict['identity'] != userDN:
      return S_ERROR('Requested DN does not match the obtained one in the PUSP proxy')
    timeLeft = credDict['secondsLeft']

    result = chain.generateProxyToString(timeLeft, diracGroup=userGroup)
    if not result['OK']:
      return result
    proxyString = result['Value']
    return S_OK((proxyString, timeLeft))

  def __generateProxyFromProxyProvider(self, userDN, proxyProvider):
    """ Get proxy from proxy provider

        :param basestring userDN: user DN for what need to create proxy
        :param basestring proxyProvider: proxy provider name that will ganarete proxy

        :return: S_OK(dict)/S_ERROR() -- dict with remaining seconds, proxy as a string and as a chain
    """
    if not proxyProvider:
      return S_ERROR('No proxy providers found for "%s" user DN' % userDN)
    gLogger.info('Getting proxy from proxyProvider', '(for "%s" DN by "%s")' % (userDN, proxyProvider))
    result = ProxyProviderFactory().getProxyProvider(proxyProvider)
    if not result['OK']:
      return result
    pp = result['Value']
    result = pp.getProxy({"DN": userDN})
    if not result['OK']:
      return result
    proxyStr = result['Value']
    chain = X509Chain()
    result = chain.loadProxyFromString(proxyStr)
    if not result['OK']:
      return result
    result = chain.getRemainingSecs()
    if not result['OK']:
      return result
    remainingSecs = result['Value']
    result = self.storeProxy(userDN, False, chain, proxyProvider)
    if result['OK']:
      return S_OK({'proxy': proxyStr, 'chain': chain, 'remainingSecs': remainingSecs})
    return result

  def __getProxyFromProxyProviders(self, userDN, userGroup, requiredLifeTime):
    """ Generate new proxy from exist clean proxy or from proxy provider
        for use with userDN in the userGroup

        :param basestring userDN: user DN
        :param basestring userGroup: required group name
        :param int requiredLifeTime: required proxy live time in a seconds

        :return: S_OK(tuple)/S_ERROR() -- tuple contain proxy as string and remainig seconds
    """
    result = Registry.getProxyProvidersForDN(userDN)
    if isPUSPdn(userDN):
      result = S_OK(['PUSP'])
    if result['OK']:
      for proxyProvider in result['Value']:
        result = self.__getPemAndTimeLeft(userDN, userGroup, proxyProvider=proxyProvider or 'Certificate')
        if result['OK'] and (not requiredLifeTime or result['Value'][1] > requiredLifeTime):
          return result
        result = self.__generateProxyFromProxyProvider(userDN, proxyProvider)
        if result['OK']:
          chain = result['Value']['chain']
          remainingSecs = result['Value']['remainingSecs']
          result = chain.generateProxyToString(remainingSecs, diracGroup=userGroup, rfc=True)
          if result['OK']:
            return S_OK((result['Value'], remainingSecs))
    return S_ERROR('Cannot generate proxy%s' %
                   (result.get('Message') and ': ' + result.get('Message') or ''))

  def getProxy(self, userDN, userGroup, requiredLifeTime=False):
    """ Get proxy string from the Proxy Repository for use with userDN
        in the userGroup

        :param basestring userDN: user DN
        :param basestring userGroup: required DIRAC group
        :param int requiredLifeTime: required proxy live time in a seconds

        :return: S_OK(tuple)/S_ERROR() -- tuple with proxy as chain and proxy live time in a seconds
    """
    # FIXME: this block not need if DIRAC setup use DNProperties section in configuration
    # Get the Per User SubProxy if one is requested
    if isPUSPdn(userDN):
      result = self.__getPUSProxy(userDN, userGroup, requiredLifeTime)
      if not result['OK']:
        return result
      pemData = result['Value'][0]
      timeLeft = result['Value'][1]
      chain = X509Chain()
      result = chain.loadProxyFromString(pemData)
      if not result['OK']:
        return result
      return S_OK((chain, timeLeft))

    # Standard proxy is requested
    errMsg = "Can't get proxy%s: " % (requiredLifeTime and ' for %s seconds' % requiredLifeTime or '')
    retVal = self.__getPemAndTimeLeft(userDN, userGroup)
    if not retVal['OK']:
      errMsg += '%s, try to use proxy provider' %retVal['Message']
      retVal = self.__getProxyFromProxyProviders(userDN, userGroup, requiredLifeTime=requiredLifeTime)
    elif requiredLifeTime:
      if retVal['Value'][1] < requiredLifeTime and not self.__useMyProxy:
        errMsg += 'the time left in the proxy from repository is less than required'
        errMsg += ', try to use proxy provider'
        retVal = self.__getProxyFromProxyProviders(userDN, userGroup, requiredLifeTime=requiredLifeTime)
    if not retVal['OK']:
      return S_ERROR("%s; %s" % (errMsg, retVal['Message']))
    pemData = retVal['Value'][0]
    timeLeft = retVal['Value'][1]
    chain = X509Chain()
    result = chain.loadProxyFromString(pemData)
    if not retVal['OK']:
      return S_ERROR("%s; %s" % (errMsg, retVal['Message']))
    if self.__useMyProxy:
      if requiredLifeTime:
        if timeLeft < requiredLifeTime:
          retVal = self.renewFromMyProxy(userDN, userGroup, lifeTime=requiredLifeTime, chain=chain)
          if not retVal['OK']:
            return S_ERROR("%s; the proxy lifetime from MyProxy is less than required." % errMsg)
          chain = retVal['Value']

    # Proxy is invalid for some reason, let's delete it
    if not chain.isValidProxy()['Value']:
      self.deleteProxy(userDN, userGroup)
      return S_ERROR("%s@%s has no proxy registered" % (userDN, userGroup))
    return S_OK((chain, timeLeft))

  def __getVOMSAttribute(self, userGroup, requiredVOMSAttribute=False):

    if requiredVOMSAttribute:
      return S_OK({'attribute': requiredVOMSAttribute, 'VOMSVO': Registry.getVOMSVOForGroup(userGroup)})

    csVOMSMapping = Registry.getVOMSAttributeForGroup(userGroup)
    if not csVOMSMapping:
      return S_ERROR("No mapping defined for group %s in the CS" % userGroup)

    return S_OK({'attribute': csVOMSMapping, 'VOMSVO': Registry.getVOMSVOForGroup(userGroup)})

  def getVOMSProxy(self, userDN, userGroup, requiredLifeTime=False, requestedVOMSAttr=False):
    """ Get proxy string from the Proxy Repository for use with userDN
        in the userGroup

        :param basestring userDN: user DN
        :param basestring userGroup: required DIRAC group
        :param int requiredLifeTime: required proxy live time in a seconds
        :param basestring requestedVOMSAttr: VOMS name

        :return: S_OK(tuple)/S_ERROR() -- tuple with proxy as chain and proxy live time in a seconds
    """
    retVal = self.__getVOMSAttribute(userGroup, requestedVOMSAttr)
    if not retVal['OK']:
      return retVal
    vomsAttr = retVal['Value']['attribute']
    vomsVO = retVal['Value']['VOMSVO']

    # Look in the cache
    retVal = self.__getPemAndTimeLeft(userDN, userGroup, vomsAttr)
    if retVal['OK']:
      pemData = retVal['Value'][0]
      vomsTime = retVal['Value'][1]
      chain = X509Chain()
      retVal = chain.loadProxyFromString(pemData)
      if retVal['OK']:
        retVal = chain.getRemainingSecs()
        if retVal['OK']:
          remainingSecs = retVal['Value']
          if requiredLifeTime and requiredLifeTime <= vomsTime and requiredLifeTime <= remainingSecs:
            return S_OK((chain, min(vomsTime, remainingSecs)))

    if isPUSPdn(userDN):
      # Get the Per User SubProxy if one is requested
      result = self.__getPUSProxy(userDN, userGroup, requiredLifeTime, requestedVOMSAttr)
      if not result['OK']:
        return result
      pemData = result['Value'][0]
      chain = X509Chain()
      result = chain.loadProxyFromString(pemData)
      if not result['OK']:
        return result

    else:
      # Get the stored proxy and dress it with the VOMS extension
      retVal = self.getProxy(userDN, userGroup, requiredLifeTime)
      if not retVal['OK']:
        return retVal
      chain, secsLeft = retVal['Value']

      if requiredLifeTime and requiredLifeTime > secsLeft:
        return S_ERROR("Stored proxy is not long lived enough")

      vomsMgr = VOMS()

      retVal = vomsMgr.getVOMSAttributes(chain)
      if retVal['OK']:
        attrs = retVal['Value']
        if len(attrs) > 0:
          if attrs[0] != vomsAttr:
            return S_ERROR(
                "Stored proxy has already a different VOMS attribute %s than requested %s" %
                (vomsAttr, attrs[0]))
          else:
            result = self.__storeVOMSProxy(userDN, userGroup, vomsAttr, chain)
            if not result['OK']:
              return result
            secsLeft = result['Value']
            if requiredLifeTime and requiredLifeTime <= secsLeft:
              return S_OK((chain, secsLeft))
            return S_ERROR("Stored proxy has already a different VOMS attribute and is not long lived enough")

      retVal = vomsMgr.setVOMSAttributes(chain, vomsAttr, vo=vomsVO)
      if not retVal['OK']:
        return S_ERROR("Cannot append voms extension: %s" % retVal['Message'])
      chain = retVal['Value']

    # We have got the VOMS proxy, store it into the cache
    result = self.__storeVOMSProxy(userDN, userGroup, vomsAttr, chain)
    if not result['OK']:
      return result
    secsLeft = result['Value']
    return S_OK((chain, secsLeft))

  def __storeVOMSProxy(self, userDN, userGroup, vomsAttr, chain):
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

  def getRemainingTime(self, userDN, userGroup):
    """ Returns the remaining time the proxy is valid in a seconds

        :param basestring userDN: user DN
        :param basestring userGroup: user group

        :return: S_OK(int)/S_ERROR()
    """
    try:
      userDN = self._escapeString(userDN)['Value']
      userGroup = self._escapeString(userGroup)['Value']
    except KeyError:
      return S_ERROR("Invalid DN or group")
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) FROM `ProxyDB_Proxies`"
    retVal = self._query("%s WHERE UserDN = %s AND UserGroup = %s" % (cmd, userDN, userGroup))
    if not retVal['OK']:
      return retVal
    data = retVal['Value']
    if not data:
      return S_OK(0)
    return S_OK(int(data[0][0]))

  def getUsers(self, validSecondsLeft=0, userName=False):
    """ Get all the distinct users from the Proxy Repository. Optionally, only users
        with valid proxies within the given validity period expressed in seconds

        :param int validSecondsLeft: validity period expressed in seconds
        :param basestring userName: user name that need to add to search filter

        :return: S_OK(list)/S_ERROR() -- list contain dicts with user name, DN, group
                                         expiration time, persistent flag
    """
    cmd = "SELECT UserName, UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    sqlCond = []

    if validSecondsLeft:
      try:
        validSecondsLeft = int(validSecondsLeft)
      except ValueError:
        return S_ERROR("Seconds left has to be an integer")
      sqlCond.append("TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > %d" % validSecondsLeft)

    if userName:
      try:
        sUserName = self._escapeString(userName)['Value']
      except KeyError:
        return S_ERROR("Can't escape user name")
      sqlCond.append('UserName = "%s"' % sUserName)

    if sqlCond:
      cmd += " WHERE %s" % " AND ".join(sqlCond)

    retVal = self._query(cmd)
    if not retVal['OK']:
      return retVal
    data = []
    for record in retVal['Value']:
      data.append({'Name': record[0],
                   'DN': record[1],
                   'group': record[2],
                   'expirationtime': record[3],
                   'persistent': record[4] == 'True'})
    return S_OK(data)

  def getCredentialsAboutToExpire(self, requiredSecondsLeft, onlyPersistent=True):
    cmd = "SELECT UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    cmd += " WHERE TIMESTAMPDIFF( SECOND, ExpirationTime, UTC_TIMESTAMP() ) < %d and " % requiredSecondsLeft
    cmd += "TIMESTAMPDIFF( SECOND, ExpirationTime, UTC_TIMESTAMP() ) > 0"
    if onlyPersistent:
      cmd += " AND PersistentFlag = 'True'"
    return self._query(cmd)

  def setPersistencyFlag(self, userDN, userGroup, persistent=True):
    """ Set the proxy PersistentFlag to the flag value

        :param basestring userDN: user DN
        :param basestring userGroup: group name
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
      cmd = "INSERT INTO `ProxyDB_Proxies` ( UserDN, UserGroup, Pem, ExpirationTime, PersistentFlag ) VALUES "
      cmd += "( %s, %s, '', UTC_TIMESTAMP(), 'True' )" % (sUserDN, sUserGroup)
    else:
      cmd = "UPDATE `ProxyDB_Proxies` SET PersistentFlag='%s' WHERE UserDN=%s AND UserGroup=%s" % (sqlFlag,
                                                                                                   sUserDN,
                                                                                                   sUserGroup)
    retVal = self._update(cmd)
    if not retVal['OK']:
      return retVal
    return S_OK()

  def getProxiesContent(self, selDict, sortList, start=0, limit=0):
    """ Get the contents of the db, parameters are a filter to the db
    """
    data = []
    sqlWhere = ["Pem is not NULL"]
    for table, fields in [('ProxyDB_CleanProxies', ("UserName", "UserDN", "ExpirationTime")),
                          ('ProxyDB_Proxies', ("UserName", "UserDN", "UserGroup", "ExpirationTime", "PersistentFlag"))]:
      cmd = "SELECT %s FROM `%s`" % (", ".join(fields), table)
      for field in selDict:
        if field not in fields:
          continue
        fVal = selDict[field]
        if isinstance(fVal, (dict, tuple, list)):
          sqlWhere.append("%s in (%s)" % (field, ", ".join([self._escapeString(str(value))['Value'] for value in fVal])))
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
          record.insert(2, '')
          record.insert(4, False)
        if record[4] == 'True':
          record[4] = True
        else:
          record[4] = False
        data.append(record)
    totalRecords = len(data)
    cmd = "SELECT COUNT( UserGroup ) FROM `ProxyDB_Proxies`"
    if sqlWhere:
      cmd = "%s WHERE %s" % (cmd, " AND ".join(sqlWhere))
    retVal = self._query(cmd)
    if retVal['OK']:
      totalRecords = retVal['Value'][0][0]
    return S_OK({'ParameterNames': fields, 'Records': data, 'TotalRecords': totalRecords})

  def logAction(self, action, issuerDN, issuerGroup, targetDN, targetGroup):
    """
      Add an action to the log
    """
    try:
      sAction = self._escapeString(action)['Value']
      sIssuerDN = self._escapeString(issuerDN)['Value']
      sIssuerGroup = self._escapeString(issuerGroup)['Value']
      sTargetDN = self._escapeString(targetDN)['Value']
      sTargetGroup = self._escapeString(targetGroup)['Value']
    except KeyError:
      return S_ERROR("Can't escape from death")
    cmd = "INSERT INTO `ProxyDB_Log` ( Action, IssuerDN, IssuerGroup, TargetDN, TargetGroup, Timestamp ) VALUES "
    cmd += "( %s, %s, %s, %s, %s, UTC_TIMESTAMP() )" % (sAction, sIssuerDN, sIssuerGroup, sTargetDN, sTargetGroup)
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
    """
    Function to get the contents of the logs table
      parameters are a filter to the db
    """
    fields = ("Action", "IssuerDN", "IssuerGroup", "TargetDN", "TargetGroup", "Timestamp")
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

  def generateToken(self, requesterDN, requesterGroup, numUses=1, lifeTime=0, retries=10):
    """
    Generate and return a token and the number of uses for the token
    """
    if not lifeTime:
      lifeTime = gConfig.getValue("/DIRAC/VOPolicy/TokenLifeTime", self.__defaultTokenLifetime)
    maxUses = gConfig.getValue("/DIRAC/VOPolicy/TokenMaxUses", self.__defaultTokenMaxUses)
    numUses = max(1, min(numUses, maxUses))
    m = hashlib.md5()
    rndData = "%s.%s.%s.%s" % (time.time(), random.random(), numUses, lifeTime)
    m.update(rndData)
    token = m.hexdigest()
    fieldsSQL = ", ".join(("Token", "RequesterDN", "RequesterGroup", "ExpirationTime", "UsesLeft"))
    valuesSQL = ", ".join((self._escapeString(token)['Value'],
                           self._escapeString(requesterDN)['Value'],
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

        :return: S_OK()/S_ERROR()
    """
    delSQL = "DELETE FROM `ProxyDB_Tokens` WHERE ExpirationTime < UTC_TIMESTAMP() OR UsesLeft < 1"
    return self._update(delSQL)

  def useToken(self, token, requesterDN, requesterGroup):
    sqlCond = " AND ".join(("UsesLeft > 0",
                            "Token=%s" % self._escapeString(token)['Value'],
                            "RequesterDN=%s" % self._escapeString(requesterDN)['Value'],
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
        if not self._notifyProxyAboutToExpire(userDN, group, lTime, notifLimit):
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

  def _notifyProxyAboutToExpire(self, userDN, userGroup, lTime, notifLimit):
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
