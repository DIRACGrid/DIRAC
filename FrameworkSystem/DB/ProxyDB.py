########################################################################
# $HeadURL$
########################################################################
""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id$"

import time
import random
import types
try:
  import hashlib 
  md5 = hashlib
except:
  import md5
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security.X509Request import X509Request
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security.MyProxy import MyProxy
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient


class ProxyDB( DB ):

  NOTIFICATION_TIMES = [ 2592000, 1296000 ]

  def __init__( self,
                useMyProxy = False,
                maxQueueSize = 10 ):
    DB.__init__( self, 'ProxyDB', 'Framework/ProxyDB', maxQueueSize )
    random.seed()
    self.__defaultRequestLifetime = 300 # 5min
    self.__defaultTokenLifetime = 86400 * 7 # 1 week
    self.__defaultTokenMaxUses = 50
    self.__useMyProxy = useMyProxy
    self._minSecsToAllowStore = 3600
    self.__notifClient = NotificationClient()
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ] )
    self.purgeExpiredProxies( sendNotifications = False )
    self.__checkDBVersion()

  def getMyProxyServer( self ):
    return gConfig.getValue( "/DIRAC/VOPolicy/MyProxyServer" , "myproxy.cern.ch" )

  def getMyProxyMaxLifeTime( self ):
    return gConfig.getValue( "/DIRAC/VOPolicy/MyProxyMaxDelegationTime", 168 ) * 3600

  def __initializeDB( self ):
    """
    Create the tables
    """
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesD = {}

    if 'ProxyDB_Requests' not in tablesInDB:
      tablesD[ 'ProxyDB_Requests' ] = { 'Fields' : { 'Id' : 'INTEGER AUTO_INCREMENT NOT NULL',
                                                     'UserDN' : 'VARCHAR(255) NOT NULL',
                                                     'Pem' : 'BLOB',
                                                     'ExpirationTime' : 'DATETIME'
                                                   },
                                        'PrimaryKey' : 'Id'
                                      }

    if 'ProxyDB_Proxies' not in tablesInDB:
      tablesD[ 'ProxyDB_Proxies' ] = { 'Fields' : { 'UserName' : 'VARCHAR(64) NOT NULL',
                                                    'UserDN' : 'VARCHAR(255) NOT NULL',
                                                    'UserGroup' : 'VARCHAR(255) NOT NULL',
                                                    'Pem' : 'BLOB',
                                                    'ExpirationTime' : 'DATETIME',
                                                    'PersistentFlag' : 'ENUM ("True","False") NOT NULL DEFAULT "True"',
                                                  },
                                      'PrimaryKey' : [ 'UserDN', 'UserGroup' ]
                                     }

    if 'ProxyDB_VOMSProxies' not in tablesInDB:
      tablesD[ 'ProxyDB_VOMSProxies' ] = { 'Fields' : { 'UserName' : 'VARCHAR(64) NOT NULL',
                                                        'UserDN' : 'VARCHAR(255) NOT NULL',
                                                        'UserGroup' : 'VARCHAR(255) NOT NULL',
                                                        'VOMSAttr' : 'VARCHAR(255) NOT NULL',
                                                        'Pem' : 'BLOB',
                                                        'ExpirationTime' : 'DATETIME',
                                                  },
                                           'PrimaryKey' : [ 'UserDN', 'UserGroup', 'vomsAttr'  ]
                                     }

    if 'ProxyDB_Log' not in tablesInDB:
      tablesD[ 'ProxyDB_Log' ] = { 'Fields' : { 'IssuerDN' : 'VARCHAR(255) NOT NULL',
                                                'IssuerGroup' : 'VARCHAR(255) NOT NULL',
                                                'TargetDN' : 'VARCHAR(255) NOT NULL',
                                                'TargetGroup' : 'VARCHAR(255) NOT NULL',
                                                'Action' : 'VARCHAR(128) NOT NULL',
                                                'Timestamp' : 'DATETIME',
                                              }
                                  }

    if 'ProxyDB_Tokens' not in tablesInDB:
      tablesD[ 'ProxyDB_Tokens' ] = { 'Fields' : { 'Token' : 'VARCHAR(64) NOT NULL',
                                                   'RequesterDN' : 'VARCHAR(255) NOT NULL',
                                                   'RequesterGroup' : 'VARCHAR(255) NOT NULL',
                                                   'ExpirationTime' : 'DATETIME NOT NULL',
                                                   'UsesLeft' : 'SMALLINT UNSIGNED DEFAULT 1',
                                                 },
                                      'PrimaryKey' : 'Token'
                                  }

    if 'ProxyDB_ExpNotifs' not in tablesInDB:
      tablesD[ 'ProxyDB_ExpNotifs' ] = { 'Fields' : { 'UserDN' : 'VARCHAR(255) NOT NULL',
                                                      'UserGroup' : 'VARCHAR(255) NOT NULL',
                                                      'LifeLimit' : 'INTEGER UNSIGNED DEFAULT 0',
                                                      'ExpirationTime' : 'DATETIME NOT NULL',
                                                    },
                                         'PrimaryKey' : [ 'UserDN', 'UserGroup' ]
                                       }

    return self._createTables( tablesD )

  def __addUserNameToTable( self, tableName ):
    result = self._update( "ALTER TABLE `%s` ADD COLUMN UserName VARCHAR(64) NOT NULL" % tableName )
    if not result[ 'OK' ]:
      return result
    result = self._query( "SELECT DISTINCT UserName, UserDN FROM `%s`" % tableName )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    for userName, userDN in data:
      if not userName:
        result = Registry.getUsernameForDN( userDN )
        if not result[ 'OK' ]:
          self.log.error( "Could not retrieve username for DN %s" % userDN )
          continue
        userName = result[ 'Value' ]
        try:
          userName = self._escapeString( userName )[ 'Value' ]
          userDN = self._escapeString( userDN )[ 'Value' ]
        except KeyError:
          self.log.error( "Could not escape username %s or DN %s" % ( userName, userDN ) )
          continue
        userName = result[ 'Value' ]
        result = self._update( "UPDATE `%s` SET UserName=%s WHERE UserDN=%s" % ( tableName, userName, userDN ) )
        if not result[ 'OK' ]:
          self.log.error( "Could update username for DN %s: %s" % ( userDN, result[ 'Message' ] ) )
          continue
        self.log.info( "UserDN %s has user %s" % ( userDN, userName ) )
    return S_OK()


  def __checkDBVersion( self ):
    for tableName in ( "ProxyDB_Proxies", "ProxyDB_VOMSProxies" ):
      result = self._query( "describe `%s`" % tableName )
      if not result[ 'OK' ]:
        return result
      if 'UserName' not in [ row[0] for row in result[ 'Value' ] ]:
        self.log.notice( "Username missing in table %s schema. Adding it" % tableName )
        result = self.__addUserNameToTable( tableName )
        if not result[ 'OK' ]:
          return result



  def generateDelegationRequest( self, proxyChain, userDN ):
    """
    Generate a request  and store it for a given proxy Chain
    """
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return retVal
    connObj = retVal[ 'Value' ]
    retVal = proxyChain.generateProxyRequest()
    if not retVal[ 'OK' ]:
      return retVal
    request = retVal[ 'Value' ]
    retVal = request.dumpRequest()
    if not retVal[ 'OK' ]:
      return retVal
    reqStr = retVal[ 'Value' ]
    retVal = request.dumpPKey()
    if not retVal[ 'OK' ]:
      return retVal
    allStr = reqStr + retVal[ 'Value' ]
    try:
      sUserDN = self._escapeString( userDN )[ 'Value' ]
      sAllStr = self._escapeString( allStr )[ 'Value' ]
    except KeyError:
      return S_ERROR( "Cannot escape DN" )
    cmd = "INSERT INTO `ProxyDB_Requests` ( Id, UserDN, Pem, ExpirationTime )"
    cmd += " VALUES ( 0, %s, %s, TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ) )" % ( sUserDN,
                                                                              sAllStr,
                                                                              self.__defaultRequestLifetime )
    retVal = self._update( cmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    #99% of the times we will stop here
    if 'lastRowId' in retVal:
      return S_OK( { 'id' : retVal['lastRowId'], 'request' : reqStr } )
    #If the lastRowId hack does not work. Get it by hand
    retVal = self._query( "SELECT Id FROM `ProxyDB_Requests` WHERE Pem='%s'" % reqStr )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    if len( data ) == 0:
      return S_ERROR( "Insertion of the request in the db didn't work as expected" )
    retVal = proxyChain.getDIRACGroup()
    if retVal[ 'OK' ] and retVal[ 'Value' ]:
      userGroup = retVal[ 'Value' ]
    else:
      userGroup = "unset"
    self.logAction( "request upload", userDN, userGroup, userDN, "any" )
    #Here we go!
    return S_OK( { 'id' : data[0][0], 'request' : reqStr } )

  def retrieveDelegationRequest( self, requestId, userDN ):
    """
    Retrieve a request from the DB
    """
    try:
      sUserDN = self._escapeString( userDN )[ 'Value' ]
    except KeyError:
      return S_ERROR( "Cannot escape DN" )
    cmd = "SELECT Pem FROM `ProxyDB_Requests` WHERE Id = %s AND UserDN = %s" % ( requestId,
                                                                                   sUserDN )
    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    if len( data ) == 0:
      return S_ERROR( "No requests with id %s" % requestId )
    request = X509Request()
    retVal = request.loadAllFromString( data[0][0] )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( request )

  def purgeExpiredRequests( self ):
    """
    Purge expired requests from the db
    """
    cmd = "DELETE FROM `ProxyDB_Requests` WHERE ExpirationTime < UTC_TIMESTAMP()"
    return self._update( cmd )

  def deleteRequest( self, requestId ):
    """
    Delete a request from the db
    """
    cmd = "DELETE FROM `ProxyDB_Requests` WHERE Id=%s" % requestId
    return self._update( cmd )

  def completeDelegation( self, requestId, userDN, delegatedPem ):
    """
    Complete a delegation and store it in the db
    """
    retVal = self.retrieveDelegationRequest( requestId, userDN )
    if not retVal[ 'OK' ]:
      return retVal
    request = retVal[ 'Value' ]
    chain = X509Chain( keyObj = request.getPKey() )
    retVal = chain.loadChainFromString( delegatedPem )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = chain.isValidProxy( ignoreDefault = True )
    noGroupFlag = False
    if not retVal[ 'OK' ]:
      if retVal['Message'] == "Proxy does not have an explicit group":
        noGroupFlag = True
      else:  
        return retVal

    result = chain.isVOMS()
    if result[ 'OK' ] and result[ 'Value' ]:
      return S_ERROR( "Proxies with VOMS extensions are not allowed to be uploaded" )

    retVal = request.checkChain( chain )
    if not retVal[ 'OK' ]:
      return retVal
    if not retVal[ 'Value' ]:
      return S_ERROR( "Received chain does not match request: %s" % retVal[ 'Message' ] )

    retVal = chain.getDIRACGroup()
    if not retVal[ 'OK' ]:
      return retVal
    userGroup = retVal[ 'Value' ]
    if not userGroup:
      userGroup = Registry.getDefaultUserGroup()

    retVal = Registry.getGroupsForDN( userDN )
    if not retVal[ 'OK' ]:
      return retVal
    if not userGroup in retVal[ 'Value' ]:
      return S_ERROR( "%s group is not valid for %s" % ( userGroup, userDN ) )

    # For proxies without embedded DIRAC group only one default is allowed
    # Cleaning all the proxies for this DN if any before uploading the new one.
    if noGroupFlag:
      retVal = self.deleteProxy( userDN )
      if not retVal[ 'OK' ]:
        return retVal

    retVal = self.storeProxy( userDN, userGroup, chain )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self.deleteRequest( requestId )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK()

  def storeProxy( self, userDN, userGroup, chain ):
    """ Store user proxy into the Proxy repository for a user specified by his
        DN and group.
    """
    retVal = Registry.getUsernameForDN( userDN )
    if not retVal[ 'OK' ]:
      return retVal
    userName = retVal[ 'Value' ]
    #Get remaining secs
    retVal = chain.getRemainingSecs()
    if not retVal[ 'OK' ]:
      return retVal
    remainingSecs = retVal[ 'Value' ]
    if remainingSecs < self._minSecsToAllowStore:
      return S_ERROR( "Cannot store proxy, remaining secs %s is less than %s" % ( remainingSecs, self._minSecsToAllowStore ) )

    #Compare the DNs
    retVal = chain.getIssuerCert()
    if not retVal[ 'OK' ]:
      return retVal
    proxyIdentityDN = retVal[ 'Value' ].getSubjectDN()[ 'Value' ]
    if not userDN == proxyIdentityDN:
      msg = "Mismatch in the user DN"
      vMsg = "Proxy says %s and credentials are %s" % ( proxyIdentityDN, userDN )
      self.log.error( msg, vMsg )
      return S_ERROR( "%s. %s" % ( msg, vMsg ) )
    #Check the groups
    retVal = chain.getDIRACGroup()
    if not retVal[ 'OK' ]:
      return retVal
    proxyGroup = retVal[ 'Value' ]
    if not proxyGroup:
      proxyGroup = Registry.getDefaultUserGroup()
    if not userGroup == proxyGroup:
      msg = "Mismatch in the user group"
      vMsg = "Proxy says %s and credentials are %s" % ( proxyGroup, userGroup )
      self.log.error( msg, vMsg )
      return S_ERROR( "%s. %s" % ( msg, vMsg ) )
    #Check if its limited
    if chain.isLimitedProxy()['Value']:
      return S_ERROR( "Limited proxies are not allowed to be stored" )
    dLeft = remainingSecs / 86400
    hLeft = remainingSecs / 3600 - dLeft * 24
    mLeft = remainingSecs / 60 - hLeft * 60 - dLeft * 1440
    sLeft = remainingSecs - hLeft * 3600 - mLeft * 60 - dLeft * 86400
    self.log.info( "Storing proxy for credentials %s (%d:%02d:%02d:%02d left)" % ( proxyIdentityDN, dLeft, hLeft, mLeft, sLeft ) )

    try:
      sUserDN = self._escapeString( userDN )[ 'Value' ]
      sUserGroup = self._escapeString( userGroup )[ 'Value' ]
    except KeyError:
      return S_ERROR( "Cannot escape DN" )
    # Check what we have already got in the repository
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ), Pem FROM `ProxyDB_Proxies` WHERE UserDN=%s AND UserGroup=%s" % ( sUserDN, sUserGroup )
    result = self._query( cmd )
    if not result['OK']:
      return result
    # check if there is a previous ticket for the DN
    data = result[ 'Value' ]
    sqlInsert = True
    if len( data ) > 0:
      sqlInsert = False
      pem = data[0][1]
      if pem:
        remainingSecsInDB = data[0][0]
        if remainingSecs <= remainingSecsInDB:
          self.log.info( "Proxy stored is longer than uploaded, omitting.", "%s in uploaded, %s in db" % ( remainingSecs, remainingSecsInDB ) )
          return S_OK()

    pemChain = chain.dumpAllToString()['Value']
    dValues = { 'UserName' : self._escapeString( userName )[ 'Value' ],
                'UserDN' : sUserDN,
                'UserGroup' : sUserGroup,
                'Pem' : self._escapeString( pemChain )[ 'Value' ],
                'ExpirationTime' : 'TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() )' % int( remainingSecs ),
                'PersistentFlag' : "'False'" }
    if sqlInsert:
      sqlFields = []
      sqlValues = []
      for key in dValues:
        sqlFields.append( key )
        sqlValues.append( dValues[ key ] )
      cmd = "INSERT INTO `ProxyDB_Proxies` ( %s ) VALUES ( %s )" % ( ", ".join( sqlFields ), ", ".join( sqlValues ) )
    else:
      sqlSet = []
      sqlWhere = []
      for k in dValues:
        if k in ( 'UserDN', 'UserGroup' ):
          sqlWhere.append( "%s = %s" % ( k, dValues[k] ) )
        else:
          sqlSet.append( "%s = %s" % ( k, dValues[k] ) )
      cmd = "UPDATE `ProxyDB_Proxies` SET %s WHERE %s" % ( ", ".join( sqlSet ), " AND ".join( sqlWhere ) )

    self.logAction( "store proxy", userDN, userGroup, userDN, userGroup )
    return self._update( cmd )

  def purgeExpiredProxies( self, sendNotifications = True ):
    """
    Purge expired requests from the db
    """

    purged = 0
    for tableName in ( "ProxyDB_Proxies", "ProxyDB_VOMSProxies" ):
      cmd = "DELETE FROM `%s` WHERE ExpirationTime < UTC_TIMESTAMP()" % tableName
      result = self._update( cmd )
      if not result[ 'OK' ]:
        return result
      purged += result[ 'Value' ]
      self.log.info( "Purged %s expired proxies from %s" % ( result[ 'Value' ], tableName ) )
    if sendNotifications:
      result = self.sendExpirationNotifications()
      if not result[ 'OK' ]:
        return result
    return S_OK( purged )


  def deleteProxy( self, userDN, userGroup='any' ):
    """ Remove proxy of the given user from the repository
    """
    try:
      userDN = self._escapeString( userDN )[ 'Value' ]
      if userGroup != 'any':
        userGroup = self._escapeString( userGroup )[ 'Value' ]
    except KeyError:
      return S_ERROR( "Invalid DN or group" )
    
    req = "DELETE FROM `ProxyDB_Proxies` WHERE UserDN=%s" % userDN
    if userGroup != 'any':
      req += " AND UserGroup=%s" % userGroup
    return self._update( req )

  def __getPemAndTimeLeft( self, userDN, userGroup = False, vomsAttr = False ):
    try:
      sUserDN = self._escapeString( userDN )[ 'Value' ]
      if userGroup:
        sUserGroup = self._escapeString( userGroup )[ 'Value' ]
      if vomsAttr:
        sVomsAttr = self._escapeString( vomsAttr )[ 'Value' ]
    except KeyError:
      return S_ERROR( "Invalid DN or group" )
    if not vomsAttr:
      table = "`ProxyDB_Proxies`"
    else:
      table = "`ProxyDB_VOMSProxies`"
    cmd = "SELECT Pem, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) from %s" % table
    cmd += "WHERE UserDN=%s AND TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > 0" % ( sUserDN )
    if userGroup:
      cmd += " AND UserGroup=%s" % sUserGroup
    if vomsAttr:
      cmd += " AND VOMSAttr=%s" % sVomsAttr
    retVal = self._query( cmd )
    if not retVal['OK']:
      return retVal
    data = retVal[ 'Value' ]
    for record in data:
      if record[0]:
        return S_OK( ( record[0], record[1] ) )
    if userGroup:
      userMask = "%s@%s" % ( userDN, userGroup )
    else:
      userMask = userDN
    return S_ERROR( "%s has no proxy registered" % userMask )

  def renewFromMyProxy( self, userDN, userGroup, lifeTime = False, chain = False ):
    if not lifeTime:
      lifeTime = 43200
    if not self.__useMyProxy:
      return S_ERROR( "myproxy is disabled" )
    #Get the chain
    if not chain:
      retVal = self.__getPemAndTimeLeft( userDN, userGroup )
      if not retVal[ 'OK' ]:
        return retVal
      pemData = retVal[ 'Value' ][0]
      chain = X509Chain()
      retVal = chain.loadProxyFromString( pemData )
      if not retVal[ 'OK' ]:
        return retVal

    originChainLifeTime = chain.getRemainingSecs()[ 'Value' ]
    maxMyProxyLifeTime = self.getMyProxyMaxLifeTime()
    #If we have a chain that's 0.8 of max mplifetime don't ask to mp
    if originChainLifeTime > maxMyProxyLifeTime * 0.8:
      self.log.error( "Skipping myproxy download",
                     "user %s %s  chain has %s secs and requested %s secs" % ( userDN,
                                                                               userGroup,
                                                                               originChainLifeTime,
                                                                               maxMyProxyLifeTime ) )
      return S_OK( chain )

    lifeTime *= 1.3
    if lifeTime > maxMyProxyLifeTime:
      lifeTime = maxMyProxyLifeTime
    self.log.error( "Renewing proxy from myproxy", "user %s %s for %s secs" % ( userDN, userGroup, lifeTime ) )

    myProxy = MyProxy( server = self.getMyProxyServer() )
    retVal = myProxy.getDelegatedProxy( chain, lifeTime )
    if not retVal[ 'OK' ]:
      return retVal
    mpChain = retVal[ 'Value' ]
    retVal = mpChain.getRemainingSecs()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't retrieve remaining secs from renewed proxy: %s" % retVal[ 'Message' ] )
    mpChainSecsLeft = retVal['Value']
    if mpChainSecsLeft < originChainLifeTime:
      self.log.info( "Chain downloaded from myproxy has less lifetime than the one stored in the db",
                    "\n Downloaded from myproxy: %s secs\n Stored in DB: %s secs" % ( mpChainSecsLeft, originChainLifeTime ) )
      return S_OK( chain )
    retVal = mpChain.getDIRACGroup()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't retrieve DIRAC Group from renewed proxy: %s" % retVal[ 'Message' ] )
    chainGroup = retVal['Value']
    if chainGroup != userGroup:
      return S_ERROR( "Mismatch between renewed proxy group and expected: %s vs %s" % ( userGroup, chainGroup ) )
    retVal = self.storeProxy( userDN, userGroup, mpChain )
    if not retVal[ 'OK' ]:
      self.log.error( "Cannot store proxy after renewal", retVal[ 'Message' ] )
    retVal = myProxy.getServiceDN()
    if not retVal[ 'OK' ]:
      hostDN = userDN
    else:
      hostDN = retVal[ 'Value' ]
    self.logAction( "myproxy renewal", hostDN, "host", userDN, userGroup )
    return S_OK( mpChain )

  def getProxy( self, userDN, userGroup, requiredLifeTime = False ):
    """ Get proxy string from the Proxy Repository for use with userDN
        in the userGroup
    """

    retVal = self.__getPemAndTimeLeft( userDN, userGroup )
    if not retVal[ 'OK' ]:
      return retVal
    pemData = retVal[ 'Value' ][0]
    timeLeft = retVal[ 'Value' ][1]
    chain = X509Chain()
    retVal = chain.loadProxyFromString( pemData )
    if not retVal[ 'OK' ]:
      return retVal
    if requiredLifeTime:
      if timeLeft < requiredLifeTime:
        retVal = self.renewFromMyProxy( userDN, userGroup, lifeTime = requiredLifeTime, chain = chain )
        if not retVal[ 'OK' ]:
          return S_ERROR( "Can't get a proxy for %s seconds: %s" % ( requiredLifeTime, retVal[ 'Message' ] ) )
        chain = retVal[ 'Value' ]
    #Proxy is invalid for some reason, let's delete it
    if not chain.isValidProxy()['Value']:
      self.deleteProxy( userDN, userGroup )
      return S_ERROR( "%s@%s has no proxy registered" % ( userDN, userGroup ) )
    return S_OK( ( chain, timeLeft ) )

  def __getVOMSAttribute( self, userGroup, requiredVOMSAttribute = False ):

    if requiredVOMSAttribute:
      return S_OK( { 'attribute' : requiredVOMSAttribute, 'VOMSVO' : Registry.getVOMSVOForGroup( userGroup ) } )

    csVOMSMapping = Registry.getVOMSAttributeForGroup( userGroup )
    if not csVOMSMapping:
      return S_ERROR( "No mapping defined for group %s in the CS" % userGroup )

    return S_OK( { 'attribute' : csVOMSMapping, 'VOMSVO' : Registry.getVOMSVOForGroup( userGroup ) } )

  def getVOMSProxy( self, userDN, userGroup, requiredLifeTime = False, requestedVOMSAttr = False ):
    """ Get proxy string from the Proxy Repository for use with userDN
        in the userGroup and VOMS attr
    """
    retVal = self.__getVOMSAttribute( userGroup, requestedVOMSAttr )
    if not retVal[ 'OK' ]:
      return retVal
    vomsAttr = retVal[ 'Value' ][ 'attribute' ]
    vomsVO = retVal[ 'Value' ][ 'VOMSVO' ]

    #Look in the cache
    retVal = self.__getPemAndTimeLeft( userDN, userGroup, vomsAttr )
    if retVal[ 'OK' ]:
      pemData = retVal[ 'Value' ][0]
      vomsTime = retVal[ 'Value' ][1]
      chain = X509Chain()
      retVal = chain.loadProxyFromString( pemData )
      if retVal[ 'OK' ]:
        retVal = chain.getRemainingSecs()
        if retVal[ 'OK' ]:
          remainingSecs = retVal[ 'Value' ]
          if requiredLifeTime and requiredLifeTime <= vomsTime and requiredLifeTime <= remainingSecs:
            return S_OK( ( chain, min( vomsTime, remainingSecs ) ) )

    retVal = self.getProxy( userDN, userGroup, requiredLifeTime )
    if not retVal[ 'OK' ]:
      return retVal
    chain, secsLeft = retVal[ 'Value' ]

    if requiredLifeTime and requiredLifeTime > secsLeft:
      return S_ERROR( "Stored proxy is not long lived enough" )

    vomsMgr = VOMS()

    retVal = vomsMgr.getVOMSAttributes( chain )
    if retVal[ 'OK' ]:
      attrs = retVal[ 'Value' ]
      if len( attrs ) > 0:
        if attrs[0] != vomsAttr:
          return S_ERROR( "Stored proxy has already a different VOMS attribute %s than requested %s" % ( vomsAttr, attrs[0] ) )
        else:
          result = self.__storeVOMSProxy( userDN, userGroup, vomsAttr, chain )
          if not result[ 'OK' ]:
            return result
          secsLeft = result[ 'Value' ]
          if requiredLifeTime and requiredLifeTime <= secsLeft:
            return S_OK( ( chain, secsLeft ) )
          return S_ERROR( "Stored proxy has already a different VOMS attribute and is not long lived enough" )

    retVal = vomsMgr.setVOMSAttributes( chain , vomsAttr, vo = vomsVO )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Cannot append voms extension: %s" % retVal[ 'Message' ] )
    chain = retVal[ 'Value' ]
    result = self.__storeVOMSProxy( userDN, userGroup, vomsAttr, chain )
    if not result[ 'OK' ]:
      return result
    secsLeft = result[ 'Value' ]
    return S_OK( ( chain, secsLeft ) )

  def __storeVOMSProxy( self, userDN, userGroup, vomsAttr, chain ):
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return retVal
    connObj = retVal[ 'Value' ]
    retVal1 = VOMS().getVOMSProxyInfo( chain, 'actimeleft' )
    retVal2 = VOMS().getVOMSProxyInfo( chain, 'timeleft' )
    if not retVal1[ 'OK' ]:
      return retVal1
    if not retVal2[ 'OK' ]:
      return retVal2
    try:
      vomsSecsLeft1 = int( retVal1[ 'Value' ].strip() )
      vomsSecsLeft2 = int( retVal2[ 'Value' ].strip() )
      vomsSecsLeft = min( vomsSecsLeft1, vomsSecsLeft2 )
    except Exception, e:
      return S_ERROR( "Can't parse VOMS time left: %s" % str( e ) )
    secsLeft = min( vomsSecsLeft, chain.getRemainingSecs()[ 'Value' ] )
    pemData = chain.dumpAllToString()[ 'Value' ]
    result = Registry.getUsernameForDN( userDN )
    if not result[ 'OK' ]:
      userName = ""
    else:
      userName = result[ 'Value' ]
    try:
      sUserName = self._escapeString( userName )[ 'Value' ]
      sUserDN = self._escapeString( userDN )[ 'Value' ]
      sUserGroup = self._escapeString( userGroup )[ 'Value' ]
      sVomsAttr = self._escapeString( vomsAttr )[ 'Value' ]
      sPemData = self._escapeString( pemData )[ 'Value' ]
    except KeyError:
      return S_ERROR( "Could not escape some data" )
    cmd = "REPLACE INTO `ProxyDB_VOMSProxies` ( UserName, UserDN, UserGroup, VOMSAttr, Pem, ExpirationTime ) VALUES "
    cmd += "( %s, %s, %s, %s, %s, TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() ) )" % ( sUserName, sUserDN, sUserGroup,
                                                                                     sVomsAttr, sPemData, secsLeft )
    result = self._update( cmd, conn = connObj )
    if not result[ 'OK' ]:
      return result
    return S_OK( secsLeft )

  def getRemainingTime( self, userDN, userGroup ):
    """
    Returns the remaining time the proxy is valid
    """
    try:
      userDN = self._escapeString( userDN )[ 'Value' ]
      userGroup = self._escapeString( userGroup )[ 'Value' ]
    except KeyError:
      return S_ERROR( "Invalid DN or group" )
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) FROM `ProxyDB_Proxies`"
    retVal = self._query( "%s WHERE UserDN = %s AND UserGroup = %s" % ( cmd, userDN, userGroup ) )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    if not data:
      return S_OK( 0 )
    return S_OK( int( data[0][0] ) )

  def getUsers( self, validSecondsLeft = 0, dnMask = False, groupMask = False, userMask = False ):
    """ Get all the distinct users from the Proxy Repository. Optionally, only users
        with valid proxies within the given validity period expressed in seconds
    """

    cmd = "SELECT UserName, UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    sqlCond = []

    if validSecondsLeft:
      try:
        validSecondsLeft = int( validSecondsLeft )
      except ValueError:
        return S_ERROR( "Seconds left has to be an integer" )
      sqlCond.append( "TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > %d" % validSecondsLeft )

    for field, mask in ( ( 'UserDN', dnMask ), ( 'UserGroup', groupMask ), ( 'UserName', userMask ) ):
      if not mask:
        continue
      if type( mask ) not in ( types.ListType, types.TupleType ):
        mask = [ mask ]
      mask = [ self._escapeString( entry )[ 'Value' ] for entry in mask ]
      sqlCond.append( "%s in ( %s )" % ( field, ", ".join( mask ) ) )

    if sqlCond:
      cmd += " WHERE %s" % " AND ".join( sqlCond )

    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = []
    for record in retVal[ 'Value' ]:
      data.append( { 'Name': record[0],
                     'DN' : record[1],
                     'group' : record[2],
                     'expirationtime' : record[3],
                     'persistent' : record[4] == 'True' } )
    return S_OK( data )

  def getCredentialsAboutToExpire( self, requiredSecondsLeft, onlyPersistent = True ):
    cmd = "SELECT UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    cmd += " WHERE TIMESTAMPDIFF( SECOND, ExpirationTime, UTC_TIMESTAMP() ) < %d and TIMESTAMPDIFF( SECOND, ExpirationTime, UTC_TIMESTAMP() ) > 0" % requiredSecondsLeft
    if onlyPersistent:
      cmd += " AND PersistentFlag = 'True'"
    return self._query( cmd )

  def setPersistencyFlag( self, userDN, userGroup, persistent = True ):
    """ Set the proxy PersistentFlag to the flag value
    """
    try:
      sUserDN = self._escapeString( userDN )[ 'Value' ]
      sUserGroup = self._escapeString( userGroup )[ 'Value' ]
    except KeyError:
      return S_ERROR( "Can't escape something" )
    if persistent:
      sqlFlag = "True"
    else:
      sqlFlag = "False"
    retVal = self._query( "SELECT PersistentFlag FROM `ProxyDB_Proxies` WHERE UserDN=%s AND UserGroup=%s" % ( sUserDN, sUserGroup ) )
    sqlInsert = True
    if retVal[ 'OK' ]:
      data = retVal[ 'Value' ]
      if len( data ) > 0:
        sqlInsert = False
        if data[0][0] == sqlFlag:
          return S_OK()
    if sqlInsert:
      #If it's not in the db and we're removing the persistency then do nothing
      if not persistent:
        return S_OK()
      cmd = "INSERT INTO `ProxyDB_Proxies` ( UserDN, UserGroup, Pem, ExpirationTime, PersistentFlag ) VALUES "
      cmd += "( %s, %s, '', UTC_TIMESTAMP(), 'True' )" % ( sUserDN, sUserGroup )
    else:
      cmd = "UPDATE `ProxyDB_Proxies` SET PersistentFlag='%s' WHERE UserDN=%s AND UserGroup=%s" % ( sqlFlag,
                                                                                            sUserDN,
                                                                                            sUserGroup )

    retVal = self._update( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK()

  def getProxiesContent( self, selDict, sortList, start = 0, limit = 0 ):
    """
    Function to get the contents of the db
      parameters are a filter to the db
    """
    fields = ( "UserName", "UserDN", "UserGroup", "ExpirationTime", "PersistentFlag" )
    cmd = "SELECT %s FROM `ProxyDB_Proxies`" % ", ".join( fields )
    sqlWhere = [ "Pem is not NULL" ]
    for field in selDict:
      if field not in fields:
        continue
      fVal = selDict[field]
      if type( fVal ) in ( types.DictType, types.TupleType, types.ListType ):
        sqlWhere.append( "%s in (%s)" % ( field, ", ".join( [ self._escapeString( str( value ) )[ 'Value' ] for value in fVal ] ) ) )
      else:
        sqlWhere.append( "%s = %s" % ( field, self._escapeString( str( fVal ) )[ 'Value' ] ) )
    sqlOrder = []
    if sortList:
      for sort in sortList:
        if len( sort ) == 1:
          sort = ( sort, "DESC" )
        elif len( sort ) > 2:
          return S_ERROR( "Invalid sort %s" % sort )
        if sort[0] not in fields:
          return S_ERROR( "Invalid sorting field %s" % sort[0] )
        if sort[1].upper() not in ( "ASC", "DESC" ):
          return S_ERROR( "Invalid sorting order %s" % sort[1] )
        sqlOrder.append( "%s %s" % ( sort[0], sort[1] ) )
    if sqlWhere:
      cmd = "%s WHERE %s" % ( cmd, " AND ".join( sqlWhere ) )
    if sqlOrder:
      cmd = "%s ORDER BY %s" % ( cmd, ", ".join( sqlOrder ) )
    if limit:
      try:
        start = int( start )
        limit = int( limit )
      except ValueError:
        return S_ERROR( "start and limit have to be integers" )
      cmd += " LIMIT %d,%d" % ( start, limit )
    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = []
    for record in retVal[ 'Value' ]:
      record = list( record )
      if record[4] == 'True':
        record[4] = True
      else:
        record[4] = False
      data.append( record )
    totalRecords = len( data )
    cmd = "SELECT COUNT( UserGroup ) FROM `ProxyDB_Proxies`"
    if sqlWhere:
      cmd = "%s WHERE %s" % ( cmd, " AND ".join( sqlWhere ) )
    retVal = self._query( cmd )
    if retVal[ 'OK' ]:
      totalRecords = retVal[ 'Value' ][0][0]
    return S_OK( { 'ParameterNames' : fields, 'Records' : data, 'TotalRecords' : totalRecords } )

  def logAction( self, action, issuerDN, issuerGroup, targetDN, targetGroup ):
    """
      Add an action to the log
    """
    try:
      sAction = self._escapeString( action )[ 'Value' ]
      sIssuerDN = self._escapeString( issuerDN )[ 'Value' ]
      sIssuerGroup = self._escapeString( issuerGroup )[ 'Value' ]
      sTargetDN = self._escapeString( targetDN )[ 'Value' ]
      sTargetGroup = self._escapeString( targetGroup )[ 'Value' ]
    except KeyError:
      return S_ERROR( "Can't escape from death" )
    cmd = "INSERT INTO `ProxyDB_Log` ( Action, IssuerDN, IssuerGroup, TargetDN, TargetGroup, Timestamp ) VALUES "
    cmd += "( %s, %s, %s, %s, %s, UTC_TIMESTAMP() )" % ( sAction, sIssuerDN, sIssuerGroup, sTargetDN, sTargetGroup )
    retVal = self._update( cmd )
    if not retVal[ 'OK' ]:
      self.log.error( "Can't add a proxy action log: ", retVal[ 'Message' ] )

  def purgeLogs( self ):
    """
    Purge expired requests from the db
    """
    cmd = "DELETE FROM `ProxyDB_Log` WHERE TIMESTAMPDIFF( SECOND, Timestamp, UTC_TIMESTAMP() ) > 15552000"
    return self._update( cmd )

  def getLogsContent( self, selDict, sortList, start = 0, limit = 0 ):
    """
    Function to get the contents of the logs table
      parameters are a filter to the db
    """
    fields = ( "Action", "IssuerDN", "IssuerGroup", "TargetDN", "TargetGroup", "Timestamp" )
    cmd = "SELECT %s FROM `ProxyDB_Log`" % ", ".join( fields )
    if selDict:
      qr = []
      if 'beforeDate' in selDict:
        qr.append( "Timestamp < %s" % self._escapeString( selDict[ 'beforeDate' ] )[ 'Value' ] )
        del( selDict[ 'beforeDate' ] )
      if 'afterDate' in selDict:
        qr.append( "Timestamp > %s" % self._escapeString( selDict[ 'afterDate' ] )[ 'Value' ] )
        del( selDict[ 'afterDate' ] )
      for field in selDict:
        qr.append( "(%s)" % " OR ".join( [ "%s=%s" % ( field, self._escapeString( str( value ) )[ 'Value' ] ) for value in selDict[field] ] ) )
      whereStr = " WHERE %s" % " AND ".join( qr )
      cmd += whereStr
    else:
      whereStr = ""
    if sortList:
      cmd += " ORDER BY %s" % ", ".join( [ "%s %s" % ( sort[0], sort[1] ) for sort in sortList ] )
    if limit:
      cmd += " LIMIT %d,%d" % ( start, limit )
    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    totalRecords = len( data )
    cmd = "SELECT COUNT( Timestamp ) FROM `ProxyDB_Log`"
    cmd += whereStr
    retVal = self._query( cmd )
    if retVal[ 'OK' ]:
      totalRecords = retVal[ 'Value' ][0][0]
    return S_OK( { 'ParameterNames' : fields, 'Records' : data, 'TotalRecords' : totalRecords } )

  def generateToken( self, requesterDN, requesterGroup, numUses = 1, lifeTime = 0, retries = 10 ):
    """
    Generate and return a token and the number of uses for the token
    """
    if not lifeTime:
      lifeTime = gConfig.getValue( "/DIRAC/VOPolicy/TokenLifeTime", self.__defaultTokenLifetime )
    maxUses = gConfig.getValue( "/DIRAC/VOPolicy/TokenMaxUses", self.__defaultTokenMaxUses )
    numUses = max( 1, min( numUses, maxUses ) )
    m = md5.md5()
    rndData = "%s.%s.%s.%s" % ( time.time(), random.random(), numUses, lifeTime )
    m.update( rndData )
    token = m.hexdigest()
    fieldsSQL = ", ".join( ( "Token", "RequesterDN", "RequesterGroup", "ExpirationTime", "UsesLeft" ) )
    valuesSQL = ", ".join( ( self._escapeString( token )['Value'],
                              self._escapeString( requesterDN )['Value'],
                              self._escapeString( requesterGroup )['Value'],
                            "TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() )" % lifeTime,
                            str( numUses ) ) )

    insertSQL = "INSERT INTO `ProxyDB_Tokens` ( %s ) VALUES ( %s )" % ( fieldsSQL, valuesSQL )
    result = self._update( insertSQL )
    if result[ 'OK' ]:
      return S_OK( ( token, numUses ) )
    if result[ 'Message' ].find( "uplicate entry" ) > -1:
      if retries:
        return self.generateToken( numUses, lifeTime, retries - 1 )
      return S_ERROR( "Max retries reached for token generation. Aborting" )
    return result

  def purgeExpiredTokens( self ):
    delSQL = "DELETE FROM `ProxyDB_Tokens` WHERE ExpirationTime < UTC_TIMESTAMP() OR UsesLeft < 1"
    return self._update( delSQL )

  def useToken( self, token, requesterDN, requesterGroup ):
    sqlCond = " AND ".join( ( "UsesLeft > 0",
                              "Token=%s" % self._escapeString( token )['Value'],
                              "RequesterDN=%s" % self._escapeString( requesterDN )['Value'],
                              "RequesterGroup=%s" % self._escapeString( requesterGroup )['Value'],
                              "ExpirationTime >= UTC_TIMESTAMP()" ) )
    updateSQL = "UPDATE `ProxyDB_Tokens` SET UsesLeft = UsesLeft - 1 WHERE %s" % sqlCond
    result = self._update( updateSQL )
    if not result[ 'OK' ]:
      return result
    return S_OK( result[ 'Value' ] > 0 )

  def __cleanExpNotifs( self ):
    cmd = "DELETE FROM `ProxyDB_ExpNotifs` WHERE ExpirationTime < UTC_TIMESTAMP()"
    return self._update( cmd )

  def sendExpirationNotifications( self ):
    result = self.__cleanExpNotifs()
    if not result[ 'OK' ]:
      return result
    cmd = "SELECT UserDN, UserGroup, LifeLimit FROM `ProxyDB_ExpNotifs`"
    result = self._query( cmd )
    if not result[ 'OK' ]:
      return result
    notifDone = dict( [ ( ( row[0], row[1] ), row[2] ) for row in result[ 'Value' ] ] )
    notifLimits = sorted( [ int( x ) for x in self.getCSOption( "NotificationTimes", ProxyDB.NOTIFICATION_TIMES ) ] )
    sqlSel = "UserDN, UserGroup, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime )"
    sqlCond = "TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) < %d" % max( notifLimits )
    cmd = "SELECT %s FROM `ProxyDB_Proxies` WHERE %s" % ( sqlSel, sqlCond )
    result = self._query( cmd )
    if not result[ 'OK' ]:
      return result
    pilotProps = ( Properties.GENERIC_PILOT, Properties.PILOT )
    data = result[ 'Value' ]
    sent = []
    for row in data:
      userDN, group, lTime = row
      #If it's a pilot proxy, skip it
      if Registry.groupHasProperties( group, pilotProps ):
        continue
      #IF it dosn't hace the auto upload proxy, skip it
      if not Registry.getGroupOption( group, "AutoUploadProxy", False ):
        continue
      notKey = ( userDN, group )
      for notifLimit in notifLimits:
        if notifLimit < lTime:
          #Not yet in this notification limit
          continue
        if notKey in notifDone and notifDone[ notKey ] <= notifLimit:
          #Already notified for this notification limit
          break
        if not self.__notifyProxyAboutToExpire( userDN, group, lTime, notifLimit ):
          #Cannot send notification, retry later
          break
        try:
          sUserDN = self._escapeString( userDN )[ 'Value' ]
          sGroup = self._escapeString( group )[ 'Value' ]
        except KeyError:
          return S_ERROR( "OOPS" )
        if notKey not in notifDone:
          values = "( %s, %s, %d, TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ) )" % ( sUserDN, sGroup, notifLimit, lTime )
          cmd = "INSERT INTO `ProxyDB_ExpNotifs` ( UserDN, UserGroup, LifeLimit, ExpirationTime ) VALUES %s" % values
          result = self._update( cmd )
          if not result[ 'OK' ]:
            gLogger.error( "Could not mark notification as sent", result[ 'Message' ] )
        else:
          values = "LifeLimit = %d, ExpirationTime = TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() )" % ( notifLimit, lTime )
          cmd = "UPDATE `ProxyDB_ExpNotifs` SET %s WHERE UserDN = %s AND UserGroup = %s" % ( values, sUserDN, sGroup )
          result = self._update( cmd )
          if not result[ 'OK' ]:
            gLogger.error( "Could not mark notification as sent", result[ 'Message' ] )
        sent.append( ( userDN, group, lTime ) )
        notifDone[ notKey ] = notifLimit
    return S_OK( sent )

  def __notifyProxyAboutToExpire( self, userDN, userGroup, lTime, notifLimit ):
    result = Registry.getUsernameForDN( userDN )
    if not result[ 'OK' ]:
      return False
    userName = result[ 'Value' ]
    userEMail = Registry.getUserOption( userName, "Email", "" )
    if not userEMail:
      gLogger.error( "Could not discover %s's email" % userName )
      return False
    daysLeft = int( lTime / 86400 )
    msgSubject = "Your proxy uploaded to DIRAC will expire in %d days" % daysLeft
    msgBody = """\
Dear %s,

  The proxy you uploaded to DIRAC will expire in aproximately %d days. The proxy
  information is:

  DN:    %s
  Group: %s

  If you plan on keep using this credentials please upload a newer proxy to
  DIRAC by executing:

  $ dirac-proxy-init -UP -g %s

  If you have been issued different certificate, please make sure you have a
  proxy uploaded with that certificate.

Cheers,
 DIRAC's Proxy Manager
""" % ( userName, daysLeft, userDN, userGroup, userGroup )
    result = self.__notifClient.sendMail( userEMail, msgSubject, msgBody, fromAddress = 'proxymanager@diracgrid.org' )
    if not result[ 'OK' ]:
      gLogger.error( "Could not send email", result[ 'Message' ] )
      return False
    return True
