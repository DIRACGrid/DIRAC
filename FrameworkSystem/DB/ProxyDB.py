########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/DB/ProxyDB.py,v 1.23 2008/07/30 09:28:48 acasajus Exp $
########################################################################
""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id: ProxyDB.py,v 1.23 2008/07/30 09:28:48 acasajus Exp $"

import time
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security.X509Request import X509Request
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security.MyProxy import MyProxy
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security import CS

class ProxyDB(DB):

  def __init__(self, requireVoms = False,
               useMyProxy = False,
               maxQueueSize = 10 ):
    DB.__init__(self,'ProxyDB','Framework/ProxyDB',maxQueueSize)
    self.__defaultRequestLifetime = 300 # 5min
    self.__vomsRequired = requireVoms
    self.__useMyProxy = useMyProxy
    self._minSecsToAllowStore = 3600
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ])

  def getMyProxyServer(self):
    return gConfig.getValue( "/DIRAC/VOPolicy/MyProxyServer" , "myproxy.cern.ch" )

  def getMyProxyMaxLifeTime(self):
    return gConfig.getValue( "/DIRAC/VOPolicy/MyProxyMaxDelegationTime", 168 ) * 3600

  def __initializeDB(self):
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
      tablesD[ 'ProxyDB_Proxies' ] = { 'Fields' : { 'UserDN' : 'VARCHAR(255) NOT NULL',
                                                    'UserGroup' : 'VARCHAR(255) NOT NULL',
                                                    'Pem' : 'BLOB',
                                                    'ExpirationTime' : 'DATETIME',
                                                    'PersistentFlag' : 'ENUM ("True","False") NOT NULL DEFAULT "True"',
                                                  },
                                      'PrimaryKey' : [ 'UserDN', 'UserGroup' ]
                                     }
    if 'ProxyDB_VOMSProxies' not in tablesInDB:
      tablesD[ 'ProxyDB_VOMSProxies' ] = { 'Fields' : { 'UserDN' : 'VARCHAR(255) NOT NULL',
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
    return self._createTables( tablesD )

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
    cmd = "INSERT INTO `ProxyDB_Requests` ( Id, UserDN, Pem, ExpirationTime )"
    cmd += " VALUES ( 0, '%s', '%s', TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ) )" % ( userDN,
                                                                              allStr,
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
    #Here we go!
    return S_OK( { 'id' : data[0][0], 'request' : reqStr } )

  def retrieveDelegationRequest( self, requestId, userDN ):
    """
    Retrieve a request from the DB
    """
    cmd = "SELECT Pem FROM `ProxyDB_Requests` WHERE Id = %s AND UserDN = '%s'" % ( requestId,
                                                                                   userDN )
    retVal = self._query( cmd)
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

  def __checkVOMSisAlignedWithGroup( self, userGroup, chain ):
    voms = VOMS()
    if not voms.vomsInfoAvailable():
      if self.__vomsRequired:
        return S_ERROR( "VOMS is required, but it's not available" )
      gLogger.warn( "voms-proxy-info is not available" )
      return S_OK()
    retVal = voms.getVOMSAttributes( chain )
    if not retVal[ 'OK' ]:
      return retVal
    attr = retVal[ 'Value' ]
    validVOMSAttrs = CS.getVOMSAttributeForGroup( userGroup )
    if len( attr ) == 0 or attr[0] in validVOMSAttrs:
      return S_OK( 'OK' )
    msg = "VOMS attributes are not aligned with dirac group"
    msg += "Attributes are %s and allowed are %s for group %s" % ( attr, validVOMSAttrs, userGroup )
    return S_ERROR( msg )

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
    retVal = chain.isValidProxy()
    if not retVal[ 'OK' ]:
      return retVal
    if not retVal[ 'Value' ]:
      return S_ERROR( "Chain received is not a valid proxy: %s" % retVal[ 'Message' ] )

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
      userGroup = CS.getDefaultUserGroup()

    retVal = CS.getGroupsForDN( userDN )
    if not retVal[ 'OK' ]:
      return retVal
    if not userGroup in retVal[ 'Value' ]:
      return S_ERROR( "%s group is not valid for %s" % ( userGroup, userDN ) )

    retVal = self.__checkVOMSisAlignedWithGroup( userGroup, chain )
    if not retVal[ 'OK' ]:
      return retVal

    retVal = self.storeProxy( userDN, userGroup, chain )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self.deleteRequest( requestId )
    if not retVal[ 'OK' ]:
      return retVal
    self.logAction( "upload proxy", userDN, userGroup, userDN, userGroup )
    return S_OK()

  def storeProxy(self, userDN, userGroup, chain ):
    """ Store user proxy into the Proxy repository for a user specified by his
        DN and group.
    """
    #Get remaining secs
    retVal = chain.getRemainingSecs()
    if not retVal[ 'OK' ]:
      return retVal
    remainingSecs = retVal[ 'Value' ]
    if remainingSecs < self._minSecsToAllowStore:
      return S_ERROR( "Cannot store proxy, remaining secs %s is less than %s" %( remainingSecs, self._minSecsToAllowStore ) )

    #Compare the DNs
    retVal = chain.getIssuerCert()
    if not retVal[ 'OK' ]:
      return retVal
    proxyIdentityDN = retVal[ 'Value' ].getSubjectDN()[ 'Value' ]
    if not userDN == proxyIdentityDN:
      msg = "Mismatch in the user DN"
      vMsg = "Proxy says %s and credentials are %s" % ( proxyIdentityDN, userDN )
      gLogger.error( msg, vMsg )
      return S_ERROR(  "%s. %s" % ( msg, vMsg ) )
    #Check the groups
    retVal = chain.getDIRACGroup()
    if not retVal[ 'OK' ]:
      return retVal
    proxyGroup = retVal[ 'Value' ]
    if not proxyGroup:
      proxyGroup = CS.getDefaultUserGroup()
    if not userGroup == proxyGroup:
      msg = "Mismatch in the user group"
      vMsg = "Proxy says %s and credentials are %s" % ( proxyGroup, userGroup )
      gLogger.error( msg, vMsg )
      return S_ERROR(  "%s. %s" % ( msg, vMsg ) )
    #Check if its limited
    if chain.isLimitedProxy()['Value']:
      return S_ERROR( "Limited proxies are not allowed to be stored" )
    gLogger.info( "Storing proxy for credentials %s (%s secs)" %( proxyIdentityDN,remainingSecs ) )

    # Check what we have already got in the repository
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ), Pem FROM `ProxyDB_Proxies` WHERE UserDN='%s' AND UserGroup='%s'" % ( userDN,
                                                                                                               userGroup)
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
          gLogger.info( "Proxy stored is longer than uploaded, omitting.", "%s in uploaded, %s in db" % (remainingSecs, remainingSecsInDB ) )
          return S_OK()

    pemChain = chain.dumpAllToString()['Value']
    if sqlInsert:
      cmd = "INSERT INTO `ProxyDB_Proxies` ( UserDN, UserGroup, Pem, ExpirationTime, PersistentFlag ) VALUES "
      cmd += "( '%s', '%s', '%s', TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ), 'False' )" % ( userDN,
                                                                                  userGroup,
                                                                                  pemChain,
                                                                                  remainingSecs )
    else:
      cmd = "UPDATE `ProxyDB_Proxies` set Pem='%s', ExpirationTime = TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ) WHERE UserDN='%s' AND UserGroup='%s'" % ( pemChain,
                                                                                                                                                remainingSecs,
                                                                                                                                                userDN,
                                                                                                                                                userGroup)

    return self._update( cmd )

  def purgeExpiredProxies( self ):
    """
    Purge expired requests from the db
    """
    cmd = "DELETE FROM `ProxyDB_Proxies` WHERE ExpirationTime < UTC_TIMESTAMP() and PersistentFlag = 'False'"
    return self._update( cmd )

  def deleteProxy( self, userDN, userGroup ):
    """ Remove proxy of the given user from the repository
    """

    req = "DELETE FROM `ProxyDB_Proxies` WHERE UserDN='%s' AND UserGroup='%s'" % ( userDN,
                                                                                   userGroup )
    return self._update(req)

  def __getPemAndTimeLeft( self, userDN, userGroup = False, vomsAttr = False ):
    if not vomsAttr:
      table = "`ProxyDB_Proxies`"
    else:
      table = "`ProxyDB_VOMSProxies`"
    cmd = "SELECT Pem, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) from %s" % table
    cmd += "WHERE UserDN='%s' AND TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > 0" % ( userDN )
    if userGroup:
      cmd += " AND UserGroup='%s'" % userGroup
    if vomsAttr:
      cmd += " AND VOMSAttr='%s'" % vomsAttr
    retVal = self._query(cmd)
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
      gLogger.error( "Skipping myproxy download",
                     "user %s %s  chain has %s secs and requested %s secs" % ( userDN,
                                                                               userGroup,
                                                                               originChainLifeTime,
                                                                               maxMyProxyLifeTime ) )
      return S_OK( chain )

    lifeTime *= 1.3
    if lifeTime > maxMyProxyLifeTime:
      lifeTime = maxMyProxyLifeTime
    gLogger.error( "Renewing proxy from myproxy", "user %s %s for %s secs" % ( userDN, userGroup, lifeTime ) )

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
      gLogger.info( "Chain downloaded from myproxy has less lifetime than the one stored in the db",
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
      gLogger.error( "Cannot store proxy after renewal", retVal[ 'Message' ] )
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
    return S_OK( chain )

  def __getVOMSAttribute( self, userGroup, requiredVOMSAttribute = False ):
    if requiredVOMSAttribute:
      return S_OK( requiredVOMSAttribute )
    csVOMSMappings = CS.getVOMSAttributeForGroup( userGroup )
    if not csVOMSMappings:
      return S_ERROR( "No mapping defined for group %s in the CS" % userGroup )
    if len( csVOMSMappings ) > 1:
      return S_ERROR( "More than one VOMS attribute defined for group %s and one required" % userGroup )
    return S_OK( csVOMSMappings[0] )

  def getVOMSProxy( self, userDN, userGroup, requiredLifeTime = False, requestedVOMSAttr = False ):
    """ Get proxy string from the Proxy Repository for use with userDN
        in the userGroup and VOMS attr
    """
    retVal = self.__getVOMSAttribute( userGroup, requestedVOMSAttr )
    if not retVal[ 'OK' ]:
      return retVal
    vomsAttr = retVal[ 'Value' ]

    #Look in the cache
    retVal = self.__getPemAndTimeLeft( userDN, userGroup, vomsAttr )
    if retVal[ 'OK' ]:
      pemData = retVal[ 'Value' ][0]
      chain = X509Chain()
      retVal = chain.loadProxyFromString( pemData )
      if retVal[ 'OK' ]:
        if requiredLifeTime and requiredLifeTime >= chain.getRemainingSecs()[ 'Value' ]:
          return S_OK( chain )

    retVal = self.getProxy( userDN, userGroup, requiredLifeTime )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]

    vomsMgr = VOMS()

    retVal = vomsMgr.getAttributes( chain )
    if retVal[ 'OK' ]:
      attrs = retVal[ 'Value' ]
      if len( attrs ) > 0:
        if attrs[0] != requestedVOMSAttr:
          return S_ERROR( "Stored proxy has already a different VOMS attribute %s than requested %s" %( requestedVOMSAttr, attrs[0] ) )
        else:
          self.__storeVOMSProxy( userDN, userGroup, vomsAttr, chain )
          return S_OK( chain )

    retVal = vomsMgr.setVOMSAttributes( chain , vomsAttr )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Cannot append voms extension: %s" % retVal[ 'Value' ] )
    chain = retVal[ 'Value' ]
    self.__storeVOMSProxy( userDN, userGroup, vomsAttr, chain )
    return S_OK( chain )

  def __storeVOMSProxy( self, userDN, userGroup, vomsAttr, chain ):
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return retVal
    connObj = retVal[ 'Value' ]
    cmd = "DELETE FROM `ProxyDB_VOMSProxies` WHERE UserDN='%s' AND UserGroup='%s' AND VOMSAttr='%s'" % ( userDN, userGroup, vomsAttr )
    retVal = self._update( cmd, conn = connObj )
    if not retVal[ 'OK' ]:
      return retVal
    secsLeft = chain.getRemainingSecs()[ 'Value' ]
    pemData = chain.dumpAllToString()[ 'Value' ]
    cmd = "INSERT INTO `ProxyDB_VOMSProxies` ( UserDN, UserGroup, VOMSAttr, Pem, ExpirationTime ) VALUES "
    cmd += "( '%s', '%s', '%s', '%s', TIMESTAMPADD( SECOND, %s, UTC_TIMESTAMP() ) )" % ( userDN, userGroup,
                                                                                         vomsAttr, pemData, secsLeft )
    return self._update( cmd, conn = connObj )

  def getRemainingTime( self, userDN, userGroup ):
    """
    Returns the remaining time the proxy is valid
    """
    cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) FROM `ProxyDB_Proxies`"
    retVal = self._query( "%s WHERE UserDN = '%s' AND UserGroup = '%s'" % ( cmd, userDN, userGroup ) )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    if not data:
      return S_OK( 0 )
    return S_OK( int( data[0][0] ) )

  def getUsers( self, validSecondsLeft = 0, dnMask = False, groupMask = False ):
    """ Get all the distinct users from the Proxy Repository. Optionally, only users
        with valid proxies within the given validity period expressed in seconds
    """

    cmd = "SELECT UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    sqlCond = []
    if validSecondsLeft:
      sqlCond.append( "( UTC_TIMESTAMP() + INTERVAL %d SECOND ) < ExpirationTime" % validSecondsLeft )
    if dnMask:
      maskCond = []
      for dn in dnMask:
        maskCond.append( "UserDN = '%s'" % dn )
      sqlCond.append( "( %s )" % " OR ".join( maskCond ) )
    if groupMask:
      maskCond = []
      for group in groupMask:
        maskCond.append( "UserGroup = '%s'" % group )
      sqlCond.append( "( %s )" % " OR ".join( maskCond ) )
    if sqlCond:
      cmd += " WHERE %s" % " AND ".join( sqlCond )

    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = []
    for record in retVal[ 'Value' ]:
      data.append( { 'DN' : record[0],
                     'group' : record[1],
                     'expirationtime' : record[2],
                     'persistent' : record[3] == 'True' } )
    return S_OK( data )

  def getCredentialsAboutToExpire( self, requiredSecondsLeft, onlyPersistent = True ):
    cmd = "SELECT UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    cmd += " WHERE TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) < %s" % requiredSecondsLeft
    if onlyPersistent:
      cmd += " AND PersistentFlag = 'True'"
    return self._query( cmd )

  def setPersistencyFlag( self, userDN, userGroup, persistent = True ):
    """ Set the proxy PersistentFlag to the flag value
    """
    if persistent:
      sqlFlag="True"
    else:
      sqlFlag="False"
    retVal = self._query( "SELECT PersistentFlag FROM `ProxyDB_Proxies` WHERE UserDN='%s' AND UserGroup='%s'" % ( userDN, userGroup ) )
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
      cmd += "( '%s', '%s', '', UTC_TIMESTAMP(), 'True' )" % ( userDN, userGroup )
    else:
      cmd = "UPDATE `ProxyDB_Proxies` SET PersistentFlag='%s' WHERE UserDN='%s' AND UserGroup='%s'" % ( sqlFlag,
                                                                                            userDN,
                                                                                            userGroup )

    retVal = self._update(cmd)
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK()

  def getProxiesContent( self, selDict, sortList, start = 0, limit = 0 ):
    """
    Function to get the contents of the db
      parameters are a filter to the db
    """
    fields = ( "UserDN", "UserGroup", "ExpirationTime", "PersistentFlag" )
    cmd = "SELECT %s FROM `ProxyDB_Proxies` WHERE Pem is not NULL" % ", ".join( fields )
    for field in selDict:
      cmd += " AND (%s)" % " OR ".join( [ "%s=%s" % ( field, self._escapeString( str( value ) )[ 'Value' ] ) for value in selDict[field] ] )
    if sortList:
      cmd += " ORDER BY %s" % ", ".join( [ "%s %s" % ( sort[0], sort[1] ) for sort in sortList ] )
    if limit:
      cmd += " LIMIT %d,%d" % ( start, limit )
    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = []
    for record in retVal[ 'Value' ]:
      record = list( record )
      if record[3] == 'True':
        record[3] = True
      else:
        record[3] = False
      data.append( record )
    totalRecords = len( data )
    cmd = "SELECT COUNT( UserGroup ) FROM `ProxyDB_Proxies`"
    if selDict:
      qr = []
      for field in selDict:
        qr.append( "(%s)" % " OR ".join( [ "%s=%s" % ( field, self._escapeString( str( value ) )[ 'Value' ] ) for value in selDict[field] ] ) )
      cmd += " WHERE %s" % " AND ".join( qr )
    retVal = self._query( cmd )
    if retVal[ 'OK' ]:
      totalRecords = retVal[ 'Value' ][0][0]
    return S_OK( { 'ParameterNames' : fields, 'Records' : data, 'TotalRecords' : totalRecords } )

  def logAction( self, action, issuerDN, issuerGroup, targetDN, targetGroup ):
    """
      Add an action to the log
    """
    cmd = "INSERT INTO `ProxyDB_Log` ( Action, IssuerDN, IssuerGroup, TargetDN, TargetGroup, Timestamp ) VALUES "
    cmd += "( '%s', '%s', '%s', '%s', '%s', UTC_TIMESTAMP() )" % ( action,
                                                                   issuerDN,
                                                                   issuerGroup,
                                                                   targetDN,
                                                                   targetGroup )
    retVal = self._update( cmd )
    if not retVal[ 'OK' ]:
      gLogger.error( "Can't add a log: %s" % retVal[ 'Message' ] )

  def purgeLogs( self ):
    """
    Purge expired requests from the db
    """
    cmd = "DELETE FROM `ProxyDB_Log` WHERE TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > 63936000"
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