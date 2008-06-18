########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/Attic/ProxyDB.py,v 1.2 2008/06/18 20:02:21 acasajus Exp $
########################################################################
""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id: ProxyDB.py,v 1.2 2008/06/18 20:02:21 acasajus Exp $"

import time
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security.X509Request import X509Request
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security import CS

#############################################################################
class ProxyDB(DB):

  def __init__(self, systemInstance='Default',maxQueueSize=10):
    DB.__init__(self,'ProxyRepositoryDB','WorkloadManagement/ProxyRepositoryDB',maxQueueSize)
    self.VO = gConfig.getValue('/DIRAC/VirtualOrganization', "unknown" )
    self.__defaultRequestLifetime = 300 # 5min
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ])

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
                                                     'UserGroup' : 'VARCHAR(255) NOT NULL',
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
    return self._createTables( tablesD )

#############################################################################
  def generateDelegationRequest( self, proxyChain, userDN, userGroup ):
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
    retVal = request.dumpAll()
    if not retVal[ 'OK' ]:
      return retVal
    reqStr = retVal[ 'Value' ]
    cmd = "INSERT INTO `ProxyDB_Requests` ( Id, UserDN, UserGroup, Pem, ExpirationTime )"
    cmd += " VALUES ( 0, '%s', '%s', '%s', TIMESTAMPADD( SECOND, %s, NOW() ) )" % ( userDN,
                                                                              userGroup,
                                                                              reqStr,
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

#############################################################################
  def retrieveDelegationRequest( self, requestId, userDN, userGroup ):
    """
    Retrieve a request from the DB
    """
    cmd = "SELECT Pem FROM `ProxyDB_Requests` WHERE Id = %s AND UserDN = '%s' and UserGroup = '%s'" % ( requestId,
                                                                                                userDN,
                                                                                                userGroup )
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

#############################################################################
  def purgeExpiredRequests( self ):
    """
    Purge expired requests from the db
    """
    cmd = "DELETE FROM `ProxyDB_Requests` WHERE ExpirationTime > NOW()"
    return self._update( cmd )

#############################################################################
  def deleteRequest( self, requestId ):
    """
    Delete a request from the db
    """
    cmd = "DELETE FROM `ProxyDB_Requests` WHERE Id=%s" % requestId
    return self._update( cmd )

#############################################################################
  def completeDelegation( self, requestId, userDN, userGroup, delegatedPem ):
    """
    Complete a delegation and store it in the db
    """
    retVal = self.retrieveDelegationRequest( requestId, userDN, userGroup )
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

    #TODO:Check for VOMS and make sure it's aligned
    retVal = self.storeProxy( chain, userDN, userGroup )
    if not retVal[ 'OK' ]:
      return retVal
    return self.deleteRequest( requestId )

#############################################################################
  def storeProxy(self, chain, userDN, userGroup ):
    """ Store user proxy into the Proxy repository for a user specified by his
        DN and group.
    """

    retVal = chain.getRemainingSecs()
    if not retVal[ 'OK' ]:
      return retVal
    remainingSecs = retVal[ 'Value' ]
    retVal = chain.getIssuerCert()
    if not retVal[ 'OK' ]:
      return retVal
    proxyIdentityDN = retVal[ 'Value' ].getSubjectDN()[ 'Value' ]
    if not userDN == proxyIdentityDN:
      return S_ERROR( "Mismatch in the user DN\nProxy says %s and credentials are %s" % ( proxyIdentityDN,
                                                                                          userDN ) )
    retVal = chain.getDIRACGroup()
    if not retVal[ 'OK' ]:
      return retVal
    proxyGroup = retVal[ 'Value' ]
    if not proxyGroup:
      proxyGroup = CS.getDefaultUserGroup()
    if not userGroup == proxyGroup:
      return S_ERROR( "Mismatch in the user group\nProxy says %s and credentials are %s" % ( proxyGroup,
                                                                                             userGroup ) )

    # Check what we have already got in the repository
    cmd = "SELECT TIMESTAMPDIFF( SECOND, NOW(), ExpirationTime ) FROM `ProxyDB_Proxies` WHERE UserDN='%s' AND UserGroup='%s'" % ( userDN,
                                                                                                               userGroup)
    result = self._query( cmd )
    if not result['OK']:
      return result
    # check if there is a previous ticket for the DN
    data = result[ 'Value' ]
    insert = True
    if data:
      insert = False
      remainingSecsInDB = result['Value'][0][0]
      if remainingSecs <= remainingSecsInDB:
        return S_OK()

    pemChain = chain.dumpAllToString()['Value']
    if insert:
      cmd = "INSERT INTO `ProxyDB_Proxies` ( UserDN, UserGroup, Pem, ExpirationTime, PersistentFlag ) VALUES "
      cmd += "( '%s', '%s', '%s', TIMESTAMPADD( SECOND, %s, NOW() ), 'False' )" % ( userDN,
                                                                                  userGroup,
                                                                                  pemChain,
                                                                                  remainingSecs )
    else:
      cmd = "UPDATE `ProxyDB_Proxies` set Pem='%s', ExpirationTime = TIMESTAMPADD( SECOND, %s, NOW() ) WHERE UserDN='%s' AND UserGroup='%s'" % ( pemChain,
                                                                                                                                                remainingSecs,
                                                                                                                                                userDN,
                                                                                                                                                userGroup)

    return self._update( cmd )

#############################################################################
  def purgeExpiredProxies( self ):
    """
    Purge expired requests from the db
    """
    cmd = "DELETE FROM `ProxyDB_Proxies` WHERE ExpirationTime > NOW() and PersistentFlag = 'False'"
    return self._update( cmd )

#############################################################################
  def deleteProxy( self, userDN, userGroup ):
    """ Remove proxy of the given user from the repository
    """

    req = "DELETE FROM `ProxyDB_Proxies` WHERE UserDN='%s' AND UserGroup='%s'" % ( userDN,
                                                                                   userGroup )
    return self._update(req)

#############################################################################
  def getProxy( self, userDN, userGroup, requiredLifeTime = False ):
    """ Get proxy string from the Proxy Repository for use with userDN
        in the userGroup
    """

    cmd = "SELECT Pem, TIMESTAMPDIFF( SECOND, NOW(), ExpirationTime ) from `ProxyDB_Proxies`"
    cmd += "WHERE UserDN='%s' AND UserGroup = '%s'" % ( userDN, userGroup )
    result = self._query(cmd)
    if not result['OK']:
      return result
    data = result[ 'Value' ]
    if len( data ) == 0:
      return S_ERROR( "%s@%s has no proxy registered" % ( userDN, userGroup ) )
    pemData = data[0][0]
    if requiredLifeTime:
      if data[0][1] < requiredLifeTime:
        #TODO: Get delegation from myproxy
        return S_ERROR( "Can't get a proxy for %s seconds" % requiredLifeTime )
    chain = X509Chain()
    retVal = chain.loadProxyFromString( pemData )
    if not retVal[ 'OK' ]:
      return retVal
    #Proxy is invalid for some reason, let's delete it
    if not chain.isValidProxy()['Value']:
      self.deleteProxy( userDN, userGroup )
      return S_ERROR( "%s@%s has no proxy registered" % ( userDN, userGroup ) )
    return S_OK( chain )

#############################################################################
  def getUsers( self, validSecondsLeft = 0 ):
    """ Get all the distinct users from the Proxy Repository. Optionally, only users
        with valid proxies within the given validity period expressed in seconds
    """

    cmd = "SELECT UserDN, UserGroup, ExpirationTime, PersistentFlag FROM `ProxyDB_Proxies`"
    if validSecondsLeft:
      cmd += " WHERE ( NOW() + INTERVAL %d SECOND ) < ExpirationTime" % validSecondsLeft
    retVal = self._query( cmd )
    if not retVal[ 'OK' ]:
      return retVal
    data = []
    for record in retVal[ 'Value' ]:
      data.append( { 'DN' : record[0], 'group' : record[1], 'expirationtime' : record[2] } )
    return S_OK( data )

#############################################################################
  def setPersistencyFlag( self, userDN, userGroup, flag = True ):
    """ Set the proxy PersistentFlag to the flag value
    """

    if flag:
      sqlFlag="True"
    else:
      sqlFlag="False"
    cmd = "UPDATE `ProxyDB_Proxies` SET PersistentFlag='%s' WHERE UserDN='%s' AND UserGroup='%s'" % ( sqlFlag,
                                                                                            userDN,
                                                                                            userGroup )

    return self._update(cmd)
