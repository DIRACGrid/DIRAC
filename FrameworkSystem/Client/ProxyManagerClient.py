########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Client/ProxyManagerClient.py,v 1.1 2008/06/25 20:00:50 acasajus Exp $
########################################################################
""" ProxyManagementAPI has the functions to "talk" to the ProxyManagement service
"""
__RCSID__ = "$Id: ProxyManagerClient.py,v 1.1 2008/06/25 20:00:50 acasajus Exp $"

import os
import datetime
from DIRAC.Core.Utilities import Time, ThreadSafe
from DIRAC.Core.Security import Locations, CS
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security.X509Request import X509Request
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_OK, S_ERROR, gLogger

gUsersSync = ThreadSafe.Synchronizer()
gProxiesSync = ThreadSafe.Synchronizer()
gVOMSProxiesSync = ThreadSafe.Synchronizer()

class ProxyManagerClient:

  def __init__( self ):
    self.__usersCache = DictCache()
    self.__proxiesCache = DictCache()
    self.__vomsProxiesCache = DictCache()
    self.__filesCache = DictCache( self.__deleteTemporalFile )

  def __deleteTemporalFile( self, filename ):
    try:
      os.unlink( filename )
    except:
      pass

  @gUsersSync
  def userHasProxy( self, userDN, userGroup, validSeconds = 0 ):
    """
    Check if a user(DN-group) has a proxy in the proxy management
      - Updates internal cache if needed to minimize queries to the
          service
    """
    cacheKey = ( userDN, userGroup )
    if self.__usersCache.exists( cacheKey, validSeconds ):
      return S_OK( True )
    rpcClient = RPCClient( "Framework/ProxyManager" )
    #Get list of users from the DB with proxys at least 300 seconds
    gLogger.verbose( "Updating list of users in proxy management" )
    retVal = rpcClient.getRegisteredUsers( validSeconds )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    #Update the cache
    for record in data:
      self.__usersCache.add( cacheKey, record[ 'expirationtime' ] )
    return S_OK( self.__usersCache.exists( cacheKey, validSeconds ) )

  def uploadProxy( self, proxyLocation = False ):
    """
    Upload a proxy to the proxy management service using delgation
    """
    #Discover proxy location
    if not proxyLocation:
      proxyLocation = Locations.getProxyLocation()
    if not proxyLocation:
      return S_ERROR( "Can't find a valid proxy" )

    #Load proxy and make sure it's valid
    chain = X509Chain()
    retVal = chain.loadProxyFromFile( proxyLocation )
    if not retVal[ 'OK' ]:
      return retVal
    if chain.hasExpired()[ 'Value' ]:
      return S_ERROR( "Proxy %s has expired" % proxyLocation )

    rpcClient = RPCClient( "Framework/ProxyManager", proxyLocation = proxyLocation )
    #Get a delegation request
    retVal = rpcClient.requestDelegation()
    if not retVal[ 'OK' ]:
      return retVal
    reqDict = retVal[ 'Value' ]
    print reqDict['id']
    #Generate delegated chain
    retVal = chain.generateChainFromRequestString( reqDict[ 'request' ] )
    if not retVal[ 'OK' ]:
      return retVal
    #Upload!
    return rpcClient.completeDelegation( reqDict[ 'id' ], retVal[ 'Value' ] )

  @gProxiesSync
  def downloadProxy( self, userDN, userGroup, limited = False, requiredTimeLeft = 43200 ):
    """
    Get a proxy Chain from the proxy management
    """
    cacheKey = ( userDN, userGroup )
    if self.__proxiesCache.exists( cacheKey, requiredTimeLeft ):
      return S_OK( self.__proxiesCache.get( cacheKey ) )
    req = X509Request()
    req.generateProxyRequest( limited = limited )
    rpcClient = RPCClient( "Framework/ProxyManager" )
    retVal = rpcClient.getDelegatedProxy( userDN, userGroup, req.dumpAll()['Value'], requiredTimeLeft )
    if not retVal[ 'OK' ]:
      return retVal
    chain = X509Chain( keyObj = req.getPKey() )
    retVal = chain.loadChainFromString( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    self.__proxiesCache.add( cacheKey, chain.getNotAfterDate()['Value'], chain )
    return S_OK( chain )

  def downloadProxyToFile( self, userDN, userGroup, limited = False, requiredTimeLeft = 43200 ):
    """
    Get a proxy Chain from the proxy management and write it to file
    """
    retVal = self.downloadProxy( userDN, userGroup, limited, requiredTimeLeft )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = self.dumpProxyToFile( chain )
    if not retVal[ 'OK' ]:
      return retVal
    retVal[ 'chain' ] = chain
    return retVal

  def __getVOMSAttribute( self, userGroup, requiredVOMSAttribute = False ):
    csVOMSMappings = CS.getVOMSAttributeForGroup( userGroup )
    if not csVOMSMappings:
      return S_ERROR( "No mapping defined for group %s in the CS" )
    if requiredVOMSAttribute not in csVOMSMappings:
      return S_ERROR( "Required attribute %s is not allowed for group %s" % ( requiredVOMSAttribute, userGroup ) )
    if len( csVOMSMappings ) > 1 and not requiredVOMSAttribute:
      return S_ERROR( "More than one VOMS attribute defined for group %s and none required" % userGroup )
    vomsAttribute = requiredVOMSAttribute
    if not vomsAttribute:
      vomsAttribute = csVOMSMappings[0]
    return S_OK( vomsAttribute )

  @gVOMSProxiesSync
  def downloadVOMSProxy( self, userDN, userGroup, limited = False, requiredTimeLeft = 43200, requiredVOMSAttribute = False ):
    """
    Download a proxy if needed and transform it into a VOMS one
    """
    retVal = self.__getVOMSAttribute( userGroup, requiredVOMSAttribute )
    if not retVal[ 'OK' ]:
      return retVal
    vomsAttribute = retVal[ 'Value' ]

    cacheKey = ( userDN, userGroup, vomsAttribute )
    if self.__vomsProxiesCache.exists( cacheKey, requiredTimeLeft ):
      return S_OK( self.__vomsProxiesCache.get( cacheKey ) )

    retVal = self.downloadProxy( userDN, userGroup, limited, requiredTimeLeft )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    vomsMgr = VOMS()
    retVal = vomsMgr.getVOMSAttributes( chain )
    if not retVal[ 'OK' ]:
      return retVal
    if vomsAttribute not in retVal[ 'Value' ]:
      retVal = vomsMgr.setVOMSAttributes( chain, vomsAttribute )
      if not retVal[ 'OK' ]:
        return retVal
      chain = retVal[ 'Value' ]
    self.__proxyCache.add( cacheKey, chain.getNotAfterDate()['Value'], chain )
    return S_OK( chain )

  def downloadVOMSProxyToFile( self, userDN, userGroup, limited = False, requiredTimeLeft = 43200, requiredVOMSAttribute = False ):
    """
    Download a proxy if needed, transform it into a VOMS one and write it to file
    """
    retVal = self.downloadVOMSProxy( userDN, userGroup, limited, requiredTimeLeft, requiredVOMSAttribute )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = self.dumpProxyToFile( chain )
    if not retVal[ 'OK' ]:
      return retVal
    retVal[ 'chain' ] = chain
    return retVal

  def dumpProxyToFile( self, chain, requiredTimeLeft = 600 ):
    """
    Dump a proxy to a file. It's cached so multiple calls won't generate extra files
    """
    if self.__filesCache.exists( chain, requiredTimeLeft ):
      filepath = self.__filesCache.get( chain )
      if os.path.isfile( filepath ):
        return S_OK( filepath )
      self.__filesCache.delete( filepath )
    retVal = chain.dumpAllToFile()
    if not retVal[ 'Value' ]:
      return retVal
    filename = retVal[ 'Value' ]
    self.__filesCache.add( chain, chain.getNotAfterDate()['Value'], filename )
    return S_OK( filename )

  def deleteGeneratedProxyFile( self, chain ):
    """
    Delete a file generated by a dump
    """
    self.__filesCache.delete( chain )
    return S_OK()
#
# Helper class to handle the dict caches
#

class DictCache:

  def __init__(self, deleteFunction = False ):
    self.__cache = {}
    self.__deleteFunction = deleteFunction

  def exists( self, cKey, validSeconds = 0 ):
    #Is the key in the cache?
    if cKey in self.__cache:
      expTime = self.__cache[ cKey ][ 'expirationTime' ]
      #If it's valid return True!
      if expTime > Time.dateTime() + datetime.timedelta( seconds = validSeconds ):
        return True
      else:
        #Delete expired
        self.delete( cKey )
    return False

  def delete( self, cKey ):
    if cKey not in self.__cache:
      return
    if self.__deleteFunction:
      self.__deleteFunction( self.__cache[ cKey ][ 'value' ] )
    del( self.__cache[ cKey ] )

  def add( self, cKey, expirationTime, value = None ):
    vD = { 'expirationTime' : expirationTime, 'value' : value }
    self.__cache[ cKey ] = vD

  def get( self, cKey, validSeconds = 0 ):
    #Is the key in the cache?
    if cKey in self.__cache:
      expTime = self.__cache[ cKey ][ 'expirationTime' ]
      #If it's valid return True!
      if expTime > Time.dateTime() + datetime.timedelta( seconds = validSeconds ):
        return self.__cache[ cKey ][ 'value' ]
      else:
        #Delete expired
        self.delete( cKey )
    return False

gProxyManager = ProxyManagerClient()