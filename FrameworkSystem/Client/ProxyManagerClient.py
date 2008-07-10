########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Client/ProxyManagerClient.py,v 1.13 2008/07/10 17:15:21 acasajus Exp $
########################################################################
""" ProxyManagementAPI has the functions to "talk" to the ProxyManagement service
"""
__RCSID__ = "$Id: ProxyManagerClient.py,v 1.13 2008/07/10 17:15:21 acasajus Exp $"

import os
import datetime
import types
from DIRAC.Core.Utilities import Time, ThreadSafe, DictCache
from DIRAC.Core.Security import Locations, CS, File, Properties
from DIRAC.Core.Security.X509Chain import X509Chain, g_X509ChainType
from DIRAC.Core.Security.X509Request import X509Request
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security.MyProxy import MyProxy
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_OK, S_ERROR, gLogger, gConfig

gUsersSync = ThreadSafe.Synchronizer()
gProxiesSync = ThreadSafe.Synchronizer()
gVOMSProxiesSync = ThreadSafe.Synchronizer()
gPilotProxiesSync = ThreadSafe.Synchronizer()

class ProxyManagerClient:

  def __init__( self ):
    self.__usersCache = DictCache()
    self.__proxiesCache = DictCache()
    self.__vomsProxiesCache = DictCache()
    self.__pilotProxiesCache = DictCache()
    self.__filesCache = DictCache( self.__deleteTemporalFile )

  def __deleteTemporalFile( self, filename ):
    try:
      os.unlink( filename )
    except:
      pass

  def __getSecondsLeftToExpiration( self, expiration, utc = True ):
    if utc:
      td = expiration - datetime.datetime.utcnow()
    else:
      td = expiration - datetime.datetime.now()
    return td.days * 86400 + td.seconds

  def __refreshUserCache( self, validSeconds = 0 ):
    rpcClient = RPCClient( "Framework/ProxyManager" )
    retVal = rpcClient.getRegisteredUsers( validSeconds )
    if not retVal[ 'OK' ]:
      return retVal
    data = retVal[ 'Value' ]
    #Update the cache
    for record in data:
      cacheKey = ( record[ 'DN' ], record[ 'group' ] )
      self.__usersCache.add( cacheKey,
                             self.__getSecondsLeftToExpiration( record[ 'expirationtime' ] ),
                             record )
    return S_OK()

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
    #Get list of users from the DB with proxys at least 300 seconds
    gLogger.verbose( "Updating list of users in proxy management" )
    retVal = self.__refreshUserCache( validSeconds )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( self.__usersCache.exists( cacheKey, validSeconds ) )

  @gUsersSync
  def getUserPersistence( self, userDN, userGroup, validSeconds = 0 ):
    """
    Check if a user(DN-group) has a proxy in the proxy management
      - Updates internal cache if needed to minimize queries to the
          service
    """
    cacheKey = ( userDN, userGroup )
    userData = self.__usersCache.get( cacheKey, validSeconds )
    if userData:
      if userData[ 'persistent' ]:
        return S_OK( True )
    #Get list of users from the DB with proxys at least 300 seconds
    gLogger.verbose( "Updating list of users in proxy management" )
    retVal = self.__refreshUserCache( validSeconds )
    if not retVal[ 'OK' ]:
      return retVal
    userData = self.__usersCache.get( cacheKey, validSeconds )
    if userData:
      return S_OK( userData[ 'persistent' ] )
    return S_OK( False )

  def setPersistency( self, userDN, userGroup, persistent ):
    """
    Set the persistency for user/group
    """
    #Hack to ensure bool in the rpc call
    persistentFlag = True
    if not persistent:
      persistentFlag = False
    rpcClient = RPCClient( "Framework/ProxyManager" )
    retVal = rpcClient.setPersistency( userDN, userGroup, persistentFlag )
    if not retVal[ 'OK' ]:
      return retVal
    #Update internal persistency cache
    cacheKey = ( userDN, userGroup )
    record = self.__usersCache.get( cacheKey, 0 )
    if record:
      record[ 'persistent' ] = persistentFlag
      self.__usersCache.add( cacheKey,
                             self.__getSecondsLeftToExpiration( record[ 'expirationtime' ] ),
                             record )
    return retVal

  def uploadProxy( self, proxy = False ):
    """
    Upload a proxy to the proxy management service using delgation
    """
    #Discover proxy location
    if type( proxy ) == g_X509ChainType:
      chain = proxy
    else:
      if not proxy:
        proxyLocation = Locations.getProxyLocation()
      elif type( proxy ) in ( types.StringType, types.UnicodeType ):
        proxyLocation = proxy
      else:
        return S_ERROR( "Can't find a valid proxy" )
      chain = X509Chain()
      retVal = chain.loadProxyFromFile( proxyLocation )
      if not retVal[ 'OK' ]:
        return S_ERROR( "Can't load %s: %s " % ( proxyLocation, retVal[ 'Message' ] ) )

    #Make sure it's valid
    if chain.hasExpired()[ 'Value' ]:
      return S_ERROR( "Proxy %s has expired" % proxyLocation )

    rpcClient = RPCClient( "Framework/ProxyManager", proxyChain = chain )
    #Get a delegation request
    retVal = rpcClient.requestDelegation()
    if not retVal[ 'OK' ]:
      return retVal
    #Check if the delegation has been granted
    if 'Value' not in retVal or not retVal[ 'Value' ]:
      return S_OK()
    reqDict = retVal[ 'Value' ]
    #Generate delegated chain
    retVal = chain.generateChainFromRequestString( reqDict[ 'request' ] )
    if not retVal[ 'OK' ]:
      return retVal
    #Upload!
    return rpcClient.completeDelegation( reqDict[ 'id' ], retVal[ 'Value' ] )

  @gProxiesSync
  def downloadProxy( self, userDN, userGroup, limited = False, requiredTimeLeft = 43200, proxyToConnect = False ):
    """
    Get a proxy Chain from the proxy management
    """
    cacheKey = ( userDN, userGroup )
    if self.__proxiesCache.exists( cacheKey, requiredTimeLeft ):
      return S_OK( self.__proxiesCache.get( cacheKey ) )
    req = X509Request()
    req.generateProxyRequest( limited = limited )
    if proxyToConnect:
      rpcClient = RPCClient( "Framework/ProxyManager", proxyChain = proxyToConnect )
    else:
      rpcClient = RPCClient( "Framework/ProxyManager" )
    retVal = rpcClient.getProxy( userDN, userGroup, req.dumpRequest()['Value'], long( requiredTimeLeft * 1.5 ) )
    if not retVal[ 'OK' ]:
      return retVal
    chain = X509Chain( keyObj = req.getPKey() )
    retVal = chain.loadChainFromString( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    self.__proxiesCache.add( cacheKey, chain.getRemainingSecs()['Value'], chain )
    return S_OK( chain )

  def downloadProxyToFile( self, userDN, userGroup, limited = False, requiredTimeLeft = 43200, proxyToConnect = False ):
    """
    Get a proxy Chain from the proxy management and write it to file
    """
    retVal = self.downloadProxy( userDN, userGroup, limited, requiredTimeLeft, proxyToConnect )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = self.dumpProxyToFile( chain )
    if not retVal[ 'OK' ]:
      return retVal
    retVal[ 'chain' ] = chain
    return retVal

  @gVOMSProxiesSync
  def downloadVOMSProxy( self, userDN, userGroup, limited = False, requiredTimeLeft = 43200, requiredVOMSAttribute = False, proxyToConnect = False ):
    """
    Download a proxy if needed and transform it into a VOMS one
    """

    cacheKey = ( userDN, userGroup, requiredVOMSAttribute )
    if self.__vomsProxiesCache.exists( cacheKey, requiredTimeLeft ):
      return S_OK( self.__vomsProxiesCache.get( cacheKey ) )
    req = X509Request()
    req.generateProxyRequest( limited = limited )
    if proxyToConnect:
      rpcClient = RPCClient( "Framework/ProxyManager", proxyChain = proxyToConnect )
    else:
      rpcClient = RPCClient( "Framework/ProxyManager" )
    retVal = rpcClient.getVOMSProxy( userDN, userGroup, req.dumpRequest()['Value'], long( requiredTimeLeft * 1.5 ), requiredVOMSAttribute )
    if not retVal[ 'OK' ]:
      return retVal
    chain = X509Chain( keyObj = req.getPKey() )
    retVal = chain.loadChainFromString( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    self.__vomsProxiesCache.add( cacheKey, chain.getRemainingSecs()['Value'], chain )
    return S_OK( chain )

  def downloadVOMSProxyToFile( self, userDN, userGroup, limited = False, requiredTimeLeft = 43200, requiredVOMSAttribute = False, proxyToConnect = False ):
    """
    Download a proxy if needed, transform it into a VOMS one and write it to file
    """
    retVal = self.downloadVOMSProxy( userDN, userGroup, limited, requiredTimeLeft, requiredVOMSAttribute, proxyToConnect )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = self.dumpProxyToFile( chain )
    if not retVal[ 'OK' ]:
      return retVal
    retVal[ 'chain' ] = chain
    return retVal

  def __createPilotProxyFromMyProxy( self, userDN, userGroup, requiredTimeLeft = 43200, proxyToConnect = False ):
    retVal = self.downloadVOMSProxy( userDN, userGroup, requiredTimeLeft = 600, proxyToConnect = proxyToConnect )
    if not retVal[ 'OK' ]:
      return retVal
    originChain = retVal[ 'Value' ]
    myProxy = MyProxy( server = gConfig.getValue( "/DIRAC/VOPolicy/MyProxyServer", "myproxy.cern.ch" ) )
    retVal = myProxy.getDelegatedProxy( originChain, requiredTimeLeft * 1.5, useDNAsUserName = True )
    if not retVal[ 'OK' ]:
      return retVal
    mpChain = retVal[ 'Value' ]
    retVal = mpChain.getDIRACGroup()
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't retrieve DIRAC Group from downloaded proxy: %s" % retVal[ 'Message' ] )
    group = retVal[ 'Value' ]
    props = CS.getPropertiesForGroup( group )
    if Properties.GENERIC_PILOT in props:
      if userGroup != group:
        return S_ERROR( "Stored proxy in MyProxy has generic pilot group (%s) and requested group for a private one" % userGroup )
    vomsGroups = CS.getVOMSAttributeForGroup( userGroup )
    if not vomsGroups:
      return S_ERROR( "No voms attributes assigned to group %s" % userGroup )
    if len( vomsGroups ) > 1:
      return S_ERROR( "More than one voms attribute defined for group %s: %s" % ( userGroup, vomsGroups ) )
    vomsAttr = vomsGroups[0]
    vomsMgr = VOMS()
    return vomsMgr.setVOMSAttributes( mpChain , vomsAttr )

  @gPilotProxiesSync
  def downloadPilotProxy( self, userDN, userGroup, requiredTimeLeft = 43200, proxyToConnect = False ):
    """
    Download a pilot proxy with VOMS extensions depending on the group
    """
    cacheKey = ( userDN, userGroup )
    if self.__pilotProxiesCache.exists( cacheKey, requiredTimeLeft ):
      return S_OK( self.__pilotProxiesCache.get( cacheKey ) )
    retVal = self.__createPilotProxyFromMyProxy( userDN, userGroup, requiredTimeLeft, proxyToConnect )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    self.__pilotProxiesCache.add( cacheKey, chain.getRemainingSecs()['Value'], chain )
    return S_OK( chain )

  def downloadPilotProxyToFile( self, userDN, userGroup, requiredTimeLeft = 43200, proxyToConnect = False ):
    """
    Download a proxy if needed, transform it into a VOMS one and write it to file
    """
    retVal = self.downloadPilotProxy( userDN, userGroup, limited, requiredTimeLeft, proxyToConnect )
    if not retVal[ 'OK' ]:
      return retVal
    chain = retVal[ 'Value' ]
    retVal = self.dumpProxyToFile( chain )
    if not retVal[ 'OK' ]:
      return retVal
    retVal[ 'chain' ] = chain
    return retVal

  def dumpProxyToFile( self, chain, destinationFile = False, requiredTimeLeft = 600 ):
    """
    Dump a proxy to a file. It's cached so multiple calls won't generate extra files
    """
    if self.__filesCache.exists( chain, requiredTimeLeft ):
      filepath = self.__filesCache.get( chain )
      if os.path.isfile( filepath ):
        return S_OK( filepath )
      self.__filesCache.delete( filepath )
    retVal = chain.dumpAllToFile( destinationFile )
    if not retVal[ 'Value' ]:
      return retVal
    filename = retVal[ 'Value' ]
    self.__filesCache.add( chain, chain.getRemainingSecs()['Value'], filename )
    return S_OK( filename )

  def deleteGeneratedProxyFile( self, chain ):
    """
    Delete a file generated by a dump
    """
    self.__filesCache.delete( chain )
    return S_OK()

  def renewProxy( self, proxyToBeRenewed = False, minLifeTime = 3600, newProxyLifeTime = 43200, proxyToConnect = False ):
    """
    Renew a proxy using the ProxyManager
    Arguments:
      proxyToBeRenewed : proxy to renew
      minLifeTime : if proxy life time is less than this, renew. Skip otherwise
      newProxyLifeTime : life time of new proxy
      proxyToConnect : proxy to use for connecting to the service
    """
    retVal = File.multiProxyArgument( proxyToBeRenewed )
    if not retVal[ 'Value' ]:
      return retVal
    proxyToRenewDict = retVal[ 'Value' ]

    secs = proxyToRenewDict[ 'chain' ].getRemainingSecs()[ 'Value' ]
    if secs > minLifeTime:
      File.deleteMultiProxy( proxyToRenewDict )
      return S_OK()

    if not proxyToConnect:
      proxyToConnectDict = proxyToRenewDict
    else:
      retVal = File.multiProxyArgument( proxyToConnect )
      if not retVal[ 'Value' ]:
        File.deleteMultiProxy( proxyToRenewDict )
        return retVal
      proxyToConnectDict = retVal[ 'Value' ]

    userDN = proxyToRenewDict[ 'chain' ].getIssuerCert()[ 'Value' ].getSubjectDN()[ 'Value' ]
    retVal = proxyToRenewDict[ 'chain' ].getDIRACGroup()
    if not retVal[ 'OK' ]:
      File.deleteMultiProxy( proxyToRenewDict )
      File.deleteMultiProxy( proxyToConnectDict )
      return retVal
    userGroup = retVal[ 'Value' ]
    limited = proxyToRenewDict[ 'chain' ].isLimitedProxy()[ 'Value' ]

    voms = VOMS()
    retVal = voms.getVOMSAttributes( proxyToRenewDict[ 'chain' ] )
    if not retVal[ 'OK' ]:
      File.deleteMultiProxy( proxyToRenewDict )
      File.deleteMultiProxy( proxyToConnectDict )
      return retVal
    vomsAttrs = retVal[ 'Value' ]
    if vomsAttrs:
      retVal = self.downloadVOMSProxy( userDN,
                                       userGroup,
                                       limited = limited,
                                       requiredTimeLeft = newProxyLifeTime,
                                       requiredVOMSAttribute = vomsAttrs[0],
                                       proxyToConnect =  proxyToConnectDict[ 'chain' ] )
    else:
      retVal = self.downloadProxy( userDN,
                                   userGroup,
                                   limited = limited,
                                   requiredTimeLeft = newProxyLifeTime,
                                   proxyToConnect =  proxyToConnectDict[ 'chain' ] )

    File.deleteMultiProxy( proxyToRenewDict )
    File.deleteMultiProxy( proxyToConnectDict )

    if not retVal[ 'OK' ]:
      return retVal

    if not proxyToRenewDict[ 'tempFile' ]:
      return proxyToRenewDict[ 'chain' ].dumpAllToFile( proxyToRenewDict[ 'file' ] )

    return S_OK( proxyToRenewDict[ 'chain' ] )

  def getDBContents(self):
    """
    Get the contents of the db
    """
    rpcClient = RPCClient( "Framework/ProxyManager" )
    return rpcClient.getContents( {}, [ [ 'UserDN', 'DESC' ] ], 0, 0 )

gProxyManager = ProxyManagerClient()