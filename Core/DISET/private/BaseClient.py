# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/BaseClient.py,v 1.2 2007/03/16 11:58:30 rgracian Exp $
__RCSID__ = "$Id: BaseClient.py,v 1.2 2007/03/16 11:58:30 rgracian Exp $"

from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
      
class BaseClient:
  
  sDefaultRole = "/lhcb"

  def __splitURL( self ):
    self.iPort = 9135
    iProtocolEnd = self.sURL.find( "://" )
    if iProtocolEnd == -1:
      raise Exception( "'%s' URL is malformed" % self.sURL )
    self.sProtocol = self.sURL[ : iProtocolEnd ]
    sRemainingURL = self.sURL[ iProtocolEnd + 3: ]
    iPathStart = sRemainingURL.find( "/" )
    if iPathStart > -1:
      self.sHost = sRemainingURL[ :iPathStart ]
      self.sPath = sRemainingURL[ iPathStart + 1: ]
    else:
      self.sHost = sRemainingURL
      self.sPath = "/"
    if self.sPath[-1] == "/":
      self.sPath = self.sPath[:-1]
    iSplitter = self.sHost.find( ":" )
    if iSplitter > -1:
      self.iPort = int( self.sHost[ iSplitter+1: ] )
      self.sHost = self.sHost[ :iSplitter ]
  
  def __init__( self, sServiceName, 
                sForcedRole = False, 
                bAllowCertificates = False, 
                iTimeout = False ):
    self.sServiceName = sServiceName
    self.sDIRACInstance = self.__discoverInstance()
    self.sURL = self.__discoverServiceURL()
    try:
      self.__splitURL()
    except:
      gLogger.exception()
      gLogger.error( "URL is malformed", "%s is not valid" % self.sURL)
      return S_ERROR( "URL is malformed" )
    if self.sProtocol == "diset":
      try:
        if not sForcedRole:
          self.sRole = sForcedRole
        else:
          #TODO: Get real role
          self.sRole = self.sDefaultRole
      except:
        gLogger.warn( "No role defined", "Using default role %s" % self.sDefaultRole )
        self.sRole = self.sDefaultRole
      
  def __discoverServiceURL( self ):
    if self.sServiceName.find( "diset://" ) or self.sServiceName.find( "dit://" ):
      gLogger.debug( "Already given a valid url", self.sServiceName )
      return self.sServiceName
    
    sServicesPath = "/DIRACInstances"
    dRetVal = gConfig.getOption( "%s/%s/Gateway" % ( sServicesPath, self.sDIRACInstance ) )
    if dRetVal[ 'OK' ]:
      gLogger.debug( "Using gateway", "%s" % dRetVal[ 'Value' ] )
      return "%s/%s" % ( dRetVal[ 'Value' ], self.sServiceName )
    
    sConfigurationPath = "%s/%s/%s" % ( sServicesPath, self.sDIRACInstance, self.sServiceName )
    dRetVal = gConfig.getOption( sConfigurationPath )
    if not dRetVal[ 'OK' ]:
      raise Exception( dRetVal[ 'Error' ] )
    sURL = List.randomize( List.fromChar( dRetVal[ 'Value'], "," ) )
    gLogger.debug( "Discovering URL for service", "%s -> %s" % ( sConfigurationPath, sURL ) )
    return sURL
      
  def __discoverInstance( self ):
    #TODO: Put the proper path of the instance
    dRetVal = gConfig.getOption( "/Local/DIRACInstance" )
    if not dRetVal[ 'OK' ]:
      return "production"
    return dRetVal[ 'Value' ]
    
  def _connect( self ):
    try:
      if self.sProtocol == "diset":
        gLogger.debug( "Using diset protocol" )
        from DIRAC.Core.DISET.private.Transports.SSLTransport import SSLTransport
        self.oServerTransport = SSLTransport(  ( self.sHost, self.iPort ) )
      elif self.sProtocol == "dit":
        gLogger.debug( "Using dit protocol" )
        from DIRAC.Core.DISET.private.Transports.PlainTransport import PlainTransport
        self.oServerTransport = PlainTransport( ( self.sHost, self.iPort ) )
      else:
        return S_ERROR( "Unknown protocol (%s)" % self.sProtocol )
      self.oServerTransport.initAsClient()
    except Exception, e:
      return S_ERROR( "Can't connect: %s" % str( e ) )
    return S_OK()
