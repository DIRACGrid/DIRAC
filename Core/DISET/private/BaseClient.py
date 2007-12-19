# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/BaseClient.py,v 1.21 2007/12/19 17:51:40 acasajus Exp $
__RCSID__ = "$Id: BaseClient.py,v 1.21 2007/12/19 17:51:40 acasajus Exp $"

import DIRAC
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List, Network
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import *
from DIRAC.Core.Utilities import GridCredentials

class BaseClient:

  defaultUserGroup = "lhcb"
  defaultHostGroup = "hosts"

#TODO

  def __init__( self, serviceName,
                groupToUse = False,
                useCertificates = "auto",
                timeout = False,
                setup = "",
                delegatedDN = "",
                delegatedGroup = "" ):
    self.serviceName = serviceName
    self.timeout = timeout
    #Which setup to use?
    if setup:
      self.setup = str( setup )
    else:
      self.setup = gConfig.getValue( "/DIRAC/Setup", "LHCb-Development" )
    #Calculate final URL
    self.serviceURL = self.__discoverServiceURL()
    retVal = Network.splitURL( self.serviceURL )
    if retVal[ 'OK' ]:
      self.URLTuple = retVal[ 'Value' ]
    else:
      gLogger.error( "URL is malformed", retVal[ 'Message' ] )
      raise Exception( retVal[ 'Message' ] )
    #Use certificates?
    self.useCertificates = useCertificates
    if self.useCertificates == "auto":
      self.useCertificates = gConfig._useServerCertificate()
    #Wich group to use?
    if groupToUse:
      self.groupToUse = groupToUse
    else:
      if self.useCertificates:
        self.groupToUse = self.defaultHostGroup
      else:
        self.groupToUse = GridCredentials.getDIRACGroup( self.defaultUserGroup )
    #Are we delegating something?
    if delegatedDN:
      if delegatedGroup:
        self.groupToUse = delegatedGroup
      self.groupToUse = ( delegatedDN, self.groupToUse )
    self.__checkTransportSanity()

  def __discoverServiceURL( self ):
    for protocol in gProtocolDict.keys():
      if self.serviceName.find( "%s://" % protocol ) == 0:
        gLogger.debug( "Already given a valid url", self.serviceName )
        return self.serviceName

    dRetVal = gConfig.getOption( "/DIRAC/Gateway" )
    if dRetVal[ 'OK' ]:
      gLogger.debug( "Using gateway", "%s" % dRetVal[ 'Value' ] )
      return "%s/%s" % ( List.randomize( List.fromChar( dRetVal[ 'Value'], "," ) ), self.serviceName )

    urls = getServiceURL( self.serviceName, setup = self.setup )
    if not urls:
      raise Exception( "URL for service %s not found" % self.serviceName )
    sURL = List.randomize( List.fromChar( urls, "," ) )[0]
    gLogger.debug( "Discovering URL for service", "%s -> %s" % ( self.serviceName, sURL ) )
    return sURL

  def _connect( self ):
    gLogger.info( "Connecting to: %s" % self.serviceURL )
    try:
      gLogger.debug( "Using %s protocol" % self.URLTuple[0] )
      self.transport = gProtocolDict[ self.URLTuple[0] ][0]( self.URLTuple[1:3], useCertificates = self.useCertificates )
      self.transport.initAsClient()
    except Exception, e:
      return S_ERROR( "Can't connect: %s" % str( e ) )
    return S_OK()

  def _proposeAction( self, sAction ):
    stConnectionInfo = ( ( self.URLTuple[3], self.setup ), sAction, self.groupToUse )
    self.transport.sendData( S_OK( stConnectionInfo ) )
    return self.transport.receiveData()

  def __checkTransportSanity( self ):
      retVal = gProtocolDict[ self.URLTuple[0] ][1]( self.URLTuple[1:3], useCertificates = self.useCertificates )
      if not retVal:
        DIRAC.abort( 10, "Insane environment for protocol" )

  def __nonzero__( self ):
    return True