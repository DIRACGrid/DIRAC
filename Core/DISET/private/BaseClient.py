# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/BaseClient.py,v 1.5 2007/05/08 14:44:07 acasajus Exp $
__RCSID__ = "$Id: BaseClient.py,v 1.5 2007/05/08 14:44:07 acasajus Exp $"

import DIRAC
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List, Network
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import *

class BaseClient:

  defaultGroup = "/lhcb"

  def __init__( self, serviceName,
                groupToUse = False,
                useCertificates = False,
                timeout = False ):
    self.serviceName = serviceName
    #self.setup = gConfig.get( "/DIRAC/Setup" )
    self.timeout = timeout
    self.serviceURL = self.__discoverServiceURL()
    try:
      retVal = Network.splitURL( self.serviceURL )
      if retVal[ 'OK' ]:
        self.URLTuple = retVal[ 'Value' ]
      else:
        return retVal
    except:
      gLogger.exception()
      gLogger.error( "URL is malformed", "%s is not valid" % self.sURL)
      return S_ERROR( "URL is malformed" )
    if groupToUse:
      self.groupToUse = groupToUse
    else:
      #TODO: Get real role
      self.groupToUse = self.defaultGroup

  def __discoverServiceURL( self ):
    for protocol in gProtocolDict.keys():
      if self.serviceName.find( "%s://" % protocol ) == 0:
        gLogger.debug( "Already given a valid url", self.serviceName )
        return self.serviceName

    dRetVal = gConfig.get( "/DIRAC/Gateway" )
    if dRetVal[ 'OK' ]:
      gLogger.debug( "Using gateway", "%s" % dRetVal[ 'Value' ] )
      return "%s/%s" % ( List.randomize( List.fromChar( dRetVal[ 'Value'], "," ) ), self.serviceName )

    dRetVal = getServiceURL( self.serviceName )
    if not dRetVal[ 'OK' ]:
      raise Exception( dRetVal[ 'Error' ] )
    sURL = List.randomize( List.fromChar( dRetVal[ 'Value'], "," ) )
    gLogger.debug( "Discovering URL for service", "%s -> %s" % ( sConfigurationPath, sURL ) )
    return sURL

  def _connect( self ):
    try:
      gLogger.debug( "Using %s protocol" % self.URLTuple[0] )
      self.transport = gProtocolDict[ self.URLTuple[0] ]( self.URLTuple[1:3] )
      self.transport.initAsClient()
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Can't connect: %s" % str( e ) )
    return S_OK()

  def _proposeAction( self, sAction ):
    stConnectionInfo = ( ( self.URLTuple[3], DIRAC.setup ), sAction )
    self.transport.sendData( stConnectionInfo )
    return self.transport.receiveData()
