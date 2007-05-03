# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/ServiceConfiguration.py,v 1.3 2007/05/03 18:59:48 acasajus Exp $
__RCSID__ = "$Id: ServiceConfiguration.py,v 1.3 2007/05/03 18:59:48 acasajus Exp $"

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection

class ServiceConfiguration:

  def __init__( self, serviceName ):
    self.serviceName = serviceName
    self.serviceURL = False
    self.serviceSectionPath = getServiceSection( serviceName )

  def getOption( self, optionName ):
    if optionName[0] != "/":
      optionName = "%s/%s" % ( self.serviceSectionPath, optionName )
    return gConfigurationData.extractOptionFromCFG( optionName )

  def getHandlerLocation( self ):
    return self.getOption( "HandlerPath" )

  def getName( self ):
    return self.serviceName

  def setURL( self, sURL ):
    self.serviceURL = sURL

  def getURL( self, URL = False ):
    optionValue = self.getOption( "URL" )
    if optionValue:
      return optionValue
    elif URL:
        return URL
    else:
        return self.serviceURL

  def getMaxThreads( self ):
    try:
      return int( self.getOption( "MaxThreads" ) )
    except:
      return 1000

  def getMaxWaitingPetitions( self ):
    try:
      return int( self.getOption( "MaxWaitingPetitions" ) )
    except:
      return 100

  def getMaxThreadsPerFunction( self, funcName ):
    try:
      return int( self.getOption( "%sMaxThreads" % funcName ) )
    except:
      return 1000

  def getPort( self ):
    try:
      return int( self.getOption( "Port" ) )
    except:
      return 9876

  def getProtocol( self ):
    optionValue = self.getOption( "Protocol" )
    if optionValue:
      return optionValue
    return "diset"

  def getSectionPath( self ):
    return self.serviceSectionPath
