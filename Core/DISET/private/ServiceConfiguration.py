# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/ServiceConfiguration.py,v 1.2 2007/03/16 11:58:30 rgracian Exp $
__RCSID__ = "$Id: ServiceConfiguration.py,v 1.2 2007/03/16 11:58:30 rgracian Exp $"

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

class ServiceConfiguration:
  
  def __init__( self, sService ):
    self.sServiceName = sService
    self.sServiceURL = False
    self.sServicePath = "%s/%s" % ( gConfigurationData.getServicesPath(), sService )
    
  def getOption( self, sOption ):
    if sOption[0] != "/":
      sOption = "%s/%s" % ( self.sServicePath, sOption )
    return gConfigurationData.extractOptionFromCFG( sOption )
      
  def getHandlerLocation( self ):
    return self.getOption( "HandlerPath" )
  
  def getName( self ):
    return self.sServiceName

  def setURL( self, sURL ):
    self.sServiceURL = sURL
  
  def getURL( self, sURL = False ):
    sValue = self.getOption( "URL" )
    if sValue:
      return sValue
    elif sURL:
        return sURL
    else:
        return self.sServiceURL  
    
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
    
  def getMaxThreadsPerFunction( self, sFunction ):
    try:
      return int( self.getOption( "%sMaxThreads" % sFunction ) )
    except:
      return 1000
    
  def getPort( self ):
    try:
      return int( self.getOption( "Port" ) )
    except:
      return 9876
    
  def getInstance( self ):
    return self.getOption( "/Local/DIRACInstace" )
