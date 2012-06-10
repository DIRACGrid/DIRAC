# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

def getDIRACSetup():
  return gConfigurationData.extractOptionFromCFG( "/DIRAC/Setup" )

def divideFullName( entityName ):
  fields = [ field.strip() for field in entityName.split( "/" ) ]
  if len( fields ) < 2:
    raise RuntimeError( "Service (%s) name must be with the form system/service" % entityName )
  return tuple( fields )

def getSystemInstance( systemName, setup = False ):
  if not setup:
    setup = gConfigurationData.extractOptionFromCFG( "/DIRAC/Setup" )
  optionPath = "/DIRAC/Setups/%s/%s" % ( setup, systemName )
  instance = gConfigurationData.extractOptionFromCFG( optionPath )
  if instance:
    return instance
  else:
    raise RuntimeError( "Option %s is not defined" % optionPath )

def getSystemSection( serviceName, serviceTuple = False, instance = False, setup = False ):
  if not serviceTuple:
    serviceTuple = divideFullName( serviceName )
  if not instance:
    instance = getSystemInstance( serviceTuple[0], setup = setup )
  return "/Systems/%s/%s" % ( serviceTuple[0], instance )

def getServiceSection( serviceName, serviceTuple = False, setup = False ):
  if not serviceTuple:
    serviceTuple = divideFullName( serviceName )
  systemSection = getSystemSection( serviceName, serviceTuple, setup = setup )
  return "%s/Services/%s" % ( systemSection, serviceTuple[1] )

def getAgentSection( agentName, agentTuple = False, setup = False ):
  if not agentTuple:
    agentTuple = divideFullName( agentName )
  systemSection = getSystemSection( agentName, agentTuple, setup = setup )
  return "%s/Agents/%s" % ( systemSection, agentTuple[1] )

def getExecutorSection( agentName, agentTuple = False, setup = False ):
  if not agentTuple:
    agentTuple = divideFullName( agentName )
  systemSection = getSystemSection( agentName, agentTuple, setup = setup )
  return "%s/Executors/%s" % ( systemSection, agentTuple[1] )

def getDatabaseSection( dbName, dbTuple = False, setup = False ):
  if not dbTuple:
    dbTuple = divideFullName( dbName )
  systemSection = getSystemSection( dbName, dbTuple, setup = setup )
  return "%s/Databases/%s" % ( systemSection, dbTuple[1] )

def getSystemURLSection( serviceName, serviceTuple = False, setup = False ):
  systemSection = getSystemSection( serviceName, serviceTuple, setup = setup )
  return "%s/URLs" % systemSection

def getServiceURL( serviceName, serviceTuple = False, setup = False ):
  if not serviceTuple:
    serviceTuple = divideFullName( serviceName )
  systemSection = getSystemSection( serviceName, serviceTuple, setup = setup )
  url = gConfigurationData.extractOptionFromCFG( "%s/URLs/%s" % ( systemSection, serviceTuple[1] ) )
  if not url:
    return ""
  if len( url.split( "/" ) ) < 5:
    url = "%s/%s" % ( url, serviceName )
  return url

def getGatewayURLs( serviceName = "" ):
  siteName = gConfigurationData.extractOptionFromCFG( "/LocalSite/Site" )
  if not siteName:
    return False
  gatewayList = gConfigurationData.extractOptionFromCFG( "/DIRAC/Gateways/%s" % siteName )
  if not gatewayList:
    return False
  if serviceName:
    gatewayList = [ "%s/%s" % ( "/".join( gw.split( "/" )[:3] ), serviceName ) for gw in List.fromChar( gatewayList, "," ) ]
  return List.randomize( gatewayList )
