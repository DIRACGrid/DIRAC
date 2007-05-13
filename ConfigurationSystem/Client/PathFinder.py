# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/Client/PathFinder.py,v 1.4 2007/05/13 20:44:57 atsareg Exp $
__RCSID__ = "$Id: PathFinder.py,v 1.4 2007/05/13 20:44:57 atsareg Exp $"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

def getDIRACSetup():
  return gConfigurationData.extractOptionFromCFG( "/DIRAC/Setup" )

def divideFullName( entityName ):
  fields = [ field.strip() for field in entityName.split( "/" ) ]
  if len( fields ) < 2:
    raise Exception( "Service (%s) name must be with the form system/service" % entityName )
  return tuple( fields )

def getSystemInstance( systemName ):
  setup = gConfigurationData.extractOptionFromCFG( "/DIRAC/Setup" )
  optionPath = "/DIRAC/Setups/%s/%s" % ( setup, systemName )
  instance = gConfigurationData.extractOptionFromCFG( optionPath )
  if instance:
    return instance
  else:
    raise Exception( "Option %s is not defined" % optionPath )

def getSystemSection( serviceName, serviceTuple = False, instance = False ):
  if not serviceTuple:
    serviceTuple = divideFullName( serviceName )
  if not instance:
    instance = getSystemInstance( serviceTuple[0] )
  return "/Systems/%s/%s" % ( serviceTuple[0], instance )

def getServiceSection( serviceName, serviceTuple = False ):
  if not serviceTuple:
    serviceTuple = divideFullName( serviceName )
  systemSection = getSystemSection( serviceName, serviceTuple )
  return "%s/Services/%s" % ( systemSection, serviceTuple[1] )

def getAgentSection( agentName, agentTuple = False ):
  if not agentTuple:
    agentTuple = divideFullName( agentName )
  systemSection = getSystemSection( agentName, agentTuple )
  return "%s/Agents/%s" % ( systemSection, agentTuple[1] )
  
def getDatabaseSection(dbName, dbTuple = False):
  if not dbTuple:
    dbTuple = divideFullName( dbName )
  systemSection = getSystemSection( dbName, dbTuple )
  return "%s/Databases/%s" % ( systemSection, dbTuple[1] )  

def getSystemURLSection( serviceName, serviceTuple = False ):
  systemSection = getSystemSection( serviceName, serviceTuple )
  return "%s/URLs" % systemSection

def getServiceURL( serviceName, serviceTuple = False ):
  if not serviceTuple:
    serviceTuple = divideFullName( serviceName )
  systemSection = getSystemSection( serviceName, serviceTuple )
  return gConfigurationData.extractOptionFromCFG( "%s/URLs/%s" % ( systemSection, serviceTuple[1] ) )

def getGatewayURL():
  gatewayList = gConfigurationData.extractOptionFromCFG( "/DIRAC/Gateway" )
  if not gatewayList:
    return False
  return List.randomize( List.fromChar( gatewayList, "," ) )
