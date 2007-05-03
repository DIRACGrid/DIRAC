
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
  optionPath = "/Setups/%s/%s" % ( setup, systemName )
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

def getSystemURLSection( serviceName, serviceTuple = False ):
  systemSection = getSystemSection( serviceName, serviceTuple )
  return "%s/URLs" % systemSection

def getServiceURL( serviceName, serviceTuple = False ):
  if not serviceTuple:
    serviceTuple = divideFullName( serviceName )
  systemSection = getSystemSection( serviceName, serviceTuple )
  return "%s/URLs/%s" % ( systemSection, serviceTuple[1] )

def getGatewayURL():
  gatewayList = gConfigurationData.extractOptionFromCFG( "/DIRAC/Gateway" )
  if not gatewayList:
    return False
  return List.randomize( List.fromChar( gatewayList, "," ) )