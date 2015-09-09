#!/usr/bin/env python
"""
  dirac-rss-policy-manager

    Script that helps manage the Policy section in the CS setup 

    Usage:
        dirac-rss-policy-manager [option] <command>

    Commands:
        [test|view|update]

    Options:
        --name=               ElementName (it admits a comma-separated list of element names); None by default
        --element=            Element family (either 'Site' or 'Resource') 
        --elementType=        ElementType narrows the search (string, list); None by default
        --setup=              Setup where the policy section should be retrieved from; 'Defaults' by default        

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..
"""

from DIRAC                                                  import gConfig, gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base                                        import Script
from DIRAC.ResourceStatusSystem.Client                      import ResourceStatusClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
from DIRAC.Core.Utilities                                   import Time
from DIRAC.Core.Utilities.PrettyPrint                       import printTable 
import datetime

from DIRAC.ConfigurationSystem.private.ConfigurationData    import CFG
from DIRAC.ResourceStatusSystem.Policy                      import Configurations
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter        import InfoGetter
import json, pprint

__RCSID__ = '$Id:$'

subLogger = None
switchDict = {}

def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''

  switches = ( 
    ( 'elementType=', 'ElementType narrows the search; None if default' ),
    ( 'element=', 'Element family ( Site, Resource )' ),
    ( 'name=', 'ElementName; None if default' ),
    ( 'setup=', "Setup where the policy section should be retrieved from; 'Defaults' by default" )
    #( 'statusType=', 'A valid StatusType argument (it admits a comma-separated list of statusTypes); None if default' ),
    #( 'status=', 'A valid Status argument ( Active, Probing, Degraded, Banned, Unknown, Error ); None if default' ),
             )

  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )


def registerUsageMessage():
  '''
    Takes the script __doc__ and adds the DIRAC version to it
  '''

  usageMessage = 'DIRAC version: %s \n' % version
  usageMessage += __doc__

  Script.setUsageMessage( usageMessage )


def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''

  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if len( args ) == 0:
    error( "argument is missing, you should enter either 'test', 'update' or 'view'" )
  else:
    cmd = args[0].lower()

  switches = dict( Script.getUnprocessedSwitches() )

  # Default values
  switches.setdefault( 'name', None )
  switches.setdefault( 'element', None )
  switches.setdefault( 'elementType', None )
  switches.setdefault( 'setup', "Defaults" )
  #switches.setdefault( 'statusType', None )
  #switches.setdefault( 'status', None )

  # when it's a add/modify query and status/reason/statusType are not specified 
  #then some specific defaults are set up
  if cmd == 'test':
    if 'elementType' is None and 'element' is None and 'name' is None:
      return error("to check, you should enter at least one switch: either element, elmentType, or name")
    else:
      if switches[ 'element' ] != None:
        switches[ 'element' ] = switches[ 'element' ].title()
        if switches[ 'element' ] not in ( 'Resource', 'Site' ):
          error( "you should enter either 'Site' or 'Resource' for switch 'elementType'" )    
      if switches[ 'elementType' ] != None:
         switches[ 'elementType' ] = switches[ 'elementType' ].title()
      
  elif cmd == 'update' or cmd == 'view':
    pass
  else:
    error( "Incorrect argument: you should enter either 'test' or 'update' or 'view'" )


  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )
  
  return args, switches


#...............................................................................
# UTILS: to check and unpack

def error( msg ):
  '''
    Format error messages
  '''

  subLogger.error( "\nERROR:" )
  subLogger.error( "\t" + msg )
  subLogger.error( "\tPlease, check documentation below" )
  Script.showHelp()
  DIRACExit( 1 )

def getToken( key ):
  '''
    Function that gets the userName from the proxy
  '''

  proxyInfo = getProxyInfo()
  if not proxyInfo[ 'OK' ]:
    error( str( proxyInfo ) )

  if key.lower() == 'owner':
    userName = proxyInfo[ 'Value' ][ 'username' ]  
    tokenOwner = S_OK( userName )
    if not tokenOwner[ 'OK' ]:
      error( tokenOwner[ 'Message' ] )
    return tokenOwner[ 'Value' ]

  elif key.lower() == 'expiration':
    expiration = proxyInfo[ 'Value' ][ 'secondsLeft' ]
    tokenExpiration = S_OK( expiration )  
    if not tokenExpiration[ 'OK' ]:
      error( tokenExpiration[ 'Message' ] )

    now = Time.dateTime()   
    #datetime.datetime.utcnow()
    expirationDate = now + datetime.timedelta( seconds=tokenExpiration['Value'] )
    expirationDate = Time.toString( expirationDate )
    expirationDate = expirationDate.split('.')[0]
    return expirationDate 


#...............................................................................

def listOperationPolicies( setup = "Defaults" ):
  policies = getPolicySection( setup )
  for p in policies:
    print " "*3, p, " || matchParams: ", policies[p]['matchParams'], " || policyType: ", policies[p]['policyType']

def listAvailablePolicies():
  policiesMeta = Configurations.POLICIESMETA
  for pm in policiesMeta:
    print " "*3, pm, " || args: ", policiesMeta[pm]['args'], " || description: ", policiesMeta[pm]['description']

def getPolicySection( cfg, setup = "Defaults" ):
  return cfg['Operations'][setup]['ResourceStatus']['Policies']


def getPoliciesThatApply( params = None ):
  ig = InfoGetter()
  return ig.getPoliciesThatApply( params )


def updatePolicy( policySection ):
  print ""
  print "\t*** 3 steps to update each policy: enter a policy name, then its match params, then a policyType ***"
  while True:
      print ""
      #setting policyName
      name = raw_input("STEP1 - Enter a policy name (leave empty otherwise): ").strip()
      if name == "":
          break
      policySection[name] = {}
      policySection[name]['matchParams'] = {}  

      while True:
          #setting match params
          param = raw_input("STEP2 - Enter a match param (either 'element', 'name', 'elementType', leave empty otherwise): ").strip()
          if param == "":
              break
          if param not in ['element', 'name', 'elementType']:
              print "WARNING: you should enter either 'element', 'name', 'elementType', or leave it empty otherwise"
              continue 
          value = raw_input("STEP2 - Enter a value for match param " + param + " or a comma separated list of values (leave empty otherwise):").strip()
          if value == "":
              break
          policySection[name]['matchParams'][param] = value
 
      print ""
      #setting policy type
      print listAvailablePolicies()
      policy = raw_input("STEP3 - Enter a policyType (see the policy listed above, leave empty otherwise): ").strip()
      if policy == "":
          break  
      policySection[name]['policyType'] = policy
      
      print ""          
    
  return S_OK( policySection )                                  

def updateCfgPolicy( cfg, policySection, setup = 'Defaults' ):
  cfg['Operations'][setup]['ResourceStatus']['Policies'] = policySection
  return cfg 



def run( cmd, params):
  cmd = cmd.pop()
  fileCFG = CFG()
  fileName = "dirac.cfg"
  fileCFG.loadFromFile( fileName )
  cfg  = fileCFG.getAsDict()
  policySection = getPolicySection( cfg )

  if cmd == 'view':
    print json.dumps( policySection, indent=2, sort_keys=True )
  elif cmd == 'test':
    params = [ params[k] for k in params if params[k] ]
    result = getPoliciesThatApply( params )
    if result['OK']:
      print json.dumps( result['Value'], indent=2, sort_keys=True )
    else:
      print result
  elif cmd == 'update':
    result = updatePolicy( policySection )
    if result['OK']:
      print json.dumps( result['Value'], indent=2, sort_keys=True )
    else:
      print result    
#...............................................................................

if __name__ == "__main__":

  subLogger = gLogger.getSubLogger( __file__ )

  #Script initialization
  registerSwitches()
  registerUsageMessage()
  cmd, params = parseSwitches()
    
  #Unpack switchDict if 'name' or 'statusType' have multiple values
  #switchDictSet = unpack( switchDict )

  #Run script
  run( cmd, params )

  #Bye
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
