#!/usr/bin/env python
"""
  dirac-rss-policy-manager

    Script to manage the Policy section within a given CS setup of a given dirac cfg file. 
    It allows you to:
        - view the policy current section (no option needed)
        - test all the policies that apply for a given 'element', 'elementType' or element 'name' 
          (one of the aforementioned options is needed)
        - update/add a policy to a given dirac cfg file (no option needed)
        - remove a policy from a given dirac cfg file ('policy' option needed)
        - restore the last backup of the diarc config file, to undo last changes (no option needed) 
        
        
    Usage:
        dirac-rss-policy-manager [option] <command>

    Commands:
        [test|view|update|remove]

    Options:
        --name=               ElementName (it admits a comma-separated list of element names); None by default
        --element=            Element family (either 'Site' or 'Resource') 
        --elementType=        ElementType narrows the search (string, list); None by default
        --setup=              Setup where the policy section should be retrieved from; 'Defaults' by default
        --file=               Fullpath config file location other then the default one (but for testing use only the original)
        --policy=             Policy name to be removed        

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
from DIRAC.ResourceStatusSystem.Utilities                   import CSHelpers
import json, shutil

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
    ( 'setup=', "Setup where the policy section should be retrieved from; 'Defaults' by default" ),
    ( 'file=', "Fullpath config file location other then the default one (but for testing use only the original)" ),
    ( 'policy=', "Policy name to be removed" )
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
    error( "Argument is missing, you should enter either 'test', 'update', 'view', 'remove', 'restore'" )
  else:
    cmd = args[0].lower()

  switches = dict( Script.getUnprocessedSwitches() )
  diracConfigFile = CSHelpers.gConfig.diracConfigFilePath

  # Default values
  switches.setdefault( 'name', None )
  switches.setdefault( 'element', None )
  switches.setdefault( 'elementType', None )
  switches.setdefault( 'setup', "Defaults" )
  switches.setdefault( 'file', diracConfigFile )
  #switches.setdefault( 'statusType', None )
  #switches.setdefault( 'status', None )

  # when it's a add/modify query and status/reason/statusType are not specified 
  #then some specific defaults are set up
  if cmd == 'test':
    if switches['elementType'] is None and switches['element'] is None and switches['name'] is None:
      error( "to test, you should enter at least one switch: either element, elmentType, or name" )
    else:
      if switches[ 'element' ] != None:
        switches[ 'element' ] = switches[ 'element' ].title()
        if switches[ 'element' ] not in ( 'Resource', 'Site' ):
          error( "you should enter either 'Site' or 'Resource' for switch 'element'" )    
      if switches[ 'elementType' ] != None:
         switches[ 'elementType' ] = switches[ 'elementType' ].title()
      if switches[ 'file' ] == None:
        error("Enter a fullpath dirac config file location when using 'file' option")
  elif cmd == 'remove' :
    if 'policy' not in switches or switches['policy'] is None:
      error( "to remove, you should enter a policy" )    
  elif cmd == 'update' or cmd == 'view' or cmd == 'restore':
    pass
  else:
    error( "Incorrect argument: you should enter either 'test', 'update', 'view', 'remove', 'restore'" )


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

def listCSPolicies( setup = "Defaults" ):
  '''
    to get the list of the policies from the dirac config file
  '''
  
  policies = getPolicySection( setup )
  for p in policies:
    print " "*3, p, " || matchParams: ", policies[p]['matchParams'], " || policyType: ", policies[p]['policyType']

def listAvailablePolicies():
  '''
    to get the list of the policies available in the RSS.Policy.Configurations
  '''
  
  policiesMeta = Configurations.POLICIESMETA
  for pm in policiesMeta:
    print " "*3, pm, " || args: ", policiesMeta[pm]['args'], " || description: ", policiesMeta[pm]['description']

def getPolicySection( cfg, setup = "Defaults" ):
  '''
    to get the Policy section from the dirac config file, and within a given setup
  '''

  return cfg['Operations'][setup]['ResourceStatus']['Policies']


def getPoliciesThatApply( params ):
  '''
    to get all the policies that apply to the given list of params
  '''
  
  paramsClone = dict( params )
  for param in paramsClone:
    if params[param] is None:
      del params[param]
  
  ig = InfoGetter()
  result = ig.getPoliciesThatApply( params )
  if result['OK']:
    return result['Value'] 
  else:
    error( "It wasn't possible to execute getPoliciesThatApply, check this: %s" % str(result) )


def updatePolicy( policySection ):
  '''
    to interactively update/add policies inside the dirac config file
  ''' 
  
  headLine( "3 steps to update/add a policy: enter a policy name, then its match params, then a policyType" )
  while True:
      #setting policyName
      name = raw_input("STEP1 - Enter a policy name (leave empty otherwise): ").strip()
      if name == "":
          break
      policySection[name] = {}
      policySection[name]['matchParams'] = {}
      
      params = ['element', 'name', 'elementType']
      while True:
        print ""
        print "\t WARNING:"
        print "\t if you enter 'element' as param then you should enter 'Site' or 'Resource' as a value"
        print "\t if you enter 'name' as param then you should enter either a name or a comma-separated list of names\n"
        
        #setting match params
        param = raw_input("STEP2 - Enter a match param (among %s), or leave empty otherwise: " % str( params )).strip()
        if param == "":
            break
        if param not in params:
            print "\t WARNING: you should enter a match param (among %s), or leave it empty otherwise" % str( params )
            continue 
        value = raw_input("STEP2 - Enter a value for match param '" + param + "', leave it empty otherwise:").strip()
        if value == "":
            break
        if param == 'element':
          value = value.title()
          if value != 'Site' and value != 'Resource':
            error( "You didn't provide either 'Site' or 'Resource' as a value for match param 'element'" )
        policySection[name]['matchParams'][param] = value
        params.remove( param )
 
      #setting policy type
      headLine( "LIST OF AVAILABLE POLICIES" )
      print listAvailablePolicies()
      policy = raw_input("STEP3 - Enter a policyType (see one of the the policies listed above, leave empty otherwise): ").strip()
      if policy == "":
          break  
      policySection[name]['policyType'] = policy
      
      headLine( " Enter another policy, if you like" )        
    
  return S_OK( policySection )                                  



def removePolicy( policySection, policies ):
  '''
    to remove some policies from the dirac config file
  ''' 
    
  for policy in policies.split(','):
    if policy == '':
      continue 
    if policy in policySection:
      del policySection[ policy ]
    else:
      print "\n\t WARNING: No policy named %s was found in the Policy section!" % policy   
    
  return policySection


def dumpPolicy( cfgDict, fileName ):
  '''
    to copy updates and removals to the dirac config file (it creates a backup copy, if needed for restoring)
  '''   
  
  fileCFG = CFG()
  #update cfg policy section
  confirmation = raw_input("Do you want to dump your changes? (replay 'yes' or 'y' to confirm): ").strip()
  if confirmation == 'yes' or confirmation == 'y':
    fileCFG.loadFromDict( cfgDict )
    shutil.copyfile( fileName, fileName + ".bkp" ) #creates a backup copy of the dirac config file
    dumpedSucccessfully = fileCFG.writeToFile( fileName )
    if dumpedSucccessfully:
      print "Your update has been dumped successfully!"
    else:
      print "It was not possible to dump your update. Something went wrong!"

def viewPolicyDict( policyDict ):
  '''
    to "prettyprint" a python dictionary
  '''
  
  print json.dumps( policyDict, indent=2, sort_keys=True )

def restoreCfgFile( fileName ):
  '''
    to restore the last backup copy of the dirac config file before the latest updates/removals
  '''  
  
  shutil.copyfile( fileName + ".bkp", fileName )
  print "\n\tWARNING: dirac config file was restored!"

def headLine( text ):
  '''
    to create a pretty printout headline 
  ''' 
  
  print "\n\t*** %s ***\n" % text

def run( cmd, params):
  '''
    to execute a command among view, test, update, remove, restore 
  ''' 
  
  cmd = cmd.pop()
  fileCFG = CFG()
  fileName = params[ 'file' ]
  setup = params[ 'setup' ]
  fileCFG.loadFromFile( fileName )
  cfgDict = fileCFG.getAsDict()
  policySection = getPolicySection( cfgDict )

  if cmd == 'view':
    viewPolicyDict( policySection )
    
  elif cmd == 'test':
    policiesThatApply = getPoliciesThatApply( params ) 
    viewPolicyDict( policiesThatApply )
      
  elif cmd == 'update':
    result = updatePolicy( policySection )
    if result['OK']:
      policySection = result['Value']
      cfgDict['Operations'][setup]['ResourceStatus']['Policies'] = policySection
      headLine( "A preview of your policy section after the update" )
      viewPolicyDict( policySection )      
      dumpPolicy( cfgDict, fileName )
        
  elif cmd == 'remove':
    policies = params[ 'policy' ]
    policySection = removePolicy( policySection, policies )
    cfgDict['Operations'][setup]['ResourceStatus']['Policies'] = policySection
    headLine( "A preview of your policy section after the removal" )
    viewPolicyDict( policySection )      
    dumpPolicy( cfgDict, fileName )
  
  elif cmd == 'restore':
    restoreCfgFile( fileName )
    fileCFG.loadFromFile( fileName )
    cfgDict = fileCFG.getAsDict()
    policySection = getPolicySection( cfgDict )
    viewPolicyDict( policySection )
         
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