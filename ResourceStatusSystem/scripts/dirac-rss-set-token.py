#!/usr/bin/env python
""" 
  dirac-rss-set-token
  
    Script that helps setting the token of the elements in RSS. It can acquire or
    release the token. If the releaseToken switch is used, no matter what was the
    previous token, it will be set to rs_svc ( RSS owns it ). If not set, the token
    will be set to whatever username is defined on the proxy loaded while issuing
    this command. In the second case, the token lasts one day.
    
"""

from datetime import datetime, timedelta

# DIRAC
from DIRAC                                                  import gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base                                        import Script

__RCSID__ = '$Id: $'

subLogger  = None
switchDict = {}

#...............................................................................

def registerSwitches():
  """
  Registers all switches that can be used while calling the script from the
  command line interface.
  """
  
  switches = (
    ( 'element=',     'Element family to be Synchronized ( Site, Resource or Node )' ),
    ( 'name=',        'Name, name of the element where the change applies' ),
    ( 'statusType=',  'StatusType, if none applies to all possible statusTypes' ),
    ( 'reason=',      'Reason to set the Status' ),
    ( 'releaseToken', 'Release the token and let the RSS go' )
             )
  
  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )

def registerUsageMessage():
  """
  Takes the script __doc__ and adds the DIRAC version to it
  """

  hLine = '  ' + '='*78 + '\n'
  
  usageMessage = hLine
  usageMessage += '  DIRAC %s\n' % version
  usageMessage += __doc__
  usageMessage += '\n' + hLine
  
  Script.setUsageMessage( usageMessage )

def parseSwitches():
  """
  Parses the arguments passed by the user
  """
  
  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if args:
    subLogger.error( "Found the following positional args '%s', but we only accept switches" % args )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )
  
  switches = dict( Script.getUnprocessedSwitches() )  
  switches.setdefault( 'statusType'  , None )
  switches.setdefault( 'releaseToken', False )
  
  for key in ( 'element', 'name', 'reason' ):

    if not key in switches:
      subLogger.error( "%s Switch missing" % key )
      subLogger.error( "Please, check documentation below" )
      Script.showHelp()
      DIRACExit( 1 )
    
  if not switches[ 'element' ] in ( 'Site', 'Resource', 'Node' ):
    subLogger.error( "Found %s as element switch" % switches[ 'element' ] )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )
    
  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )

  return switches  

#...............................................................................

def proxyUser():
  """
  Read proxy to get username.
  """

  res = getProxyInfo()
  if not res[ 'OK' ]:
    return res
  
  return S_OK( res[ 'Value' ][ 'username' ] ) 

def setToken( user ):
  '''
    Function that gets the user token, sets the validity for it. Gets the elements
    in the database for a given name and statusType(s). Then updates the status
    of all them adding a reason and the token.
  '''
  
  rssClient = ResourceStatusClient()
  
  # This is a little bit of a nonsense, and certainly needs to be improved.
  # To modify a list of elements, we have to do it one by one. However, the
  # modify method does not discover the StatusTypes ( which in this script is
  # an optional parameter ). So, we get them from the DB and iterate over them.
  elements = rssClient.selectStatusElement( switchDict[ 'element' ], 'Status', 
                                            name       = switchDict[ 'name' ], 
                                            statusType = switchDict[ 'statusType' ], 
                                            meta = { 'columns' : [ 'StatusType', 'TokenOwner' ] } )
  
  if not elements[ 'OK']:
    return elements
  elements = elements[ 'Value' ]
  
  # If there list is empty they do not exist on the DB !
  if not elements:
    subLogger.warn( 'Nothing found for %s, %s, %s' % ( switchDict[ 'element' ],
                                                       switchDict[ 'name' ],
                                                       switchDict[ 'statusType' ] ) )
    return S_OK()
   
  # If we want to release the token
  if switchDict[ 'releaseToken' ] != False:
    tokenExpiration = datetime.max
    newTokenOwner   = 'rs_svc'
  else:
    tokenExpiration = datetime.utcnow().replace( microsecond = 0 ) + timedelta( days = 1 )
    newTokenOwner   = user 
  
  subLogger.info( 'New token : %s until %s' % ( newTokenOwner, tokenExpiration ) )
  
  for statusType, tokenOwner in elements:
    
    # If a user different than the one issuing the command and RSS
    if tokenOwner != user and tokenOwner != 'rs_svc':
      subLogger.info( '%s(%s) belongs to the user: %s' % ( switchDict[ 'name' ], statusType, tokenOwner ) )
        
    # does the job    
    result = rssClient.modifyStatusElement( switchDict[ 'element' ], 'Status', 
                                            name       = switchDict[ 'name' ], 
                                            statusType = statusType,
                                            reason     = switchDict[ 'reason'],  
                                            tokenOwner = newTokenOwner, 
                                            tokenExpiration = tokenExpiration )
    if not result[ 'OK' ]:
      return result
    
    if tokenOwner == newTokenOwner:
      msg = '(extended)'
    elif newTokenOwner == 'rs_svc':
      msg = '(released)'
    else:
      msg = '(aquired from %s)' % tokenOwner
      
    subLogger.info( '%s:%s %s' % ( switchDict[ 'name' ], statusType, msg ) )  
  return S_OK()
  
def main():
  """
  Main function of the script. Gets the username from the proxy loaded and sets
  the token taking into account that user and the switchDict parameters.
  """
  
  user = proxyUser()
  if not user[ 'OK' ]:
    subLogger.error( user[ 'Message' ] )
    DIRACExit( user[ 'Message' ] )
  user = user[ 'Value' ]
  
  res = setToken( user )
  if not res[ 'OK' ]:
    subLogger.error( res[ 'Message' ] )
    DIRACExit( res[ 'Message' ] )

#...............................................................................

if __name__ == '__main__':
  
  # Logger initialization
  subLogger  = gLogger.getSubLogger( __file__ )

  # Script initialization
  registerSwitches()
  registerUsageMessage()
  switchDict = parseSwitches()
  
  from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
  from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
  
  main()
  
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF