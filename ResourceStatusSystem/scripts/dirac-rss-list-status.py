#!/usr/bin/env python
"""
  dirac-rss-list-status
  
    Script that dumps the DB information for the elements into the standard output.
    If returns information concerning the StatusType and Status attributes.  
    
    Usage:
      dirac-rss-list-status
        --element=            Element family to be Synchronized ( Site, Resource or Node )
        --elementType=        ElementType narrows the search; None if default
        --elementName=        ElementName; None if default
        --tokenOwner=         Owner of the token; None if default
        --statusType=         StatusType; None if default
        --status=             Status; None if default  
        
        
    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..        
"""

from DIRAC                                     import gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base                           import Script

__RCSID__  = '$Id:$'

subLogger  = None
switchDict = {}

def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''
  
  switches = (
    ( 'element=',     'Element family to be Synchronized ( Site, Resource or Node )' ),
    ( 'elementType=', 'ElementType narrows the search; None if default' ),
    ( 'name=',        'ElementName; None if default' ),
    ( 'tokenOwner=',  'Owner of the token; None if default' ),
    ( 'statusType=',  'StatusType; None if default' ),
    ( 'status=',      'Status; None if default' ),
             )
  
  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )

def registerUsageMessage():
  '''
    Takes the script __doc__ and adds the DIRAC version to it
  '''

  hLine = '  ' + '='*78 + '\n'
  
  usageMessage = hLine
  usageMessage += '  DIRAC %s\n' % version
  usageMessage += __doc__
  usageMessage += '\n' + hLine
  
  Script.setUsageMessage( usageMessage )
  
def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''
  
  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if args:
    subLogger.error( "Found the following positional args '%s', but we only accept switches" % args )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )
  
  switches = dict( Script.getUnprocessedSwitches() )  
  # Default values
  switches.setdefault( 'elementType',  None )
  switches.setdefault( 'name',         None )
  switches.setdefault( 'tokenOwner',   None )
  switches.setdefault( 'statusType',   None )
  switches.setdefault( 'status',       None )

  if not 'element' in switches:
    subLogger.error( "element Switch missing" )
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

def getElements():
  '''
    Given the switches, gets a list of elements with their respective statustype
    and status attributes.
  '''
  
  rssClient = ResourceStatusClient.ResourceStatusClient()
  
  meta = { 'columns' : [] }
  for key in ( 'Name', 'StatusType', 'Status', 'ElementType', 'TokenOwner' ):
    #Transforms from upper lower case to lower upper case
    if switchDict[ key[0].lower() + key[1:] ] is None:
      meta[ 'columns' ].append( key )
  
  elements = rssClient.selectStatusElement( switchDict[ 'element' ], 'Status', 
                                            name        = switchDict[ 'name' ], 
                                            statusType  = switchDict[ 'statusType' ], 
                                            status      = switchDict[ 'status' ], 
                                            elementType = switchDict[ 'elementType' ], 
                                            tokenOwner  = switchDict[ 'tokenOwner' ],
                                            meta = meta )
  
  return elements

def tabularPrint( elementsList ):
  '''
    Prints the list of elements on a tabular
  '''

  LJUST = 25

  subLogger.notice( 'Selection parameters:' )
  subLogger.notice( '  %s: %s' % ( 'element'.ljust( 15 ), switchDict[ 'element' ] ) )
  titles = []
  for key in ( 'Name', 'StatusType', 'Status', 'ElementType', 'TokenOwner' ):
    
    #Transforms from upper lower case to lower upper case
    key = key[0].lower() + key[1:]
     
    if switchDict[ key ] is None:
      titles.append( key )
    else:
      subLogger.notice( '  %s: %s' % ( key.ljust( 15 ), switchDict[ key ] ) )
  
  subLogger.notice( '\n'+''.join( [ item.ljust( LJUST ) for item in titles ] ) )
  subLogger.notice( '-' * len( titles ) * LJUST )
  
  for element in elementsList:
    
    subLogger.notice( ''.join( [ item.ljust( LJUST ) for item in element ] ) )
    
#...............................................................................

def run():
  '''
    Main function of the script
  '''
  
  elements = getElements()
  if not elements[ 'OK' ]:
    subLogger.error( elements )
    DIRACExit( 1 )
  elements = elements[ 'Value' ]
  
  tabularPrint( elements )
  
#...............................................................................

if __name__ == "__main__":
  
  subLogger  = gLogger.getSubLogger( __file__ )
  
  #Script initialization
  registerSwitches()
  registerUsageMessage()
  switchDict = parseSwitches()
  
  from DIRAC                             import gConfig
  from DIRAC.ResourceStatusSystem.Client import ResourceStatusClient
  
  #Run script
  run()
   
  #Bye
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF