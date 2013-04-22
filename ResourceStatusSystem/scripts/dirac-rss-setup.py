#!/usr/bin/env python
"""
  dirac-rss-setup

    This scripts gets the RSS basic setup ready to work. 

    Usage:
      dirac-rss-setup

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE...

"""

from DIRAC                                              import gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base                                    import Script
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB     import ResourceStatusDB
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB

__RCSID__  = '$Id:$'

subLogger = None

def registerSwitches():
  '''
    Registers usage message
  '''
  Script.setUsageMessage( __doc__ )

def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''
  Script.parseCommandLine( ignoreErrors = True )

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

#...............................................................................

def installDB():
  '''
    Installs Tables.
  '''
  
  db1 = ResourceStatusDB()
  db2 = ResourceManagementDB()
  
  res1 = db1._checkTable()
  if not res1[ 'OK' ]:
    subLogger.error( res1[ 'Message' ] )
  elif res1[ 'Value' ]:
    subLogger.info( 'ResourceStatusDB' )
    subLogger.info( res1[ 'Value' ] )
  
  res2 = db2._checkTable()
  if not res2[ 'OK' ]:
    subLogger.error( res2[ 'Message' ] )
  elif res2[ 'Value' ]:
    subLogger.info( 'ResourceManagementDB' )
    subLogger.info( res2[ 'Value' ] )
    
  return S_OK() 

#...............................................................................

def run():
  '''
    Main method
  '''
  
  subLogger.info( 'Checking DB...' )
  installDB()
  
  subLogger.info( '[done]' )  
  
#...............................................................................

if __name__ == "__main__":

  subLogger  = gLogger.getSubLogger( __file__ )

  # Script initialization
  registerSwitches()
  registerUsageMessage()
  parseSwitches()
    
  # Run the script
  run()
    
  # Bye
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF