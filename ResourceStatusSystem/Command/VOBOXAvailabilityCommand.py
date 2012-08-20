# $HeadURL: $
''' VOBOXAvailabilityCommand
  
  The Command pings a service on a vobox.
  
'''

import urlparse

from DIRAC                                      import S_OK
from DIRAC.Core.DISET.RPCClient                 import RPCClient
from DIRAC.ResourceStatusSystem.Command.Command import Command

__RCSID__ = '$Id: $'

class VOBOXAvailabilityCommand( Command ):
  '''
  Given an url pointing to a service on a vobox, use DIRAC ping against it.
  ''' 
  
  __APIs__ = [ 'ResourceManagementClient' ]
  
  def doCommand( self ):
    '''
    Run the command.
    '''
    super( VOBOXAvailabilityCommand, self ).doCommand()
    
    serviceURL   = self.args[ 0 ]
    
    pinger  = RPCClient( serviceURL )
    resPing = pinger.ping()
    
    if resPing[ 'OK' ]: 
      
      serviceUpTime = resPing[ 'Value' ].get( 'service uptime', 0 )
      machineUpTime = resPing[ 'Value' ].get( 'host uptime', 0 )
      
      parsed          = urlparse.urlparse( serviceURL )
      system, service = parsed.path.strip("/").split("/")
      site            = parsed.netloc.split(":")[0]
      
      res = S_OK( 
                  { 
                    'serviceUpTime' : serviceUpTime,
                    'machineUpTime' : machineUpTime,
                    'site'          : site,
                    'system'        : system,
                    'service'       : service
                   }
                 )
    else:
      
      res = resPing
      
    return { 'Result' : res }       
    
################################################################################      
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  