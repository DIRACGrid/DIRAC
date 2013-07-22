# $HeadURL:  $
''' RssConfiguration

  Module that collects utility functions.

'''

from DIRAC                                                import S_OK
from DIRAC.Core.Utilities                                 import List
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations
from DIRAC.ResourceStatusSystem.PolicySystem.StateMachine import RSSMachine
from DIRAC.ResourceStatusSystem.Utilities                 import Utils

__RCSID__ = '$Id:  $'

## RssConfiguration config path ################################################

_rssConfigPath = 'ResourceStatus'

## RssConfiguration ############################################################

class RssConfiguration:
  '''
  
  RssConfiguration:
  { 
    Config:
    { 
      State        : Active | InActive,
      Cache        : 300,
      FromAddress  : 'email@site.domain'
      StatusType   :
      { 
        default       : all,
        StorageElement: ReadAccess, WriteAccess, CheckAccess, RemoveAccess
      }  
    }
  }          
  
  '''
  def __init__( self ):
    self.opsHelper = Operations()

  def getConfigCache( self, default = 300 ):
    '''
      Gets from <pathToRSSConfiguration>/Config the value of Cache
    '''
    
    return self.opsHelper.getValue( '%s/Config/Cache' % _rssConfigPath, default )

  def getConfigFromAddress( self, default = None ):
    '''
      Gets from <pathToRSSConfiguration>/Config the value of FromAddress
    '''
    
    return self.opsHelper.getValue( '%s/Config/FromAddress' % _rssConfigPath, default )
  
  def getConfigStatusType( self, elementType = None ):
    '''
      Gets all the status types per elementType, if not given, it takes default
      from CS. If not, hardcoded variable DEFAULT.
    '''
    
    _DEFAULTS = ( 'all', )
    
    res = self.opsHelper.getOptionsDict( '%s/Config/StatusTypes' % _rssConfigPath )
    
    if res[ 'OK' ]:
          
      if elementType in res[ 'Value' ]:
        return List.fromChar( res[ 'Value' ][ elementType ] )
      
      if 'default' in res[ 'Value' ]:
        return List.fromChar( res[ 'Value' ][ 'default' ] )
        
    return _DEFAULTS

## RssConfiguration/Policies ###################################################

def getPolicies():
  '''
  Returns from the OperationsHelper: <_rssConfigPath>/Policies
  '''
  
  return Utils.getCSTree( '%s/Policies' % _rssConfigPath )

## RssConfiguration/PolicyActions ##############################################

def getPolicyActions():
  '''
  Returns from the OperationsHelper: <_rssConfigPath>/PolicyActions
  '''
  
  return Utils.getCSTree( '%s/PolicyActions' % _rssConfigPath )

## RssConfiguration/Notifications ##############################################

def getNotifications():
  '''
  Returns from the OperationsHelper: <_rssConfigPath>/Notification
  '''
  
  return Utils.getCSTree( '%s/Notification' % _rssConfigPath )
  
## RssConfiguration/GeneralConfig ##############################################

def getValidElements():
  '''
  Returns from the OperationsHelper: <_rssConfigPath>/GeneralConfig/ValidElements
  '''
    
  return ( 'Site', 'Resource', 'Node', 'Component' )  


#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF