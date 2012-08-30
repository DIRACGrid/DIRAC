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

_rssConfigPath = 'RSSConfiguration2'

## RssConfiguration ############################################################

class RssConfiguration:
  '''
  
  RssConfiguration:
  { 
    Config:
    { 
      State      : Active | InActive,
      RecordLogs : Active | InActive, 
      StatusType :
      { 
        default       : all,
        StorageElement: ReadAccess, WriteAccess, CheckAccess, RemoveAccess
      }  
    }
  }          
  
  '''
  def __init__( self ):
    self.opsHelper = Operations() 

  def getConfigRecordLogs( self, default = 'Active' ):
    '''
      Gets from <pathToRSSConfiguration>/Config the value of RecordLogs
    '''
    
    return self.opsHelper.getValue( '%s/Config/RecordLogs' % _rssConfigPath, default )

  def getConfigState( self, default = 'InActive' ):
    '''
      Gets from <pathToRSSConfiguration>/Config the value of State
    '''
    
    return self.opsHelper.getValue( '%s/Config/State' % _rssConfigPath, default )
  
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
 
## RssConfiguration/InspectionFreqs ############################################

# NOT USED !
#def getInspectionFreqs():
#  '''
#  Returns from the OperationsHelper: <_rssConfigPath>/InspectionFreqs
#  '''
#  
#  #result = Operations().getValue( 'RSSConfiguration/Logs/Record' )
#  #if result == 'Active':
#  #  return True
#  
#  #XME: Return S_OK
#  return { 'Site' : { '' : { 'Active' : 2, 'Bad' : 2, 'Probing' : 2, 'Banned' : 2 } } }

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
  _DEFAULTS = ( 'Site', 'Resource', 'Node' )
  
#  result = Operations().getValue( '%s/GeneralConfig/ValidElements' % _rssConfigPath )
#  if result is not None:
#    return List.fromChar( result )
  return _DEFAULTS  

def getValidStatus():
  '''
  Returns a list of statuses as were defined on the RSS(State)Machine  
  '''

  validStatus = RSSMachine( None ).getStates()
  return S_OK( validStatus )


#def getValidStatusTypes():
#  '''
#  Returns from the OperationsHelper: <_rssConfigPath>/GeneralConfig/Resources
#  '''
#  #FIXME: no defaults. If it fails, I want to know it.
#  
#  #FIXME: return S_OK
#  
#  DEFAULTS = { 
#               'Site'          : ( '', ),
#               'Resource'      : ( '', ),
#               'Node'          : ( '', )
##               'StorageElement': [ 'ReadAccess', 'WriteAccess', 
##                                   'RemoveAccess', 'CheckAccess' ]
#              }
#  
#  #FIXME: it does not work with empty configuration
##  opHelper = Operations()
##  
##  sections = opHelper.getSections( '%s/GeneralConfig/Resources' % _rssConfigPath )
##  if not sections[ 'OK' ]:
##    return DEFAULTS
##  
##  result = {}
##  for section in sections[ 'Value' ]:
##    res = opHelper.getValue( '%s/GeneralConfig/Resources/%s/StatusType' % ( _rssConfigPath, section ) )
##    if res is None:
##      if DEFAULTS.has_key( section ):
##        result[ section ] = DEFAULTS[ section ]
##      else:
##        result[ section ] = []  
##    else:
##      result[ section ] = Utils.getTypedList( res )
##      
##  return result     
#  return DEFAULTS  

#def getValidPolicyResult():
#  '''
#  Returns from the OperationsHelper: RSSConfiguration/GeneralConfig/PolicyResult
#  '''
#  
#  DEFAULTS = [ 'Error', 'Unknown', 'Banned', 'Probing', 'Bad', 'Active' ]
#  
#  result = Operations().getValue( 'RSSConfiguration/GeneralConfig/PolicyResult' )
#  if result is not None:
#    return Utils.getTypedList( result )
#  return DEFAULTS
#
#
#def getValidSiteTypes():
#  '''
#  Returns from the OperationsHelper: RSSConfiguration/GeneralConfig/SiteType
#  '''
#  
#  DEFAULTS = [ 'T1', 'T2', 'T3', 'T4' ]
#  
#  result = Operations().getValue( 'RSSConfiguration/GeneralConfig/SiteType' )
#  if result is not None:
#    return Utils.getTypedList( result )
#  return DEFAULTS
#
#
#def getValidServiceTypes():
#  '''
#  Returns from the OperationsHelper: RSSConfiguration/GeneralConfig/ServiceType
#  '''
#  
#  DEFAULTS = [ 'Computing', 'Storage', 'VO-BOX', 'VOMS', 'CondDB' ]
#  
#  result = Operations().getValue( 'RSSConfiguration/GeneralConfig/ServiceType' )
#  if result is not None:
#    return Utils.getTypedList( result )
#  return DEFAULTS
#
#
#def getValidResourceTypes():
#  '''
#  Returns from the OperationsHelper: RSSConfiguration/GeneralConfig/ResourceType
#  '''
#  
#  DEFAULTS = [ 'CE', 'CREAMCE', 'SE', 'LFC_C', 'LFC_L', 'FTS', 'VOMS' ]
#  
#  result = Operations().getValue( 'RSSConfiguration/GeneralConfig/ResourceType' )
#  if result is not None:
#    return Utils.getTypedList( result )
#  return DEFAULTS
#
#def getValidPolicyTypes():
#  '''
#  Returns from the OperationsHelper: RSSConfiguration/GeneralConfig/PolicyTypes
#  '''
#  
#  DEFAULTS = [ 'Resource_PolType', 'Alarm_PolType', 'Collective_PolType', 'RealBan_PolType' ]
#  
#  result = Operations().getValue( 'RSSConfiguration/GeneralConfig/PolicyTypes' )
#  if result is not None:
#    return Utils.getTypedList( result )
#  return DEFAULTS
#
#################################################################################
#
#views_panels = {
#  'Site'           : [ 'Site_Panel', 'Service_Computing_Panel', 
#                       'Service_Storage_Panel', 'Service_VOMS_Panel', 
#                       'Service_VO-BOX_Panel' ],
#  'Resource'       : [ 'Resource_Panel' ],
#  'StorageElement' : [ 'SE_Panel' ]
#}

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF