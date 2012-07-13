# $HeadURL $
''' RssConfiguration

  Module that collects utility functions.

'''

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ResourceStatusSystem.Utilities                import Utils

## RssConfiguration/Logs #######################################################

def getRecordLogs():
  '''
  Returns from the OperationsHelper: RSSConfiguration/Logs/Record
  '''
  
  result = Operations().getValue( 'RSSConfiguration/Logs/Record' )
  if result == 'Active':
    return True
  return False

## RssConfiguration/InspectionFreqs ############################################

def getInspectionFreqs():
  '''
  Returns from the OperationsHelper: RSSConfiguration/InspectionFreqs
  '''
  
  #result = Operations().getValue( 'RSSConfiguration/Logs/Record' )
  #if result == 'Active':
  #  return True
  return { 'Site' : { '' : { 'Active' : 2, 'Bad' : 2, 'Probing' : 2, 'Banned' : 2 } } }

## RssConfiguration/Policies ###################################################

def getPolicies():
  '''
  Returns from the OperationsHelper: RSSConfiguration/Policies
  '''
  
  return Utils.getCSTree( 'RSSConfiguration/Policies' )

## RssConfiguration/PolicyActions ##############################################

def getPolicyActions():
  '''
  Returns from the OperationsHelper: RSSConfiguration/PolicyActions
  '''
  
  return Utils.getCSTree( 'RSSConfiguration/PolicyActions' )
  
## RssConfiguration/GeneralConfig ##############################################

def getValidElements():
  '''
  Returns from the OperationsHelper: RSSConfiguration/GeneralConfig/ValidElements
  '''
  #FIXME: no defaults. If it fails, I want to know it.
  _DEFAULTS = ( 'Site', 'Resource', 'Node' )
  
  result = Operations().getValue( 'RSSConfiguration/GeneralConfig/ValidElements' )
  if result is not None:
    return Utils.getTypedList( result )
  return _DEFAULTS  

#def getValidStatus():
#  '''
#  Returns from the OperationsHelper: RSSConfiguration/GeneralConfig/Status
#  '''
#  
#  DEFAULTS = [ 'Active', 'Bad', 'Probing', 'Banned' ]
#  
#  result = Operations().getValue( 'RSSConfiguration/GeneralConfig/Status' )
#  if result is not None:
#    return Utils.getTypedList( result )
#  return DEFAULTS
#
#
def getValidStatusTypes():
  '''
  Returns from the OperationsHelper: RSSConfiguration/GeneralConfig/Resources
  '''
  #FIXME: no defaults. If it fails, I want to know it.
  DEFAULTS = { 
               'Site'          : ( '' ),
               'Resource'      : ( '' ),
               'Node'          : ( '' )
#               'StorageElement': [ 'ReadAccess', 'WriteAccess', 
#                                   'RemoveAccess', 'CheckAccess' ]
              }
  
  opHelper = Operations()
  
  sections = opHelper.getSections( 'RSSConfiguration/GeneralConfig/Resources' )
  if not sections[ 'OK' ]:
    return DEFAULTS
  
  result = {}
  for section in sections[ 'Value' ]:
    res = opHelper.getValue( 'RSSConfiguration/GeneralConfig/Resources/%s/StatusType' % section )
    if res is None:
      if DEFAULTS.has_key( section ):
        result[ section ] = DEFAULTS[ section ]
      else:
        result[ section ] = []  
    else:
      result[ section ] = Utils.getTypedList( res )
      
  return result     


  
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