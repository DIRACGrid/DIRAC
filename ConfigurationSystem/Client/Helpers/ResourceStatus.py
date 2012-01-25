################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

import datetime

from DIRAC                                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.CSAPI                 import CSAPI
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

csAPI = CSAPI()
rsc   = ResourceStatusClient()

################################################################################
# StorageElement
# o get
# o set
################################################################################

def getStorageElementStatus( elementName, statusType = None, default = None ):
  '''
  Helper with dual access, tries to get information from the RSS for the given
  StorageElement, otherwise, it gets it from the CS. 
  
  example:
    >>> getStorageElementStatus( 'CERN-USER', 'Read' )
        S_OK( { 'CERN-USER' : { 'Read': 'Active' } } )
    >>> getStorageElementStatus( 'CERN-USER', 'Write' )
        S_OK( { 'CERN-USER' : {'Read': 'Active', 'Write': 'Active', 'Check': 'Banned', 'Remove': 'Banned'}} )
    >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' ) 
        S_ERROR( xyz.. )
    >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType', 'Unknown' ) 
        S_OK( 'Unknown' ) 
  
  '''
  
  meta        = { 'columns' : [ 'StorageElementName','StatusType','Status' ] }
  kwargs      = { 
                  'elementName' : elementName,
                  'statusType'  : statusType, 
                  'meta'        : meta 
                }
  
  cs_path     = "/Resources/StorageElements"
  
  statuses    = rsc.getValidStatusTypes()
  if statuses[ 'OK' ]:
    statuses = statuses[ 'Value' ][ 'StorageElement' ][ 'StatusType' ]
  else:
    statuses = []  
  
  def getDictFromList( l ):
    
    res = {}
    
    for le in l:
      
      site, sType, status = le
      if not res.has_key( site ):
        res[ site ] = {}
      res[ site ][ sType ] = status
    
    return res  
      
  
  try:
  
    #This returns S_OK( [['StatusType1','Status1'],['StatusType2','Status2']...]
    res = rsc.getElementStatus( 'StorageElement', **kwargs )
    if res[ 'OK' ] and res['Value']:
      return S_OK( getDictFromList( res['Value'] ) )
  
    _msg = "StorageElement '%s', with statusType '%s' not found in RSS"
    gLogger.info( _msg % ( elementName, statusType ) )
  
    
    if statusType is not None:
      res = gConfig.getOption( "%s/%s/%sAccess" % ( cs_path, elementName, statusType ) )
      if res[ 'OK' ] and res[ 'Value' ]:
        return S_OK( { elementName : { statusType : res[ 'Value' ] } }  )
    else:
      res = gConfig.getOptionsDict( "%s/%s" % ( cs_path, elementName ) )
      if res[ 'OK' ] and res[ 'Value' ]:
        r = {}
        for k,v in res['Value'].items():
          k.replace( 'Access', '' )
          if k in statuses:
            r[ k ] = v
              
        return S_OK( { elementName : r } )          
                
    if default is not None:
      return S_OK( { elementName : { statusType: default } } )
  
    _msg = "StorageElement '%s', with statusType '%s' is unknown for RSS and CS"
    return S_ERROR( _msg % ( elementName, statusType ) )
  
  except Exception, e:
    
    _msg = "Error StorageElement '%s', with statusType '%s' is unknown for RSS and CS"
    gLogger.error( _msg % ( elementName, statusType ) )
    gLogger.exception( e )
    return S_ERROR( _msg % ( elementName, statusType ) )
  
def setStorageElementStatus( elementName, statusType, status, reason = None,
                             tokenOwner = None ):
  '''
  Helper with dual access, tries set information in RSS and in CS. 
  
  example:
    >>> getStorageElementStatus( 'CERN-USER', 'Read' )
        S_OK( { 'Read': 'Active' } )
    >>> getStorageElementStatus( 'CERN-USER', 'Write' )
        S_OK( {'Read': 'Active', 'Write': 'Active', 'Check': 'Banned', 'Remove': 'Banned'} )
    >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType' ) 
        S_ERROR( xyz.. )
    >>> getStorageElementStatus( 'CERN-USER', 'ThisIsAWrongStatusType', 'Unknown' ) 
        S_OK( 'Unknown' ) 
  '''
  
  cs_path = "/Resources/StorageElements/%s"
  
  # We set by default the tokenOwner duration to 1 day
  expiration = datetime.datetime.utcnow() + datetime.timedelta( days = 1 )
  
  kwargs = {
            'status'          : status, 
            'reason'          : reason,
            'tokenOwner'      : tokenOwner, 
            'tokenExpiration' : expiration 
            }
  
  try:
    
    _msg = 'Error updating StorageElement (%s,%s,%s)' % ( elementName, statusType, str( kwargs ))
    
    res = rsc.modifyElementStatus( 'StorageElement', elementName, statusType, **kwargs )
    if not res[ 'OK' ]:
      gLogger.warn( 'RSS: %s' % _msg )
      return res
      
    res = csAPI.setOption( "%s/%s/%sAccess" % ( cs_path, elementName, statusType ), status )  
    if not res[ 'OK' ]:
      gLogger.warn( 'CS: %s' % _msg )
      return res
    
    res = csAPI.commitChanges()
    return res

  except Exception, e:

    _msg = "Error setting StorageElement '%s' status '%s', with statusType '%s'"
    gLogger.error( _msg % ( elementName, status, statusType ) )
    gLogger.exception( e )
    return S_ERROR( _msg % ( elementName, status, statusType ) )



################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF