################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC                                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

rsc = ResourceStatusClient()

def getStorageElementStatus( elementName, statusType = None, default = None ):
  '''
  Helper with dual access, tries to get information from the RSS for the given
  StorageElement, otherwise, it gets it from the CS. 
  
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
  
  meta        = { 'columns' : [ 'StatusType','Status' ] }
  kwargs      = { 
                  'elementName' : elementName,
                  'statusType'  : statusType, 
                  'meta'        : meta 
                }
  
  cs_path     = "/Resources/StorageElements/%s/%sAccess"
  
  try:
  
    #This returns S_OK( [['StatusType1','Status1'],['StatusType2','Status2']...]
    res = rsc.getElementStatus( 'StorageElement', *kwargs )
    if res[ 'OK' ] and res['Value']:
      return S_OK( dict( res['Value'] ) )
  
    _msg = "StorageElement '%s', with statusType '%s' not found in RSS"
    gLogger.info( _msg % ( elementName, statusType ) )
  
    res = gConfig.getOption( cs_path % ( elementName, statusType ) )
    if res[ 'OK' ] and res[ 'Value' ]:
      return S_OK( { statusType : res[ 'Value' ] } )
  
    elif default is not None:
      return S_OK( { statusType: default } )
  
    _msg = "StorageElement '%s', with statusType '%s' is unknown for RSS and CS"
    return S_ERROR( _msg % ( elementName, statusType ) )
  
  except Exception, e:
    
    _msg = "Error StorageElement '%s', with statusType '%s' is unknown for RSS and CS"
    gLogger.error( _msg % ( elementName, statusType ) )
    gLogger.exception( e )
    return S_ERROR( _msg % ( elementName, statusType ) )
  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF