# $HeadURL:  $
''' LogStatusAction

'''

from DIRAC                                                      import S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

__RCSID__ = '$Id:  $'

class LogStatusAction( BaseAction ):
  '''
    Action that registers on the database a new entry on the <element>Status table.
    It adds or modifies if the record exists on the table.
  '''

  def __init__( self, name, decissionParams, enforcementResult, singlePolicyResults, 
                clients = None ):
    
    super( LogStatusAction, self ).__init__( name, decissionParams, enforcementResult, 
                                             singlePolicyResults, clients )
    
    if clients is not None and 'ResourceStatusClient' in clients:
      self.rsClient = clients[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient()

  def run( self ):
    '''
      Checks it has the parameters it needs and tries to addOrModify in the 
      database.
    '''
    # Minor security checks
    
    element = self.decissionParams[ 'element' ]
    if element is None:
      return S_ERROR( 'element should not be None' )
    
    name = self.decissionParams[ 'name' ] 
    if name is None:
      return S_ERROR( 'name should not be None' )
    
    statusType = self.decissionParams[ 'statusType' ]
    if statusType is None:
      return S_ERROR( 'statusType should not be None' )
    
    status = self.enforcementResult[ 'Status' ]    
    if status is None:
      return S_ERROR( 'status should not be None' )
    
    elementType = self.decissionParams[ 'elementType' ]
    if elementType is None:
      return S_ERROR( 'elementType should not be None' )
    
    reason = self.enforcementResult[ 'Reason' ]
    if reason is None:
      return S_ERROR( 'reason should not be None' )
    
    #Truncate reason to fit in database column
    reason = ( reason[ :508 ] + '..') if len( reason ) > 508 else reason
    
    resLogUpdate = self.rsClient.addOrModifyStatusElement( element, 'Status',
                                                           name = name, statusType = statusType,
                                                           status = status, elementType = elementType,
                                                           reason = reason     
                                                           )
    
    return resLogUpdate   

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF