# $HeadURL:  $
''' SummarizeLogsAgent module
'''

from datetime                                               import datetime, timedelta

from DIRAC                                                  import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

__RCSID__  = '$Id:  $'
AGENT_NAME = 'ResourceStatus/SummarizeLogsAgent'

class SummarizeLogsAgent( AgentModule ):
  
  # Date format in database
  __dateFormat = '%Y-%m-%d %H:%M:%S'
    
  def __init__( self, agentName, baseAgentName = False, properties = dict() ):
    
    AgentModule.__init__( self, agentName, baseAgentName, properties ) 
    
    self.rsClient = None  
  
  def initialize( self ):
    
    self.rsClient = ResourceStatusClient()
    
    return S_OK()

  def execute( self ):
    
    elements = ( 'Site', 'Resource', 'Node' )
        
    for element in elements:
    
      self.log.info( 'Summarizing %s' % element )
      
      selectLogElements = self._selectLogElements( element )
      if not selectLogElements[ 'OK' ]:
        self.log.error( selectLogElements[ 'Message' ] )
        continue
      selectLogElements = selectLogElements[ 'Value' ]
            
      for selectedKey, selectedItem in selectLogElements.items():
      
        sRes = self._logSelectedLogElement( element, selectedKey, selectedItem )
        if not sRes[ 'OK' ]:
          self.log.error( sRes[ 'Message' ] ) 
          

        
        
    return S_OK()
  
  def _selectLogElements( self, element ):
    '''
      For a given element, selects all the entries on the <element>Log table
      with LastCheckTime > <lastHour>. It groups them by tuples of
      ( <name>, <statusType> ) and keeps only the statuses that represent
      a change in the status.
    '''
    
    # We do not want neither minutes, nor seconds nor microseconds
    thisHour = datetime.utcnow().replace( microsecond = 0 )
    thisHour = thisHour.replace( second = 0 ).replace( minutes = 0 )
    
    lastHour = thisHour - timedelta( hours = 1 )  
      
    selectResults = self.rsClient.selectStatusElement( element, 'Log', 
                                                       meta = { 'newer' : ( 'LastCheckTime', lastHour ) } )
    if not selectResults[ 'OK' ]:
      return selectResults
    selectResults = selectResults[ 'Value' ]

    selectedItems = {}       
    selectColumns = selectResults[ 'Columns' ]
      
    for selectResult in selectResults:
      
      elementDict = dict( zip( selectColumns, selectResult ) )
      
      if elementDict[ 'LastCheckTime' ] > str( thisHour ):
        continue
    
      key = ( elementDict[ 'Name' ], elementDict[ 'StatusType' ] )
    
      if not key in selectedItems:
        selectedItems[ key ] = elementDict
      else:
        lastStatus = selectedItems[ key ][ -1 ][ 'Status' ]
        if lastStatus != elementDict[ 'Status' ]:
          selectedItems[ key ] = elementDict
          
    return S_OK( selectedItems )        
  
  def _logSelectedLogElement( self, element, selectedKey, selectedItem ):
    '''
      Given an element, a selectedKey - which is a tuple ( <name>, <statusType> )
      and a list of dictionaries, this method inserts them. Before inserting
      them, checks whether the first one is or is not on the <element>History
      table. If it is, it is not inserted.
    '''
        
    name, statusType = selectedKey
        
    selectedRes = self.rsClient.selectStatusElement( element, 'History', name, 
                                                     statusType, 
                                                     meta = { 'columns' : 'Status' } )
    
    if not selectedRes[ 'OK ' ]:
      return selectedRes
    selectedRes = selectedRes[ 'Value' ]
        
    selectedStatus = None
    if selectedRes:
      # Get the last selectedRes, which will be the newest one. Each selectedRes
      # is a tuple, in this case, containing only one element - Status.
      selectedStatus = selectedRes[ -1 ][ 0 ]
        
    # If the first of the selected items has a different status than the latest
    # on the history, we add it.    
    if selectedItem[ 0 ][ 'Status' ] != selectedStatus:
            
      res = self._logToHistoryTable( element, selectedItem[ 0 ] )
      if not res[ 'OK' ]:
        return res
                
    for selectedItemDict in selectedItem[ 1: ]:
          
      res = self._logToHistoryTable( element, selectedItemDict )
      if not res[ 'OK' ]:
        return res
        
    return S_OK()
  
  def _logToHistoryTable( self, element, elementDict ):
    '''
      Given an element and a dictionary with all the arguments, this method
      inserts a new entry on the <element>History table
    '''
  
    try:
      
      name            = elementDict[ 'Name' ]
      statusType      = elementDict[ 'StatusType' ]
      status          = elementDict[ 'Status' ]
      elementType     = elementDict[ 'ElementType' ]
      reason          = elementDict[ 'Reason' ]
      dateEffective   = elementDict[ 'DateEffective' ]
      lastCheckTime   = elementDict[ 'LastCheckTime' ]
      tokenOwner      = elementDict[ 'TokenOwner' ]
      tokenExpiration = elementDict[ 'TokenExpiration' ]
      
    except KeyError, e:
      return S_ERROR( e )
    
    return self.rsClient.insertStatusElement( element, 'History', name, statusType, 
                                              status, elementType, reason, 
                                              dateEffective, lastCheckTime, 
                                              tokenOwner, tokenExpiration )  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  