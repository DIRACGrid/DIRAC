""" SummarizeLogsAgent module

  This agents scans all the log tables ( SiteLog, ResouceLog and NodeLog ) on the
  ResourceStatusDB and summarizes them. The results are stored on the History
  tables ( SiteHistory, ResourceHistory and NodeHistory ) and the Log tables
  cleared.
  
  In order to summarize the logs, all entries with no changes on the Status or
  TokenOwner column for a given ( Name, StatusType ) tuple are discarded.

  The agent also adds a little prevention to avoid messing the summaries if the
  agent is restarted / killed abruptly. Please, please, please, DO NOT DO IT !

"""

from DIRAC                                                  import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

__RCSID__  = '$Id:  $'
AGENT_NAME = 'ResourceStatus/SummarizeLogsAgent'

class SummarizeLogsAgent( AgentModule ):
  """ SummarizeLogsAgent as extension of AgentModule.
  
  """

  def __init__( self, *args, **kwargs ):
    """ Constructor.
    
    """

    AgentModule.__init__( self, *args, **kwargs )
    
    self.rsClient = None


  def initialize( self ):
    """ Standard initialize.
    
    :return: S_OK
    
    """

    self.rsClient = ResourceStatusClient()
    return S_OK()


  def execute( self ):
    """ execute ( main method )
    
    The execute method runs over the three families of tables ( Site, Resource and
    Node ) performing identical operations. First, selects all logs for a given
    family ( and keeps track of which one is the last row ID ). It summarizes the
    logs and finally, deletes the logs from the database.
    
    :return: S_OK
    
    """

    # loop over the tables
    for element in ( 'Site', 'Resource', 'Node' ):

      self.log.info( 'Summarizing %s' % element )

      # get all logs to be summarized
      selectLogElements = self._summarizeLogs( element )
      if not selectLogElements[ 'OK' ]:
        self.log.error( selectLogElements[ 'Message' ] )
        continue
      
      lastID, logElements = selectLogElements[ 'Value' ]
      
      # logElements is a dictionary of key-value pairs as follows:
      # ( name, statusType ) : list( logs )
      for key, logs in logElements.iteritems():

        sumResult = self._registerLogs( element, key, logs )
        if not sumResult[ 'OK' ]:
          self.log.error( sumResult[ 'Message' ] )
          continue

      if lastID is not None:
        self.log.info( 'Deleting %sLog till ID %s' % ( element, lastID ) )
        deleteResult = self.rsClient.deleteStatusElement( element, 'Log', 
                                                        meta = { 'older' : ( 'ID', lastID ) } )
        if not deleteResult[ 'OK' ]:
          self.log.error( deleteResult[ 'Message' ] )
          continue

    return S_OK()


  #.............................................................................


  def _summarizeLogs( self, element ):
    """ given an element, selects all logs in table <element>Log.
    
    :Parameters:
      **element** - `string`
        name of the table family ( either Site, Resource and Node )
    
    :return: S_OK( lastID, listOfLogs ) / S_ERROR
    
    """
    
    selectResults = self.rsClient.selectStatusElement( element, 'Log' )
    
    if not selectResults[ 'OK' ]:
      return selectResults
  
    selectedItems = {}
    selectColumns = selectResults[ 'Columns' ]
    selectResults = selectResults[ 'Value' ]
    
    latestID = None
    if selectResults:
      latestID = dict( zip( selectColumns, selectResults[ -1 ] ) )[ 'ID' ]
    
    for selectResult in selectResults:
      
      elementDict = dict( zip( selectColumns, selectResult ) )
      
      key = ( elementDict[ 'Name' ], elementDict[ 'StatusType' ] )

      if not key in selectedItems:
        selectedItems[ key ] = [ elementDict ]     
      else:
        lastStatus = selectedItems[ key ][ -1 ][ 'Status' ]
        lastToken  = selectedItems[ key ][ -1 ][ 'TokenOwner' ]
        
        # If there are no changes on the Status or the TokenOwner with respect
        # the previous one, discards the log.
        if lastStatus != elementDict[ 'Status' ] or lastToken != elementDict[ 'TokenOwner' ]:
          selectedItems[ key ].append( elementDict )

    return S_OK( ( latestID, selectedItems ) )
      
  
  def _registerLogs( self, element, key, logs ):
    """ Given an element, a key - which is a tuple ( <name>, <statusType> )
    and a list of dictionaries, this method inserts them on the <element>History
    table. Before inserting them, checks whether the first one is or is not on 
    the <element>History table. If it is, it is not inserted. It also checks 
    whether the LastCheckTime parameter of the first log to be inserted is 
    larger than the last history log LastCheckTime. If not, it means an agent
    cycle has been interrupted and we can run into inconsistencies. It aborts to
    prevent more dramatic results.
    
    :Parameters:
      **element** - `string`
        name of the table family ( either Site, Resource and Node )
      **key** - `tuple`
        tuple with the name of the element and the statusType  
      **logs** - `list`
        list of dictionaries containing the logs
        
     :return: S_OK / S_ERROR   
    
    """

    # Undo key
    name, statusType = key

    selectedRes = self.rsClient.selectStatusElement( element, 'History', name,
                                                     statusType,
                                                     meta = { 'columns' : [ 'Status', 'LastCheckTime', 'TokenOwner' ],
                                                              'limit'   : 1,
                                                              'order'   : 'LastCheckTime DESC' } )

    if not selectedRes[ 'OK' ]:
      return selectedRes
    selectedRes = selectedRes[ 'Value' ]

    # We want from the <element>History table the last Status, LastCheckTime
    # and TokenOwner
    lastStatus, lastCheckTime, lastToken = None, None, None
    if selectedRes:
      lastStatus, lastCheckTime, lastToken = selectedRes[ 0 ]

    # Sanity check to avoid running if an agent cycle has been stopped
    if lastCheckTime and logs[ 0 ][ 'LastCheckTime' ] < lastCheckTime:
      return S_ERROR( 'Overlapping data. Seems the DB has not been cleared properly' )

    # If the first of the selected items has a different status than the latest
    # on the history, we add it.
    if logs[ 0 ][ 'Status' ] == lastStatus and logs[ 0 ][ 'TokenOwner' ] == lastToken:
      logs.remove( logs[ 0 ] )

    if logs:
      self.log.info( '%s ( %s )' % ( name, statusType ) )

    for selectedItemDict in logs:

      res = self.__logToHistoryTable( element, selectedItemDict )
      if not res[ 'OK' ]:
        return res   

    return S_OK()
    

  def __logToHistoryTable( self, element, elementDict ):
    """ Given an element and a dictionary with all the arguments, this method
    inserts a new entry on the <element>History table
    
    :Parameters:
      **element** - `string`
        name of the table family ( either Site, Resource and Node )
      **elementDict** - `dict`
        dictionary returned from the DB to be inserted on the History table
    
    :return: S_OK / S_ERROR 
                
    """

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

    self.log.info( '  %(Status)s %(DateEffective)s %(TokenOwner)s %(Reason)s' % elementDict )

    return self.rsClient.insertStatusElement( element, 'History', name, statusType,
                                              status, elementType, reason,
                                              dateEffective, lastCheckTime,
                                              tokenOwner, tokenExpiration )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF