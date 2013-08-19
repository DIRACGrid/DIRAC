# $HeadURL:  $
""" Statistics

  Module containing little helpers that extract information from the RSS databases
  providing information for comparisons and plots.

"""

import datetime

# DIRAC
from DIRAC                                                      import S_ERROR, S_OK
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem.StateMachine       import RSSMachine 
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration      import getValidElements

__RCSID__ = '$Id: $'

class Statistics( object ):
  """
  Statistics class that provides helpers to extract information from the database
  more easily.
  """
  
  def __init__( self ):
    """
    Constructor
    """

    self.rsClient = ResourceStatusClient()    
    #self.rmClient = ResourceManagementClient()
  
  def getElementHistory( self, element, elementName, statusType, 
                         oldAs = None, newAs = None ):
    """
    Returns the succession of statuses and the dates since they are effective. The
    values are comprised in the time interval [ oldAs, newAs ]. If not specified,
    all values up to the present are returned.
    
    It returns a list of tuples, of which the first element is the Status and the
    second one the time-stamp since it is effective. Note that the time-stamps will
    not necessarily match the time window.
        
    :Parameters:
      **element** - `str`
        element family ( either Site, Resource or Node )
      **elementName** - `str`
        element name
      **statusType** - `str`
        status type of the element <elementName> (e.g. 'all', 'ReadAccess',... )
      **oldAs** - [ None, `datetime` ]
        datetime with the start point for the time window. If not specified, it
        is used the oldest time in the history.
      **newAs** - [ None, `datetime` ]
        datetime with the end point for the time window. If not specified, it
        is used datetime.utcnow.
    
    :return: S_OK( [ (StatusA, datetimeA),(StatusB,datetimeB) ] ) | S_ERROR 
    """
    
    # Checks we are not passing a silly element ( we only accept Site, Resource and Node )    
    if not element in getValidElements():
      return S_ERROR( '"%s" is not a valid element' % element ) 

    # FIXME: read below
    # Gets all elements in history. If the history is long, this query is going to
    # be rather heavy...
    result = self.rsClient.selectStatusElement( element, 'History', name = elementName,
                                                statusType = statusType,
                                                meta = { 'columns' : [ 'Status', 'DateEffective' ] } )
    if not result[ 'OK' ]:
      return result
    result = result[ 'Value' ]
    
    if not result:
      return S_OK( [] )
    
    # To avoid making exceptions in the for-loop, we feed history with the first
    # item in the results      
    history = [ result[ 0 ] ]
    
    # Sets defaults.
    # OldAs is as old as datetime.min if not defined.
    
    #oldAs = ( 1 and oldAs ) or history[ 0 ][ 1 ]
    oldAs = ( 1 and oldAs ) or datetime.datetime.min
    
    # NewAs is as new as as set or datetime.now 
    newAs = ( 1 and newAs ) or datetime.datetime.utcnow()    
    
    # Sanity check: no funny time windows
    if oldAs > newAs:
      return S_ERROR( "oldAs (%s) > newAs (%s)" % ( oldAs, newAs ) )
    
    # This avoids that the window finishes before having the first point in the
    # history.   
    if history[ 0 ][ 1 ] > newAs:
      return S_OK( [] )
    
    # Iterate starting from the second element in the list. The elements in the
    # list are SORTED. Otherwise, the break statement would be a mess. And same
    # applies for the elif
    for historyElement in result[1:]:
      
      # If the point is newer than the superior limit of the window, we are done.  
      if historyElement[ 1 ] > newAs:
        break            
      # If the point is older than the window lower limit, we buffer it. We just
      # want the closest point to the lower limit. 
      elif historyElement[ 1 ] <= oldAs:
        history = [ historyElement ]
      # Otherwise, we add it to the history  
      else:
        history.append( historyElement )
      
    return S_OK( history )

  def getElementStatusAt( self, element, elementName, statusType, statusTime ):
    """
    Returns the status of the <element><elementName><statusType> at the given
    time <statusTime>. If not know, will return an empty list. If known, will 
    return a tuple with two elements: Status and time since it is effective.
    
    :Parameters:
      **element** - `str`
        element family ( either Site, Resource or Node )
      **elementName** - `str`
        element name
      **statusType** - `str`
        status type of the element <elementName> (e.g. 'all', 'ReadAccess',... )
      **statusTime** - `datetime`
        datetime when we want to know the status of <element><elementName><statusType>
    
    :return: S_OK( (StatusA, datetimeA) ) | S_ERROR     
    """
    
    result = self.getElementHistory( element, elementName, statusType, statusTime, statusTime )
    if not result[ 'OK' ]:
      return result
    result = result[ 'Value' ]
    
    if result:
      result = list( result[ 0 ] )
      
    return S_OK( result )   
  
  def getElementStatusTotalTimes( self, element, elementName, statusType, 
                                  oldAs = None, newAs = None ):
    """
    Returns a dictionary with all the possible statuses as keys and as values the
    number of seconds that <element><elementName><statusType> hold it for a time
    window between [ oldAs, newAs ]. If oldAs is not defined, it is considered 
    as datetime.min. If newAs is not defined, it is considered datetime.utcnow.

    :Parameters:
      **element** - `str`
        element family ( either Site, Resource or Node )
      **elementName** - `str`
        element name
      **statusType** - `str`
        status type of the element <elementName> (e.g. 'all', 'ReadAccess',... )
      **oldAs** - [ None, `datetime` ]
        datetime with the start point for the time window. If not specified, it
        is used the oldest time in the history.
      **newAs** - [ None, `datetime` ]
        datetime with the end point for the time window. If not specified, it
        is used datetime.utcnow.
    
    :return: S_OK( [ { StatusA : secondsA },{ StatusB : secondsB } ] ) | S_ERROR     
    """
    
    # Gets all history withing the window
    result = self.getElementHistory( element, elementName, statusType, oldAs, newAs )
    if not result[ 'OK' ]:
      return result
    result = result[ 'Value' ]
    
    statuses = RSSMachine( None ).getStates()
    
    # Dictionary to be returned
    statusCounter = dict.fromkeys( statuses, 0 )
    
    # If history is empty, return empty dictionary
    if not result:
      return S_OK( statusCounter )
    
    # Set defaults
    oldAs = ( 1 and oldAs ) or datetime.datetime.min
    newAs = ( 1 and newAs ) or datetime.datetime.utcnow()
    
    # If users are not behaving well, we force newAs to not be in the future. 
    newAs = min( newAs, datetime.datetime.utcnow() )
    
    # Iterate over the results in tuples.
    for statusTuple in zip( result, result[ 1: ] ):
    
      # Make sure the time taken as base is not older than the lower limit of
      # the window. In principle, this should be only checked on the first element,
      # but it is harmless anyway and cleaner than the if-else.
      startingPoint = max( statusTuple[ 0 ][ 1 ], oldAs )  
      
      # Get number of seconds and add them           
      statusCounter[ statusTuple[0][0] ] += timedelta_to_seconds( statusTuple[1][1] - startingPoint ) 
    
    # The method selected to iterate over the results does not take into account the
    # last one. Gets the time using as lower limit the window lower limit. This applies
    # when we have only one element in the list for example.
    statusCounter[ result[ -1 ][ 0 ] ] += timedelta_to_seconds( newAs - max( result[ -1 ][ 1 ], oldAs ) ) 
           
    return S_OK( statusCounter )

def timedelta_to_seconds( duration ):
  """
  As Python does not provide a function to transform a timedelta into seconds,
  here we go.
  
  :Parameters:
    **duration** - `datetime.timedelta`
      timedelta to be transformed into seconds 
     
  :return: int ( seconds )  
  """
  
  days, seconds = duration.days, duration.seconds
  
  # We use integer division, not float division !  
  hours   = seconds // 3600
  minutes = ( seconds % 3600 ) // 60
  seconds = ( seconds % 60 )
      
  return ((( days * 24 ) + hours ) * 60 + minutes ) * 60 + seconds
    
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF