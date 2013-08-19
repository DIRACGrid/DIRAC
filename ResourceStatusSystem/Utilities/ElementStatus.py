# $HeadURL$
""" ElementStatus base class for the helpers

  Each RSS Status helper extends this class, providing four methods per cache.
  * get<elementType>Statuses
  * get<elementType>Status
  * isUsable<elementType>
  * getUsable<elementType>

"""

# DIRAC
from DIRAC                                                  import gLogger, S_ERROR, S_OK
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

__RCSID__ = '$Id: $'

class ElementStatus( object ):
  """
  ElementStatus class used by SiteStatus, ResourceStatus and NodeStatus helpers
  in the RSS clients. This base class will query the get<elementType>Statuses
  method from the child classes, and build its responses with its output.
  """
  
  def __init__( self ):
    """
    Constructor. Initializes logger and ResourceStatusClient.
    """
    
    self.log       = gLogger.getSubLogger( self.__class__.__name__ )
    self.rssClient = ResourceStatusClient()  

  def getElementStatuses( self, elementType, elementNames, statusTypes ):
    """
    Method that gets from the extended class the get<elementType>Statuses method
    and runs it. This method always requires two parameters, elementNames and 
    statusTypes. Returns its output.
    
    :Parameters:
      **elementType** - `string`
        name of the elementType of the cache ( Site, ComputingElement,... ) used 
        to query get<elementType>Statuses
      **elementNames** - [ None, `string`, `list` ]
        name(s) of the elements to be matched
      **statusTypes** - [ None, `string`, `list` ]
        name(s) of the statusTypes to be matched
    
    :return: S_OK() || S_ERROR()       
    """
    
    try:
      result = getattr( self, 'get%sStatuses' % elementType )( elementNames, statusTypes )
    except AttributeError:
      return S_ERROR( "Error calling get%sStatuses" % elementType )

    return result    
  
  def getElementStatus( self, elementType, elementName, statusType ):
    """
    Given a elementName and a statusType, it returns its status from the cache
    corresponding to elementType.
    
    :Parameters:
      **elementType** - `string`
        name of the elementType of the cache ( Site, ComputingElement,... ) used 
        to query get<elementType>Statuses
      **elementName** - `string`
        name of the element to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()    
    """
    
    if not isinstance( elementName, str ):
      self.log.error( "getElementStatus expects str for elementName" )
      return S_ERROR( "getElementStatus expects str for elementName" )
    if not isinstance( statusType, str ):
      self.log.error( "getElementStatus expects str for statusType" )
      return S_ERROR( "getElementStatus expects str for statusType" )    
    
    elementStatus = self.getElementStatuses( elementType, elementName, statusType )
    
    if not elementStatus[ 'OK' ]:
      self.log.error( elementStatus[ 'Message' ] )
      return elementStatus
    
    return S_OK( elementStatus[ 'Value' ][ elementName ][ statusType ] )
  
  def isUsableElement( self, elementType, elementName, statusType ):
    """
    Similar method to getElementStatus. The difference is the output.
    Given an element name, returns a bool if the element is usable: 
      status is Active or Degraded outputs True
      anything else outputs False
    
    :Parameters:
      **elementType** - `string`
        name of the elementType of the cache ( Site, ComputingElement,... ) used 
        to query get<elementType>Statuses
      **siteName** - `string`
        name of the site to be matched
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()    
    """
    
    self.log.debug( ( elementName, statusType ) )
    
    elementStatus = self.getElementStatus( elementType, elementName, statusType )
    if not elementStatus[ 'OK' ]:
      self.log.error( elementStatus[ 'Message' ] )
      return False
    
    if elementStatus[ 'Value' ] in ( 'Active', 'Degraded' ):
      self.log.debug( 'IsUsable' )
      return True
    
    self.log.debug( 'Is NOT Usable' )
    return False  
    
  def getUsableElements( self, elementType, statusType ):
    """
    For a given statusType, returns all elements that are usable: their status
    for that particular statusType is either Active or Degraded; in a list.   
    
    :Parameters:
      **elementType** - `string`
        name of the elementType of the cache ( Site, ComputingElement,... ) used 
        to query get<elementType>Statuses    
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    if not isinstance( statusType, str ):
      self.log.error( "getUsableElements expects str for statusType" )
      return S_ERROR( "getUsableElements expects str for statusType" )
    
    elementStatuses = self.getElementStatuses( elementType, None, statusType )
    if not elementStatuses[ 'OK' ]:
      self.log.error( elementStatuses )
      return elementStatuses
    elementStatuses = elementStatuses[ 'Value' ]
    
    self.log.debug( elementStatuses )
    
    usableElements = []
    
    for elementName, statusDict in elementStatuses.items():
        
      if statusDict[ statusType ] in ( 'Active', 'Degraded' ):
        
        usableElements.append( elementName )
    
    return S_OK( usableElements )  

  def getUnusableElements( self, elementType, statusType ):
    """
    For a given statusType, returns all elements that are unusable: their status
    for that particular statusType is either Banned or Probing; in a list.   
    
    :Parameters:
      **elementType** - `string`
        name of the elementType of the cache ( Site, ComputingElement,... ) used 
        to query get<elementType>Statuses    
      **statusType** - `string`
        name of the statusType to be matched
    
    :return: S_OK() || S_ERROR()
    """
    
    if not isinstance( statusType, str ):
      self.log.error( "getUnusableElements expects str for statusType" )
      return S_ERROR( "getUnusableElements expects str for statusType" )
    
    elementStatuses = self.getElementStatuses( elementType, None, statusType )
    if not elementStatuses[ 'OK' ]:
      self.log.error( elementStatuses )
      return elementStatuses
    elementStatuses = elementStatuses[ 'Value' ]
    
    self.log.debug( elementStatuses )
    
    unusableElements = []
    
    for elementName, statusDict in elementStatuses.items():
        
      if statusDict[ statusType ] in ( 'Banned', 'Probing', 'Error', 'Unknown' ):
        
        unusableElements.append( elementName )
    
    return S_OK( unusableElements )  

  #...............................................................................

  @staticmethod
  def getCacheDictFromRawData( rawList ):
    """
    Formats the raw data list, which we know it must have tuples of three elements.
    ( element1, element2, element3 ) into a list of tuples with the format
    ( ( element1, element2 ), element3 ). Then, it is converted to a dictionary,
    which will be the new Cache.
  
    It happens that element1 is elementName, element2 is statusType and element3
    is status.
    
    :Parameters:
      **rawList** - `list`
        list of three element tuples [( element1, element2, element3 ),... ]
    
    :return: dict of the form { ( elementName, statusType ) : status, ... }
    """
      
    res = [ ( ( name, sType ), status ) for name, sType, status in rawList ]
    return dict( res )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF