"""
ResourceStatusClient class is a client for requesting info from the ResourceStatusService.
"""
# it crashes epydoc
# __docformat__ = "restructuredtext en"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes, RSSException
from DIRAC.ResourceStatusSystem.Utilities.Utils import where
from DIRAC.ResourceStatusSystem.PolicySystem.Configurations import ValidRes

class ResourceStatusClient:

#############################################################################

  def __init__( self, serviceIn = None, timeout = None ):
    """ Constructor of the ResourceStatusClient class
    """
    if serviceIn == None:
      self.rsS = RPCClient( "ResourceStatus/ResourceStatus", timeout = timeout )
    else:
      self.rsS = serviceIn

#############################################################################

  def getServiceStats( self, granularity, name ):
    """
    Returns simple statistics of active, probing and banned services of a site;

    :Parameters:
      `granularity`
        string, has to be 'Site'

      `name`
        string - a service name

    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    if granularity not in ( 'Site', 'Sites' ):
      raise InvalidRes, where( self, self.getServiceStats )

    res = self.rsS.getServiceStats( name )
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getServiceStats ) + " " + res[ 'Message' ]

    return res[ 'Value' ]

#############################################################################

  def getResourceStats( self, granularity, name ):
    """
    Returns simple statistics of active, probing and banned resources of a site or a service;

    :Parameters:
      `granularity`
        string, should be in ('Site', 'Service')

      `name`
        string, name of site or service

    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    res = self.rsS.getResourceStats( granularity, name )
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getResourceStats ) + " " + res[ 'Message' ]

    return res[ 'Value' ]

#############################################################################

  def getStorageElementsStats(self, granularity, name, access):
    """
    Returns simple statistics of active, probing and banned storageElements of a site or a resource;

    :Parameters:
      `granularity`
        string, should be in ['Site', 'Resource']

      `name`
        string, name of site or resource

      `access`
        string, either Read or Write

    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    res = self.rsS.getStorageElementsStats( granularity, name, access )
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getStorageElementsStats ) + " " + res[ 'Message' ]

    return res[ 'Value' ]

#############################################################################


  def getPeriods( self, granularity, name, status, hours ):
    """
    Returns a list of periods of time where name was in status

    :returns:
      {
        'Periods':[list of periods]
      }
    """
    #da migliorare!

    if granularity not in ValidRes:
      raise InvalidRes, where( self, self.getPeriods )

    res = self.rsS.getPeriods( granularity, name, status, hours )
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getPeriods ) + " " + res[ 'Message' ]

    return { 'Periods' : res[ 'Value' ] }

#############################################################################

  def getGeneralName( self, granularity, name, toGranularity ):
    """
    Returns simple statistics of active, probing and banned storageElements of a site or a resource;

    :Parameters:
      `granularity`
        string, should be a ValidRes

      `name`
        string, name of site or resource

      `toGranularity`
        string, should be a ValidRes

    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    res = self.rsS.getGeneralName( granularity, name, toGranularity )
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getGeneralName ) + " " + res[ 'Message' ]

    return res[ 'Value' ]

#############################################################################

  def getMonitoredStatus( self, granularity, names ):
    """
    Returns RSS status of names (could be a string or a list of strings)

    :Parameters:
      `granularity`
        string, should be a ValidRes

      `names`
        string or dict, name(s) of the ValidRes

    :returns:
      'Active'|'Probing'|'Banned'|None
    """

    if not isinstance( names, list ):
      names = [ names ]

    statusList = []

    for name in names:
      if granularity in ( 'Site', 'Sites' ):
        res = self.rsS.getSitesStatusWeb( { 'SiteName' : name }, [], 0, 1 )
      elif granularity in ( 'Service', 'Services' ):
        res = self.rsS.getServicesStatusWeb( { 'ServiceName' : name }, [], 0, 1 )
      elif granularity in ('Resource', 'Resources'):
        res = self.rsS.getResourcesStatusWeb( { 'ResourceName' : name }, [], 0, 1 )
      elif granularity in ( 'StorageElementRead', 'StorageElementsRead' ):
        res = self.rsS.getStorageElementsStatusWeb( { 'StorageElementName' : name }, [], 0, 1, 'Read' )
      elif granularity in ('StorageElementWrite', 'StorageElementsWrite'):
        res = self.rsS.getStorageElementsStatusWeb( { 'StorageElementName':name }, [], 0, 1, 'Write' )
      else:
        raise InvalidRes, where( self, self.getMonitoredStatus )

      if not res[ 'OK' ]:
        raise RSSException, where( self, self.getMonitoredStatus ) + " " + res[ 'Message' ]
      else:
        try:
          if granularity in ( 'Resource', 'Resources' ):
            statusList.append( res[ 'Value' ][ 'Records' ][ 0 ][ 5 ] ) 
          else:
            statusList.append( res[ 'Value' ][ 'Records' ][ 0 ][ 4 ] )
        except IndexError:
          return None

    return statusList

#############################################################################

  def getGridSiteName( self, granularity, name ):
    """
    Returns the grid site name (what is in GOC BD)

    :Parameters:
      `granularity`
        string, should be a ValidRes

      `name`
        string, name of site or resource
    """

    res = self.rsS.getGridSiteName( granularity, name )
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getGridSiteName ) + " " + res[ 'Message' ]

    return res[ 'Value' ]

#############################################################################

  def getResourcesList( self ):
    """
    Returns the list of resources in the RSS DB
    
    """
  
    res = self.rsS.getResourcesList()
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getResourcesList ) + " " + res[ 'Message' ] 
    
    return res[ 'Value' ]

#############################################################################

  def getStorageElementsList( self, access ):
    """
    Returns the list of storage elements in the RSS DB
    
    """
  
    res = self.rsS.getStorageElementsList( access )
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getStorageElementsList ) + " " + res[ 'Message' ] 
    
    return res[ 'Value' ]

#############################################################################

  def getServicesList( self ):
    """
    Returns the list of services in the RSS DB
    
    """
  
    res = self.rsS.getServicesList()
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getServicesList ) + " " + res[ 'Message' ] 
    
    return res[ 'Value' ]

#############################################################################

  def getSitesList( self ):
    """
    Returns the list of sites in the RSS DB
    
    """
  
    res = self.rsS.getSitesList()
    if not res[ 'OK' ]:
      raise RSSException, where( self, self.getSitesList ) + " " + res[ 'Message' ] 
    
    return res[ 'Value' ]

#############################################################################