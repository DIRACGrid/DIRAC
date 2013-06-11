# $HeadURL: $
""" ComponentSynchronizer
  
  Module that reads ComponentMonitoringDB.compmon_Components table and copies it
  to a RSS-like family of status tables to make everything easier.

"""

from DIRAC                                                  import gConfig, gLogger, S_OK
from DIRAC.FrameworkSystem.DB.ComponentMonitoringDB         import ComponentMonitoringDB
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

__RCSID__ = '$Id: $'


class ComponentSynchronizer:
  """ ComponentSynchronizer
  
  """

  
  def __init__( self ):
    """ Constructor
    
    """
  
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
  
    self.compoDB  = ComponentMonitoringDB()
    self.rsClient = ResourceStatusClient()
      
  
  def sync( self ):
    """ sync. 
    
    Reads from ComponentsMonitoringDB and prepares entries on RSS ComponentStatus
    table.  
    
    """
    
    #TODO: delete from RSS if not anymore on ComponentsMonitoringDB

    setup = gConfig.getValue( 'DIRAC/Setup')

    components = self.compoDB.getComponentsStatus( { 'Setup' : setup } )
    if not components[ 'OK' ]:
      return components
    components = components[ 'Value' ][ 0 ][ setup ]
    
    for agentName, agentsList in components[ 'agent' ].iteritems():
      
      for agentDict in agentsList:
      
        if agentDict[ 'Status' ] == 'Error':
          self.log.warn( '%(ComponentName)s %(Message)s' % agentDict )
          continue
      
        res = self.rsClient.addIfNotThereStatusElement( 'Component', 'Status', 
                                                        name        = agentName, 
                                                        statusType  = agentDict[ 'Host' ], 
                                                        status      = 'Unknown', 
                                                        elementType = 'Agent', 
                                                        reason      = 'Synchronized', 
                                                      )
        if not res[ 'OK' ]:
          return res
           
    for serviceName, servicesList in components[ 'service' ].iteritems():
      
      for serviceDict in servicesList:
      
        if serviceDict[ 'Status' ] == 'Error':
          self.log.warn( '%(ComponentName)s %(Message)s' % serviceDict )
          continue      
      
        res = self.rsClient.addIfNotThereStatusElement( 'Component', 'Status', 
                                                        name        = serviceName, 
                                                        statusType  = '%(Host)s:%(Port)s' % serviceDict, 
                                                        status      = 'Unknown', 
                                                        elementType = 'Service', 
                                                        reason      = 'Synchronized', 
                                                      )
        if not res[ 'OK' ]:
          return res                                                 

    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF