"""
ComponentMonitoring type used to monitor DIRAC components.
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

__RCSID__ = "$Id $"

########################################################################
class ComponentMonitoring( BaseType ):


  """
  .. class:: ComponentMonitoring
  """
  ########################################################################
  def __init__( self ):
    super( ComponentMonitoring, self ).__init__()

    """ c'tor
    :param self: self reference
    """

    self.__keyFields = [ 'host', 'component', 'pid', 'status']

    self.__monitoringFields = [ 'runningTime', 'memoryUsage', 'threads', 'cpuUsage' ]

    self.__doc_type = "ComponentMonitoring" 

    self.__mapping = {'host_type': {'_all': {'enabled': 'false'}, 'properties': {'host': {'index': 'not_analyzed', 'type': 'string'}}},
                      'component_type':{'_all': {'enabled': 'false'}, 'properties': {'component': { 'index': 'not_analyzed',
                                                                                                    'type': 'string'}}},
                      'status_type':{'_all': {'enabled': 'false'}, 'properties': {'status': {'index': 'not_analyzed', 'type': 'string'}}}}

    self.__dataToKeep = 86400 * 30 #we need to define...
    
    self.__period = "month"
    self.checkType()
