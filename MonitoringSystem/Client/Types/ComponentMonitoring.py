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

    self.setKeyFields( [ 'host', 'component', 'pid', 'status'] )

    self.setMonitoringFields( [ 'runningTime', 'memoryUsage', 'threads', 'cpuUsage' ] )

    #self.setIndex( 'wmshistory_index' )  # overwrite the index name

    self.setDocType( "ComponentMonitoring" )

    self.addMapping( {'host_type': {'_all': {'enabled': 'false'}, 'properties': {'host': {'index': 'not_analyzed', 'type': 'string'}}},
                      'component_type':{'_all': {'enabled': 'false'}, 'properties': {'component': { 'index': 'not_analyzed',
                                                                                                    'type': 'string'}}},
                      'status_type':{'_all': {'enabled': 'false'}, 'properties': {'status': {'index': 'not_analyzed', 'type': 'string'}}}} )

    self.setDataToKeep ( 86400 * 30 )#we need to define...

    self.checkType()
