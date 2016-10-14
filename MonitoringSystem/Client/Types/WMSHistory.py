"""
This class is a helper to create the proper index and insert the proper values....
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

__RCSID__ = "$Id $"

########################################################################
class WMSHistory( BaseType ):

  
  """
  .. class:: WMSMonitorType
  """
  ########################################################################
  def __init__( self ):
    super( WMSHistory, self ).__init__()
    
    """ c'tor
    :param self: self reference
    """
    
    self.setKeyFields( [ 'Status', 'Site', 'User', 'UserGroup', \
                        'JobGroup', 'MinorStatus', 'ApplicationStatus', \
                        'JobSplitType' ] )
    
    self.setMonitoringFields( [ 'Jobs', 'Reschedules' ] )
    
    self.setIndex( 'wmshistory_index' )  # overwrite the index name
    
    self.setDocType( "WMSHistory" )
    
    self.addMapping( {'status_type': {'_all': {'enabled': 'false'}, 'properties': {'Status': {'index': 'not_analyzed', 'type': 'string'}}},
                      'site_type':{'_all': {'enabled': 'false'}, 'properties': {'Site': {'index': 'not_analyzed', 'type': 'string'}}},
                      'jobsplit_type':{'_all': {'enabled': 'false'}, 'properties': {'JobSplitType': {'index': 'not_analyzed', 'type': 'string'}}},
                      'appStatus_type':{'_all': {'enabled': 'false'}, 'properties': {'ApplicationStatus': {'index': 'not_analyzed', 'type': 'string'}}},
                      'monorStat_type':{'_all': {'enabled': 'false'}, 'properties': {'MinorStatus': {'index': 'not_analyzed', 'type': 'string'}}},} )
    
    self.setDataToKeep ( 86400 * 30 )
    
    self.checkType()
    
