########################################################################
# $Id: $
########################################################################

"""
This class is a helper to create the proper index and insert the proper values....
"""

__RCSID__ = "$Id $"

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

########################################################################
class WMSMonitor( BaseType ):

  
  """
  .. class:: WMSMonitorType
  """
  ########################################################################
  def __init__( self ):
    BaseType.__init__( self )
    
    self.setKeyFields( [ 'Status', 'Site', 'User', 'UserGroup', \
                      'JobGroup', 'MinorStatus', 'ApplicationStatus', \
                      'JobSplitType' ] )
    
    self.setMonitoringFields( [ 'Jobs', 'Reschedules' ] )
    
    self.setIndex( 'wmshistory_index' )  # overwrite the index name
    
    self.setDataToKeep ( 86400 * 30 )
    
    self.checkType()
    
