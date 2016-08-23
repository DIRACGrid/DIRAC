########################################################################
# $Id: $
########################################################################

"""
This class is a helper to create the proper index and insert the proper values....
"""

__RCSID__ = "$Id $"

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

class WMSMonitorType( BaseType ):

  def __init__( self ):

    self.keyFields = [ 'Status', 'Site', 'User', 'UserGroup',\
                      'JobGroup', 'MinorStatus', 'ApplicationStatus',\
                      'JobSplitType' ]
    
    self.monitoringFields = [ 'Jobs', 'Reschedules' ]
    
    self.index = 'wmshistory_index'
    
    self.checkType()
    