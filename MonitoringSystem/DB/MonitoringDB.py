"""
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base.ElasticDB import ElasticDB
from DIRAC import S_OK, S_ERROR, gConfig

class MonitoringDB( ElasticDB ):

  def __init__( self, name = 'Monitoring/MonitoringDB' ):
    ElasticDB.__init__( self, 'MonitoringDB', name )