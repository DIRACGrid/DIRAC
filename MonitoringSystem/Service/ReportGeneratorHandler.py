########################################################################
# $Id: $
########################################################################

"""

It creates the reports using Elasticsearch

"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB
from types import StringType

class ReportGeneratorHandler( RequestHandler ):
  
  __db = None
  
  @classmethod
  def initializeHandler( cls, serviceInfo ):
    cls.__db = MonitoringDB()
    return S_OK()
  
  types_echo = [StringType]
  @staticmethod
  def export_echo( inputstring ):
    """ Echo input to output
    """
    return S_OK( inputstring )
