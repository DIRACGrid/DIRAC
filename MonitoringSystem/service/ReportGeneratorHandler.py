# $HeadURL$
__RCSID__ = "$Id$"

"""

It creates the reports using Elasticsearch

"""

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from type import StringType

class ReportGeneratorHandler( RequestHandler ):
  
  __db = None
  
  @classmethod
  def initializeHandler( cls, serviceInfo ):
    cls.__db = None
    return S_OK()
  
  types_echo = [StringType]
  @staticmethod
  def export_echo( inputstring ):
    """ Echo input to output
    """
    return S_OK( inputstring )
