# $HeadURL$
__RCSID__ = "$Id$"

"""

It creates the reports using Elasticsearch

"""

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class ReportGeneratorHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    return S_OK()

