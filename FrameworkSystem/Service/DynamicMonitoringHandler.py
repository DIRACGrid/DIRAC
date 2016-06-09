"""
This Service provides functionality to read logging information from the ElasticSearch database
"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.FrameworkSystem.DB.DynamicMonitoringDB import DynamicMonitoringDB

class DynamicMonitoringHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    """
    Handler class initialization
    """

    DynamicMonitoringHandler.elasticDB = DynamicMonitoringDB()

  types_getLastLog = [ basestring, basestring ]
  def export_getLastLog( self, host, component ):
    """
    Retrieves the last logging entry for the given component
    :param str host: Host where the component is installed
    :param str component: Name of the component
    :return S_OK/S_ERROR
    """
    return DynamicMonitoringHandler.elasticDB.getLastLog( host, component )

  types_getLogHistory = [ basestring, basestring, int ]
  def export_getLogHistory( self, host, component, size ):
    """
    Retrieves the history of logging entries for the given component
    :param str host: Host where the component is installed
    :param str component: Name of the component
    :param int size: Determines how many entries should be retrieved
    :return S_OK/S_ERROR
    """
    return DynamicMonitoringHandler.elasticDB.getLogHistory( host, component, size )

  types_getLogsPeriod = [ basestring, basestring, basestring, basestring ]
  def export_getLogsPeriod( self, host, component, initialDate, endDate ):
    """
    Retrieves the history of logging entries for the given component during a given given time periodtime period
    :param str host: Host where the component is installed
    :param str component: Name of the component
    :param str initialDate: String indicating the start of the time period, in the format 'DD/MM/YYYY hh:mm'
    :param str endDate: String indicating the end of the time period, in the format 'DD/MM/YYYY hh:mm'
    :return S_OK/S_ERROR
    """
    return DynamicMonitoringHandler.elasticDB.getLogsPeriod( host, component, initialDate, endDate )
