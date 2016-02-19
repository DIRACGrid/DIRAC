########################################################################
# $Id: $
########################################################################

"""

It creates the reports using Elasticsearch

"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RequestHandler              import RequestHandler
from DIRAC                                        import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.MonitoringSystem.DB.MonitoringDB       import MonitoringDB
from DIRAC.Core.Utilities                         import Time

import types


class MonitoringHandler( RequestHandler ):
  
  __reportRequestDict = { 'typeName' : types.StringType,
                        'reportName' : types.StringType,
                        'startTime' : Time._allDateTypes,
                        'endTime' : Time._allDateTypes,
                        'condDict' : types.DictType,
                        'grouping' : types.StringType
                      }
  
  __db = None
  
  @classmethod
  def initializeHandler( cls, serviceInfo ):
    cls.__db = MonitoringDB()
    return S_OK()
  
   
  types_listUniqueKeyValues = [ types.StringType ]
  def export_listUniqueKeyValues( self, typeName ):
    """
    List all values for all keys in a type
    Arguments:
      - none
    """
    setup = self.serviceInfoDict.get('clientSetup', None)
    if not setup:
      return S_ERROR("FATAL ERROR:  Problem with the service configuration!")
        #we can apply some policies if it will be needed!
    return self.__db.getKeyValues( typeName, setup)
    