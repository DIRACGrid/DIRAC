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

class ReportGeneratorHandler( RequestHandler ):
  
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
    dbUtils = DBUtils( self.__db, self.serviceInfoDict[ 'clientSetup' ] )
    credDict = self.getRemoteCredentials()
    if typeName in gPoliciesList:
      policyFilter = gPoliciesList[ typeName ]
      filterCond = policyFilter.getListingConditions( credDict )
    else:
      policyFilter = gPoliciesList[ 'Null' ]
      filterCond = {}
    retVal = dbUtils.getKeyValues( typeName, filterCond )
    if not policyFilter or not retVal[ 'OK' ]:
      return retVal
    return policyFilter.filterListingValues( credDict, retVal[ 'Value' ] )
