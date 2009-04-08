# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Service/ReportGeneratorHandler.py,v 1.21 2009/04/08 14:33:48 acasajus Exp $
__RCSID__ = "$Id: ReportGeneratorHandler.py,v 1.21 2009/04/08 14:33:48 acasajus Exp $"
import types
import os
from DIRAC import S_OK, S_ERROR, rootPath, gConfig, gLogger, gMonitor
from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB
from DIRAC.AccountingSystem.private.Summaries import Summaries
from DIRAC.AccountingSystem.private.DataCache import gDataCache
from DIRAC.AccountingSystem.private.MainReporter import MainReporter
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.AccountingSystem.private.Policies import gPoliciesList
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time

gAccountingDB = False

def initializeReportGeneratorHandler( serviceInfo ):
  global gAccountingDB
  gAccountingDB = AccountingDB()
  #Get data location
  reportSection = PathFinder.getServiceSection( "Accounting/ReportGenerator" )
  dataPath = gConfig.getValue( "%s/DataLocation" % reportSection, "data/accountingGraphs" )
  dataPath = dataPath.strip()
  if "/" != dataPath[0]:
    dataPath = os.path.realpath( "%s/%s" % ( rootPath, dataPath ) )
  gLogger.info( "Data will be written into %s" % dataPath )
  try:
    os.makedirs( dataPath )
  except:
    pass
  try:
    testFile = "%s/acc.jarl.test" % dataPath
    fd = file( testFile, "w" )
    fd.close()
    os.unlink( testFile )
  except IOError:
    gLogger.fatal( "Can't write to %s" % dataPath )
    return S_ERROR( "Data location is not writable" )
  gDataCache.setGraphsLocation( dataPath )
  gMonitor.registerActivity( "plotsDrawn", "Drawn plot images", "Accounting reports", "plots", gMonitor.OP_SUM )
  gMonitor.registerActivity( "reportsRequested", "Generated reports", "Accounting reports", "reports", gMonitor.OP_SUM )
  return S_OK()

class ReportGeneratorHandler( RequestHandler ):

  __reportRequestDict = { 'typeName' : types.StringType,
                        'reportName' : types.StringType,
                        'startTime' : Time._allDateTypes,
                        'endTime' : Time._allDateTypes,
                        'condDict' : types.DictType,
                        'grouping' : types.StringType,
                        'extraArgs' : types.DictType
                      }



  def __checkPlotRequest( self, reportRequest ):
    for key in self.__reportRequestDict:
      if key == 'extraArgs' and key not in reportRequest:
        reportRequest[ key ] = {}
      if not key in reportRequest:
        return S_ERROR( 'Missing mandatory field %s in plot reques' % key )
      requestKeyType = type( reportRequest[ key ] )
      if key in ( 'startTime', 'endTime' ):
        if requestKeyType not in self.__reportRequestDict[ key ]:
          return S_ERROR( "Type mismatch for field %s (%s), required one of %s" % ( key,
                                                                                    str(requestKeyType),
                                                                                    str( self.__reportRequestDict[ key ] ) ) )
        reportRequest[ key ] = int( Time.toEpoch( reportRequest[ key ] ) )
      else:
        if requestKeyType != self.__reportRequestDict[ key ]:
          return S_ERROR( "Type mismatch for field %s (%s), required %s" % ( key,
                                                                             str(requestKeyType),
                                                                             str( self.__reportRequestDict[ key ] ) ) )
    return S_OK( reportRequest )

  types_generatePlot = [ types.DictType ]
  def export_generatePlot( self, reportRequest ):
    """
    Plot a accounting
      Arguments:
        - viewName : Name of view (easy!)
        - startTime
        - endTime
        - argsDict : Arguments to the view.
        - grouping
        - extraArgs
    """
    retVal = self.__checkPlotRequest( reportRequest )
    if not retVal[ 'OK' ]:
      return retVal
    reporter = MainReporter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    gMonitor.addMark( "plotsDrawn" )
    reportRequest[ 'generatePlot' ] = True
    return reporter.generate( reportRequest, self.getRemoteCredentials() )

  types_getReport = [ types.DictType ]
  def export_getReport( self, reportRequest ):
    """
    Plot a accounting
      Arguments:
        - viewName : Name of view (easy!)
        - startTime
        - endTime
        - argsDict : Arguments to the view.
        - grouping
        - extraArgs
    """
    retVal = self.__checkPlotRequest( reportRequest )
    if not retVal[ 'OK' ]:
      return retVal
    reporter = MainReporter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    gMonitor.addMark( "reportsRequested" )
    reportRequest[ 'generatePlot' ] = False
    return reporter.generate( reportRequest, self.getRemoteCredentials() )

  types_listReports = [ types.StringType ]
  def export_listReports( self, typeName ):
    """
    List all available plots
      Arguments:
        none
    """
    reporter = MainReporter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    return reporter.list( typeName )

  types_listUniqueKeyValues = [ types.StringType ]
  def export_listUniqueKeyValues( self, typeName ):
    """
    List all values for all keys in a type
      Arguments:
        none
    """
    dbUtils = DBUtils( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    credDict = self.getRemoteCredentials()
    if typeName in gPoliciesList:
      policyFilter = gPoliciesList[ typeName ]
      filterCond = policyFilter.getListingConditions( credDict )
    else:
      policyFilter = False
      filterCond = {}
    retVal = dbUtils.getKeyValues( typeName, filterCond )
    if not policyFilter or not retVal[ 'OK' ]:
      return retVal
    return policyFilter.filterListingValues( credDict, retVal[ 'Value' ] )


  def transfer_toClient( self, fileId, token, fileHelper ):
    """
    Get graphs data
    """
    retVal = gDataCache.getPlotData( fileId )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = fileHelper.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.sendEOF()
    return S_OK()