# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Service/ReportGeneratorHandler.py,v 1.14 2008/07/17 09:12:32 acasajus Exp $
__RCSID__ = "$Id: ReportGeneratorHandler.py,v 1.14 2008/07/17 09:12:32 acasajus Exp $"
import types
import os
from DIRAC import S_OK, S_ERROR, rootPath, gConfig, gLogger, gMonitor
from DIRAC.AccountingSystem.private.AccountingDB import AccountingDB
from DIRAC.AccountingSystem.private.Summaries import Summaries
from DIRAC.AccountingSystem.private.PlotsCache import gPlotsCache
from DIRAC.AccountingSystem.private.MainPlotter import MainPlotter
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
  gPlotsCache.setGraphsLocation( dataPath )
  gMonitor.registerActivity( "drawnplots", "Drawn plot images", "Accounting reports", "plots", gMonitor.OP_SUM )
  gMonitor.registerActivity( "generatedsummaries", "Generated summaries", "Accounting reports", "summaries", gMonitor.OP_SUM )
  return S_OK()

class ReportGeneratorHandler( RequestHandler ):

  types_generateSummary = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.DictType ]
  def export_generateSummary( self, summaryName, startTime, endTime, argsDict ):
    """
    Generate summaries
      Arguments:
        - summaryName : Name of summary (easy!)
        - startTime
        - endTime
        - argsDict : Arguments to the summary.
    """
    summariesGeneator = Summaries( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    startTime = int( Time.toEpoch( startTime ) )
    endTime = int( Time.toEpoch( endTime ) )
    gMonitor.addMark( "generatedsummaries" )
    return summariesGeneator.generate( summaryName, startTime, endTime, argsDict )

  types_listSummaries = []
  def export_listSummaries( self ):
    """
    List all available summaries
      Arguments:
        none
    """
    summariesGeneator = Summaries( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    return S_OK( summariesGeneator.summariesList() )

  types_generatePlot = [ types.StringType, types.StringType, Time._allDateTypes, Time._allDateTypes, types.DictType, types.StringType ]
  def export_generatePlot( self, typeName, plotName, startTime, endTime, argsDict, grouping ):
    """
    Plot a accounting
      Arguments:
        - viewName : Name of view (easy!)
        - startTime
        - endTime
        - argsDict : Arguments to the view.
    """
    plotter = MainPlotter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    startTime = int( Time.toEpoch( startTime ) )
    endTime = int( Time.toEpoch( endTime ) )
    gMonitor.addMark( "drawnplots" )
    return plotter.generate( typeName, plotName, self.getRemoteCredentials(), startTime, endTime, argsDict, grouping )

  types_listPlots = [ types.StringType ]
  def export_listPlots( self, typeName ):
    """
    List all available plots
      Arguments:
        none
    """
    plotter = MainPlotter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    return plotter.plotsList( typeName )

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
    else:
      policyFilter = False
    if policyFilter:
      condDict = policyFilter.getListingConditions( credDict )
    retVal = dbUtils.getKeyValues( typeName, condDict )
    if not policyFilter or not retVal[ 'OK' ]:
      return retVal
    return policyFilter.filterListingValues( credDict, retVal[ 'Value' ] )


  def transfer_toClient( self, fileId, token, fileHelper ):
    """
    Get graphs data
    """
    retVal = gPlotsCache.getGraphData( fileId )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = fileHelper.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.sendEOF()
    return S_OK()