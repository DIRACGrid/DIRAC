# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Service/ReportGeneratorHandler.py,v 1.7 2008/05/05 15:31:14 acasajus Exp $
__RCSID__ = "$Id: ReportGeneratorHandler.py,v 1.7 2008/05/05 15:31:14 acasajus Exp $"
import types
import os
from DIRAC import S_OK, S_ERROR, rootPath, gConfig, gLogger
from DIRAC.AccountingSystem.private.AccountingDB import AccountingDB
from DIRAC.AccountingSystem.private.Summaries import Summaries
from DIRAC.AccountingSystem.private.ViewsCache import gViewsCache
from DIRAC.AccountingSystem.private.ViewPlotter import ViewPlotter
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
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
  gViewsCache.setGraphsLocation( dataPath )
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

  types_plotView = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.DictType ]
  def export_plotView( self, viewName, startTime, endTime, argsDict ):
    """
    Plot a accounting view
      Arguments:
        - viewName : Name of view (easy!)
        - startTime
        - endTime
        - argsDict : Arguments to the view.
    """
    plotter = ViewPlotter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    startTime = int( Time.toEpoch( startTime ) )
    endTime = int( Time.toEpoch( endTime ) )
    return plotter.generate( viewName, startTime, endTime, argsDict )

  types_listViews = []
  def export_listViews( self ):
    """
    List all available views
      Arguments:
        none
    """
    plotter = ViewPlotter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    return S_OK( plotter.viewsList() )

  types_listUniqueKeyValues = [ types.StringType ]
  def export_listUniqueKeyValues( self, typeName ):
    """
    List all values for all keys in a type
      Arguments:
        none
    """
    dbUtils = DBUtils( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    return dbUtils.getKeyValues( typeName )

  def transfer_toClient( self, fileId, token, fileHelper ):
    """
    Get graphs data
    """
    retVal = gViewsCache.getGraphData( fileId )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = fileHelper.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.sendEOF()
    return S_OK()