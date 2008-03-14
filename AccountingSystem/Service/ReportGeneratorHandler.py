# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Service/ReportGeneratorHandler.py,v 1.3 2008/03/14 19:10:55 acasajus Exp $
__RCSID__ = "$Id: ReportGeneratorHandler.py,v 1.3 2008/03/14 19:10:55 acasajus Exp $"
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.AccountingSystem.private.AccountingDB import AccountingDB
from DIRAC.AccountingSystem.private.Summaries import Summaries
from DIRAC.AccountingSystem.private.ViewPlotter import ViewPlotter
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time

gAccountingDB = False

def initializeReportGeneratorHandler( serviceInfo ):
  global gAccountingDB
  gAccountingDB = AccountingDB()
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