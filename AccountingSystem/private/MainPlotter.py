
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.private.Plotters import gPlottersList
from DIRAC.Core.Utilities import Time

class MainPlotter:

  requiredParams = ()

  def __init__( self, db, setup ):
    self.db = db
    self.setup = setup

  def generate( self, typeName, plotName, startTime, endTime, argsDict, grouping ):
    if typeName not in gPlottersList:
      return S_ERROR( "There's no plotter registered for type %s" % typeName )
    plotter = gPlottersList[ typeName ]( self.db, self.setup )
    return plotter.generate( plotName, startTime, endTime, argsDict, grouping )

  def plotsList( self, typeName ):
    if typeName not in gPlottersList:
      return S_ERROR( "There's no plotter registered for type %s" % typeName )
    plotter = gPlottersList[ typeName ]( self.db, self.setup )
    return S_OK( plotter.plotsList() )
