
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.private.Plotters import gPlottersList
from DIRAC.AccountingSystem.private.Policies import gPoliciesList
from DIRAC.Core.Utilities import Time

class MainPlotter:

  def __init__( self, db, setup ):
    self.db = db
    self.setup = setup

  def generate( self, typeName, plotName, credDict, startTime, endTime, argsDict, grouping, extraArgs ):
    if typeName not in gPlottersList:
      return S_ERROR( "There's no plotter registered for type %s" % typeName )
    if typeName in gPoliciesList:
      retVal = gPoliciesList[ typeName ].checkPlot( plotName, credDict, argsDict, grouping )
      if not retVal[ 'OK' ]:
        return retVal
    plotter = gPlottersList[ typeName ]( self.db, self.setup, extraArgs )
    return plotter.generate( plotName, startTime, endTime, argsDict, grouping )

  def plotsList( self, typeName ):
    if typeName not in gPlottersList:
      return S_ERROR( "There's no plotter registered for type %s" % typeName )
    plotter = gPlottersList[ typeName ]( self.db, self.setup )
    return S_OK( plotter.plotsList() )
