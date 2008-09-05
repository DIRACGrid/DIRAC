
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.private.Plotters import gPlottersList
from DIRAC.AccountingSystem.private.Policies import gPoliciesList
from DIRAC.Core.Utilities import Time

class MainReporter:

  def __init__( self, db, setup ):
    self.db = db
    self.setup = setup

  def generate( self, reportRequest, credDict ):
    typeName = reportRequest[ 'typeName' ]
    if typeName not in gPlottersList:
      return S_ERROR( "There's no reporter registered for type %s" % typeName )
    if typeName in gPoliciesList:
      retVal = gPoliciesList[ typeName ].checkRequest( reportRequest[ 'reportName' ],
                                                    credDict,
                                                    reportRequest[ 'condDict' ],
                                                    reportRequest[ 'grouping' ] )
      if not retVal[ 'OK' ]:
        return retVal
    plotter = gPlottersList[ typeName ]( self.db, self.setup, reportRequest[ 'extraArgs' ] )
    return plotter.generate( reportRequest )

  def list( self, typeName ):
    if typeName not in gPlottersList:
      return S_ERROR( "There's no plotter registered for type %s" % typeName )
    plotter = gPlottersList[ typeName ]( self.db, self.setup )
    return S_OK( plotter.plotsList() )
