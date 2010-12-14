try:
  import hashlib as md5
except:
  import md5
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.AccountingSystem.private.Plotters import gPlottersList
from DIRAC.AccountingSystem.private.Policies import gPoliciesList
from DIRAC.Core.Utilities import Time

class MainReporter:

  def __init__( self, db, setup ):
    self.db = db
    self.setup = setup
    self.csSection = getServiceSection( "Accounting/ReportGenerator", setup = setup )

  def __calculateReportHash( self, reportRequest ):
    requestToHash = dict( reportRequest )
    granularity = gConfig.getValue( "%s/CacheTimeGranularity" % self.csSection, 300 )
    for key in ( 'startTime', 'endTime' ):
      epoch = requestToHash[ key ]
      requestToHash[ key ] = epoch - epoch % granularity
    m = md5.md5()
    m.update( repr( requestToHash ) )
    m.update( self.setup )
    return m.hexdigest()

  def generate( self, reportRequest, credDict ):
    typeName = reportRequest[ 'typeName' ]
    plotterClass = gPlottersList.getPlotterClass( typeName )
    if not plotterClass:
      return S_ERROR( "There's no reporter registered for type %s" % typeName )
    if typeName in gPoliciesList:
      retVal = gPoliciesList[ typeName ].checkRequest( reportRequest[ 'reportName' ],
                                                    credDict,
                                                    reportRequest[ 'condDict' ],
                                                    reportRequest[ 'grouping' ] )
      if not retVal[ 'OK' ]:
        return retVal
    reportRequest[ 'hash' ] = self.__calculateReportHash( reportRequest )
    plotter = plotterClass( self.db, self.setup, reportRequest[ 'extraArgs' ] )
    return plotter.generate( reportRequest )

  def list( self, typeName ):
    plotterClass = gPlottersList.getPlotterClass( typeName )
    if not plotterClass:
      return S_ERROR( "There's no plotter registered for type %s" % typeName )
    plotter = plotterClass( self.db, self.setup )
    return S_OK( plotter.plotsList() )
