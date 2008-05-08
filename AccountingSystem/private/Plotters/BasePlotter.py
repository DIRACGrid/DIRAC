
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.AccountingSystem.private.PlotsCache import gPlotsCache

class BasePlotter(DBUtils):

  requiredParams = ()

  def __init__( self, db, setup ):
    DBUtils.__init__( self, db, setup )

  def _translateGrouping( self, grouping ):
    return [ grouping ]

  def generate( self, plotName, startTime, endTime, argsDict, grouping ):
    missing = []
    for param in self.requiredParams:
      if param not in argsDict:
        missing.append( param )
    if missing:
      return S_ERROR( "Argument(s) %s missing" % ", ".join( missing ) )
    funcName = "_plot%s" % plotName
    try:
      funcObj = getattr( self, funcName )
    except Exception, e:
      return S_ERROR( "Plot  %s is not defined" % plotName )
    return gPlotsCache.generatePlot( plotName,
                                     startTime,
                                     endTime,
                                     argsDict,
                                     self._translateGrouping( grouping ),
                                     funcObj )

  def plotsList( self ):
    viewList = []
    for attr in dir( self ):
      if attr.find( "_plot" ) == 0:
        viewList.append( attr.replace( "_plot", "" ) )
    viewList.sort()
    return viewList
