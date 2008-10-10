# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/private/DataCache.py,v 1.3 2008/10/10 16:40:04 acasajus Exp $
__RCSID__ = "$Id: DataCache.py,v 1.3 2008/10/10 16:40:04 acasajus Exp $"

import os
import os.path
import md5
import time
import threading

from DIRAC import S_OK, S_ERROR, gLogger, rootPath
from DIRAC.Core.Utilities import DictCache
from DIRAC.Core.Utilities import Time


class DataCache:

  def __init__( self ):
    self.graphsLocation = "%s/data/accountingPlots" % rootPath
    self.cachedGraphs = {}
    self.alive = True
    self.purgeThread = threading.Thread( target = self.purgeExpired )
    self.purgeThread.start()
    self.__dataCache = DictCache()
    self.__graphCache = DictCache( deleteFunction = self.__deleteGraph )
    self.__dataLifeTime = 600
    self.__graphLifeTime = 900

  def setGraphsLocation( self, graphsDir ):
    self.graphsLocation = graphsDir
    for graphName in os.listdir( self.graphsLocation ):
      if graphName.find( ".png" ) > 0:
        graphLocation = "%s/%s" % ( self.graphsLocation, graphName )
        gLogger.verbose( "Purging %s" % graphLocation )
        os.unlink( graphLocation )

  def purgeExpired( self ):
    while self.alive:
      time.sleep( 600 )
      self.__graphCache.purgeExpired()
      self.__dataCache.purgeExpired()

  def __deleteGraph( self, plotDict ):
    try:
      for key in plotDict:
        value = plotDict[ 'key' ]
        if value and os.path.isfile( value ):
          os.unlink( value )
    except:
      pass

  def getReportData( self, reportRequest, reportHash, dataFunc ):
    """
    Get report data from cache if exists, else generate it
    """
    reportData = self.__dataCache.get( reportHash )
    if reportData == False:
      retVal = dataFunc( reportRequest )
      if not retVal[ 'OK' ]:
        return retVal
      reportData = retVal[ 'Value' ]
      self.__dataCache.add( reportHash, self.__dataLifeTime, reportData )
    return S_OK( reportData )

  def getReportPlot( self, reportRequest, reportHash, reportData, plotFunc ):
    """
    Get report data from cache if exists, else generate it
    """
    plotDict = self.__graphCache.get( reportHash )
    if plotDict == False:
      basePlotFileName = "%s/%s" % ( self.graphsLocation, reportHash )
      retVal = plotFunc( reportRequest, reportData, basePlotFileName )
      if not retVal[ 'OK' ]:
        return retVal
      plotDict = retVal[ 'Value' ]
      if plotDict[ 'plot' ]:
        plotDict[ 'plot' ] = "%s.png" % reportHash
      if plotDict[ 'thumbnail' ]:
        plotDict[ 'thumbnail' ] = "%s.thb.png" % reportHash
      self.__graphCache.add( reportHash, self.__graphLifeTime, plotDict )
    return S_OK( plotDict )

  def getPlotData( self, plotFileName ):
    filename = "%s/%s" % ( self.graphsLocation, plotFileName )
    try:
      fd = file( filename, "rb" )
      data = fd.read()
      fd.close()
    except Exception, v:
      return S_ERROR( "Can't open file %s: %s" % ( plotFileName, str(v) ) )
    return S_OK( data )

gDataCache = DataCache()
