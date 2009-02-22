# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Service/PlotCache.py,v 1.1 2009/02/22 23:57:01 atsareg Exp $

""" Cache for the Plotting service plots
"""

__RCSID__ = "$Id: PlotCache.py,v 1.1 2009/02/22 23:57:01 atsareg Exp $"

import os
import os.path
import md5
import time
import threading

from DIRAC import S_OK, S_ERROR, gLogger, rootPath
from DIRAC.Core.Utilities import DictCache
from DIRAC.Core.Utilities import Time


class PlotCache:

  def __init__( self, plotsLocation=False ):
    self.plotsLocation = plotsLocation
    self.alive = True
    self.__graphCache = DictCache( deleteFunction = self.__deleteGraph )
    self.__graphLifeTime = 600
    self.purgeThread = threading.Thread( target = self.purgeExpired )
    self.purgeThread.start()

  def setPlotsLocation( self, plotsDir ):
    self.plotsLocation = plotsDir
    for plot in os.listdir( self.plotsLocation ):
      if plot.find( ".png" ) > 0:
        plotLocation = "%s/%s" % ( self.plotsLocation, plot )
        gLogger.verbose( "Purging %s" % plotLocation )
        os.unlink( plotLocation )

  def purgeExpired( self ):
    while self.alive:
      time.sleep( self.__graphLifeTime )
      self.__graphCache.purgeExpired()

  def __deleteGraph( self, plotDict ):
    try:
      for key in plotDict:
        value = plotDict[ key ]
        if value and os.path.isfile( value ):
          os.unlink( value )
    except:
      pass

  def getPlot( self, plotHash, plotData, plotMetadata, plotFunc ):
    """
    Get plot from the cache if exists, else generate it
    """
    plotDict = self.__graphCache.get( plotHash )
    if plotDict == False:
      basePlotFileName = "%s/%s" % ( self.plotsLocation, plotHash )
      retVal = plotFunc( plotData, plotMetadata, basePlotFileName )
      if not retVal[ 'OK' ]:
        return retVal
      plotDict = retVal[ 'Value' ]
      if plotDict[ 'plot' ]:
        plotDict[ 'plot' ] = "%s.png" % plotHash
      #if plotDict[ 'thumbnail' ]:
      #  plotDict[ 'thumbnail' ] = "%s.thb.png" % plotHash
      self.__graphCache.add( plotHash, self.__graphLifeTime, plotDict )
    return S_OK( plotDict )
    
  def getPlotData( self, plotFileName ):
    filename = "%s/%s" % ( self.plotsLocation, plotFileName )
    try:
      fd = file( filename, "rb" )
      data = fd.read()
      fd.close()
    except Exception, v:
      return S_ERROR( "Can't open file %s: %s" % ( plotFileName, str(v) ) )
    return S_OK( data )  

gPlotCache = PlotCache()
