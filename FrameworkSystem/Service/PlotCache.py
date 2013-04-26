# $HeadURL$

""" Cache for the Plotting service plots
"""

__RCSID__ = "$Id$"

import os
import os.path
import time
import threading

from DIRAC import S_OK, S_ERROR, gLogger, rootPath
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.Graphs import graph

class PlotCache:

  def __init__( self, plotsLocation = False ):
    self.plotsLocation = plotsLocation
    self.alive = True
    self.__graphCache = DictCache( deleteFunction = _deleteGraph )
    self.__graphLifeTime = 600
    self.purgeThread = threading.Thread( target = self.purgeExpired )
    self.purgeThread.setDaemon( 1 )
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

  def getPlot( self, plotHash, plotData, plotMetadata, subplotMetadata ):
    """
    Get plot from the cache if exists, else generate it
    """

    plotDict = self.__graphCache.get( plotHash )
    if plotDict == False:
      basePlotFileName = "%s/%s.png" % ( self.plotsLocation, plotHash )
      if subplotMetadata:
        retVal = graph( plotData, basePlotFileName, plotMetadata, metadata = subplotMetadata )
      else:
        retVal = graph( plotData, basePlotFileName, plotMetadata )
      if not retVal[ 'OK' ]:
        return retVal
      plotDict = retVal[ 'Value' ]
      if plotDict[ 'plot' ]:
        plotDict[ 'plot' ] = os.path.basename( basePlotFileName )
      self.__graphCache.add( plotHash, self.__graphLifeTime, plotDict )
    return S_OK( plotDict )

  def getPlotData( self, plotFileName ):
    filename = "%s/%s" % ( self.plotsLocation, plotFileName )
    try:
      fd = file( filename, "rb" )
      data = fd.read()
      fd.close()
    except Exception, v:
      return S_ERROR( "Can't open file %s: %s" % ( plotFileName, str( v ) ) )
    return S_OK( data )

def _deleteGraph( plotDict ):
  try:
    for key in plotDict:
      value = plotDict[ key ]
      if value and os.path.isfile( value ):
        os.unlink( value )
  except:
    pass

gPlotCache = PlotCache()
