# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/private/Attic/PlotsCache.py,v 1.11 2008/07/29 10:15:50 acasajus Exp $
__RCSID__ = "$Id: PlotsCache.py,v 1.11 2008/07/29 10:15:50 acasajus Exp $"

import os
import os.path
import md5
import time
import threading

from DIRAC import S_OK, S_ERROR, gLogger, rootPath
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer
from DIRAC.Core.Utilities import Time

gSynchro = Synchronizer()

class PlotsCache:

  def __init__( self ):
    self.graphsLocation = "%s/data/accountingPlots" % rootPath
    self.cachedGraphs = {}
    self.alive = True
    self.purgeThread = threading.Thread( target = self.purgeCached )
    self.purgeThread.start()

  def setGraphsLocation( self, graphsDir ):
    self.graphsLocation = graphsDir
    for graphName in os.listdir( self.graphsLocation ):
      if graphName.find( ".png" ) > 0:
        graphLocation = "%s/%s" % ( self.graphsLocation, graphName )
        gLogger.verbose( "Purging %s" % graphLocation )
        os.unlink( graphLocation )

  def __generateName( self, *args, **kwargs ):
    m = md5.new()
    m.update( repr( args ) )
    m.update( repr( kwargs ) )
    return m.hexdigest()

  def __calculateGraceTime( self, startTime, endTime ):
    """
    Calculate Grace time
    min 60 secs max 3600
    graceTime = plotSpan * secsFromNow / 10000
    """
    nowEpoch = Time.toEpoch()
    secsFromNow = max( endTime - nowEpoch, 1 )
    plotSpan = endTime - startTime
    graceTime = plotSpan * secsFromNow / 10000
    return min( max( 60, graceTime ), 3600 )

  def purgeCached( self ):
    while self.alive:
      time.sleep( 600 )
      self.__purgeExpiredGraphs()

  def __isCurrentTime( self, toSecs ):
    currentBucket = self.rrdManager.getCurrentBucketTime( self.graceTime )
    return toSecs + self.graceTime > currentBucket

  @gSynchro
  def __purgeExpiredGraphs( self ):
    now = time.time()
    graphsToDelete = []
    for graphName in self.cachedGraphs:
      fileData = self.cachedGraphs[ graphName ]
      if fileData[0] + fileData[1] < now:
        graphsToDelete.append( graphName )
    while graphsToDelete:
      graphName = graphsToDelete.pop()
      try:
        gLogger.verbose( "Purging %s" % graphName )
        os.unlink( "%s/%s" % ( self.graphsLocation, graphName ) )
      except Exception, e:
        gLogger.error( "Can't delete graph file %s: %s" % ( graphName, str(e) ) )
      del( self.cachedGraphs[ graphName ] )

  @gSynchro
  def __addToCache( self, graphName, graceTime ):
    if graphName not in self.cachedGraphs:
      self.cachedGraphs[ graphName ] = [ Time.toEpoch(), graceTime ]

  def generatePlot( self, viewName, startTime, endTime, argsDict, grouping, funcToGenerate ):
    graphName = "%s.png" % self.__generateName( ( viewName, startTime, endTime, argsDict, grouping ) )
    if graphName in self.cachedGraphs:
      return S_OK( graphName )
    try:
      retVal = funcToGenerate( startTime, endTime, argsDict, grouping, "%s/%s" % ( self.graphsLocation, graphName ) )
      if not retVal[ 'OK' ]:
        return retVal
      plotRet = retVal[ 'Value' ]
    except Exception, e:
      gLogger.exception( "Exception while generating %s view" % viewName )
      return S_ERROR( "Exception while generating %s view: %s" % ( viewName, str(e) ) )
    graceTime = self.__calculateGraceTime( startTime, endTime )
    gLogger.info( "Graph %s will be cached for %s seconds" % ( graphName, graceTime ) )
    self.__addToCache( graphName, graceTime )
    finalResult = S_OK( graphName )
    if not plotRet[ 'thumbnail' ]:
      return finalResult
    thbGraphName = graphName.replace( ".png", ".thb.png" )
    self.__addToCache( thbGraphName, graceTime )
    finalResult[ 'thumbnail' ] = thbGraphName
    return finalResult

  @gSynchro
  def __downloadedGraph( self, graphName ):
    if graphName in self.cachedGraphs:
      self.cachedGraphs[ graphName ][1] += 60

  def getGraphData( self, graphName ):
    try:
      fd = file( "%s/%s" % ( self.graphsLocation, graphName ), "rb" )
      graphData = fd.read()
      fd.close()
      self.__downloadedGraph( graphName )
    except Exception, e:
      return S_ERROR( "Can't get graph %s: %s" % ( graphName, str(e) ) )
    return S_OK( graphData )

gPlotsCache = PlotsCache()
