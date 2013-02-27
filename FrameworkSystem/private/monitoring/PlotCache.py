# $HeadURL$
__RCSID__ = "$Id$"

import os
import os.path
try:
  import hashlib as md5
except:
  import md5
import time
import threading

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer
from DIRAC.FrameworkSystem.private.monitoring.RRDManager import RRDManager
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor


gSynchro = Synchronizer()

class PlotCache:

  def __init__( self, RRDManager ):
    self.rrdManager = RRDManager
    self.plotsLocation = self.rrdManager.getGraphLocation()
    for file in os.listdir( self.plotsLocation ):
      if file.find( ".png" ) > 0:
        os.unlink( "%s/%s" % ( self.plotsLocation, file ) )
    self.cachedPlots = {}
    self.alive = True
    self.graceTime = 60
    self.purgeThread = threading.Thread( target = self.purgeCached )
    self.purgeThread.setDaemon( 1 )
    self.purgeThread.start()

  def __generateName( self, *args, **kwargs ):
    m = md5.md5()
    m.update( repr( args ) )
    m.update( repr( kwargs ) )
    return m.hexdigest()

  def __isCurrentTime( self, toSecs ):
    currentBucket = self.rrdManager.getCurrentBucketTime( self.graceTime )
    return toSecs + self.graceTime > currentBucket

  def purgeCached( self ):
    while self.alive:
      time.sleep( self.graceTime * 2 )
      self.__purgeExpiredGraphs()

  @gSynchro
  def __refreshGraph( self, graphFile ):
    self.cachedPlots[ graphFile ][1] = time.time()

  @gSynchro
  def __registerGraph( self, graphFile, fromSecs, toSecs ):
    if self.__isCurrentTime( toSecs ):
      cacheTime = self.graceTime * 2
    else:
      cacheTime = 3600
    self.cachedPlots[ graphFile ] = [ cacheTime, time.time() ]

  @gSynchro
  def __isCacheGraph( self, graphFile ):
    return graphFile in self.cachedPlots

  @gSynchro
  def __purgeExpiredGraphs( self ):
    now = time.time()
    graphsToDelete = []
    for cachedFile in self.cachedPlots:
      fileData = self.cachedPlots[ cachedFile ]
      if fileData[0] + fileData[1] < now:
        graphsToDelete.append( cachedFile )
    for cachedFile in graphsToDelete:
      try:
        filePath = "%s/%s" % ( self.plotsLocation, cachedFile )
        os.unlink( filePath )
      except Exception, e:
        gLogger.error( "Can't delete plot file %s: %s" % ( filePath, str( e ) ) )
      del( self.cachedPlots[ cachedFile ] )

  def groupPlot( self, *args ):
    return self.__getGraph( getattr( self.rrdManager, "groupPlot" ) , args )

  def plot( self, *args ):
    return self.__getGraph( getattr( self.rrdManager, "plot" ) , args )

  def __getGraph( self, plotFunc, args ):
    fromSecs = args[0]
    toSecs = args[1]
    graphFile = "%s-%s-%s.png" % ( self.__generateName( *args[2:] ),
                                   self.rrdManager.bucketize( fromSecs, self.graceTime ),
                                   self.rrdManager.bucketize( toSecs, self.graceTime )
                                )
    if self.__isCacheGraph( graphFile ):
      self.__refreshGraph( graphFile )
      gLogger.info( "Cached graph file %s" % graphFile )
      gMonitor.addMark( "cachedplots" )
      return S_OK( graphFile )
    else:
      gMonitor.addMark( "drawnplots" )
      self.__registerGraph( graphFile, fromSecs, toSecs )
      return plotFunc( graphFilename = graphFile, *args )




