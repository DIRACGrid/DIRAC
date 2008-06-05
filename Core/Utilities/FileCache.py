# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/FileCache.py,v 1.1 2008/06/05 17:53:50 acasajus Exp $
__RCSID__ = "$Id: FileCache.py,v 1.1 2008/06/05 17:53:50 acasajus Exp $"

import os
import os.path
import md5
import time
import threading

import os
from DIRAC import S_OK, S_ERROR, gLogger, rootPath
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer
from DIRAC.Core.Utilities import Time

gSynchro = Synchronizer()

class FileCache:

  def __init__( self, cacheName, defaultFileLifeTime = 600 ):
    self.cacheName = cacheName
    self.defaultLifeTime = defaultFileLifeTime
    self.filesLocation = "%s/data/cache/%s" % ( rootPath, cacheName )
    self.cachedFiles = {}
    self.alive = True
    self.__initCacheLocation()
    self.purgeThread = threading.Thread( target = self.purge )
    self.purgeThread.start()

  def __initCacheLocation(self):
    try:
      os.makedirs( self.filesLocation )
    except:
      pass
    try:
      fd = file( "%s/testPerm" % self.filesLocation, "w" )
      fd.write( self.cacheName )
      fd.close()
    except:
      raise Exception( "Can't write into %s, check perms" % self.filesLocation )

  def __generateName( self, *args, **kwargs ):
    m = md5.new()
    m.update( repr( args ) )
    m.update( repr( kwargs ) )
    return m.hexdigest()

  def purge( self ):
    while self.alive:
      time.sleep( 600 )
      self.__purgeExpired()

  def __isCurrentTime( self, toSecs ):
    currentBucket = self.rrdManager.getCurrentBucketTime( self.graceTime )
    return toSecs + self.graceTime > currentBucket

  @gSynchro
  def __purgeExpired( self ):
    now = time.time()
    filesToDelete = []
    for fileName in self.cachedFiles:
      fileData = self.cachedFiles[ fileName ]
      if fileData[0] + fileData[1] < now:
        filesToDelete.append( fileName )
    while filesToDelete:
      fileName = filesToDelete.pop()
      try:
        gLogger.verbose( "Purging %s" % fileName )
        os.unlink( "%s/%s" % ( self.filesLocation, fileName ) )
      except Exception, e:
        gLogger.error( "Can't delete file file %s: %s" % ( fileName, str(e) ) )
      del( self.cachedFiles[ fileName ] )

  @gSynchro
  def __addToCache( self, fileName, lifeTime ):
    if fileName not in self.cachedFiles:
      self.cachedFiles[ fileName ] = [ Time.toEpoch(), lifeTime ]

  def generateFile( self, funcToGenerate, funcArgs, fileName = False, lifeTime = False ):
    if not fileName:
      fileName = "%s.png" % self.__generateName( funcToGenerate, funcArgs, fileLifeTime )
    if fileName not in self.cachedFiles:
      try:
        retVal = funcToGenerate( *funcArgs )
        if not retVal[ 'OK' ]:
          return retVal
      except Exception, e:
        gLogger.exception( "Exception while generating file" )
        return S_ERROR( "Exception while generating file" )
      if not lifeTime:
        lifeTime = self.defaultLifeTime
      self.__addToCache( fileName, lifeTime )
    return S_OK( fileName )

  def getFileData( self, fileName ):
    try:
      fd = file( "%s/%s" % ( self.filesLocation, fileName ), "rb" )
      fileData = fd.read()
      fd.close()
    except Exception, e:
      return S_ERROR( "Can't get file %s: %s" % ( fileName, str(e) ) )
    return S_OK( fileData )