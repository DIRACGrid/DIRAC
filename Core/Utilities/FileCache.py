# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/FileCache.py,v 1.2 2008/06/05 18:03:52 acasajus Exp $
__RCSID__ = "$Id: FileCache.py,v 1.2 2008/06/05 18:03:52 acasajus Exp $"

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
    """
    Init with a name
    """
    self.__cacheName = cacheName
    self.__defaultLifeTime = defaultFileLifeTime
    self.__filesLocation = "%s/data/cache/%s" % ( rootPath, cacheName )
    self.__cachedFiles = {}
    self.__alive = True
    self.__initCacheLocation()
    self.__purgeThread = threading.Thread( target = self.__purgeExpired )
    self.__purgeThread.start()

  def __initCacheLocation(self):
    try:
      os.makedirs( self.__filesLocation )
    except:
      pass
    try:
      fd = file( "%s/testPerm" % self.__filesLocation, "w" )
      fd.write( self.__cacheName )
      fd.close()
    except:
      raise Exception( "Can't write into %s, check perms" % self.__filesLocation )

  def __generateName( self, *args, **kwargs ):
    m = md5.new()
    m.update( repr( args ) )
    m.update( repr( kwargs ) )
    return m.hexdigest()

  def __purgeExpired( self ):
    while self.__alive:
      time.sleep( 600 )
      self.purge()

  def __isCurrentTime( self, toSecs ):
    currentBucket = self.rrdManager.getCurrentBucketTime( self.graceTime )
    return toSecs + self.graceTime > currentBucket

  @gSynchro
  def purge( self ):
    """
    Purge exired files
    """
    now = time.time()
    filesToDelete = []
    for fileName in self.__cachedFiles:
      fileData = self.__cachedFiles[ fileName ]
      if fileData[0] + fileData[1] < now:
        filesToDelete.append( fileName )
    while filesToDelete:
      fileName = filesToDelete.pop()
      try:
        gLogger.verbose( "Purging %s" % fileName )
        os.unlink( "%s/%s" % ( self.__filesLocation, fileName ) )
      except Exception, e:
        gLogger.error( "Can't delete file file %s: %s" % ( fileName, str(e) ) )
      del( self.__cachedFiles[ fileName ] )

  @gSynchro
  def __addToCache( self, fileName, lifeTime ):
    if fileName not in self.__cachedFiles:
      self.__cachedFiles[ fileName ] = [ Time.toEpoch(), lifeTime ]

  def generateFile( self, funcToGenerate, extraArgs = (), fileName = False, lifeTime = False ):
    """
    Generate a file
      -args: funcToGenerate: Callback for generating a file
                              FIRST ARGUMENT WILL ALWAYS BE THE FILE LOCATION
             extraArgs: Extra arguments for the callback
             fileName : Filename to generate, if omited a name will be generated
             lifeTime : life time for the file
    """
    if not fileName:
      fileName = "%s.png" % self.__generateName( funcToGenerate, extraArgs, fileLifeTime )
    if fileName not in self.__cachedFiles:
      filePath = "%s/%s" %( self.__filesLocation, fileName )
      try:
        retVal = funcToGenerate( filePath, *extraArgs )
        if not retVal[ 'OK' ]:
          return retVal
      except Exception, e:
        gLogger.exception( "Exception while generating file" )
        return S_ERROR( "Exception while generating file" )
      if not lifeTime:
        lifeTime = self.__defaultLifeTime
      self.__addToCache( fileName, lifeTime )
    return S_OK( fileName )

  def getFileData( self, fileName ):
    """
    Get the contents of a file previously generated
    """
    try:
      fd = file( "%s/%s" % ( self.__filesLocation, fileName ), "rb" )
      fileData = fd.read()
      fd.close()
    except Exception, e:
      return S_ERROR( "Can't get file %s: %s" % ( fileName, str(e) ) )
    return S_OK( fileData )