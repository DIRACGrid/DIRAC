# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/MappingFileCache.py,v 1.2 2008/07/23 11:35:15 asypniew Exp $
__RCSID__ = "$Id: MappingFileCache.py,v 1.2 2008/07/23 11:35:15 asypniew Exp $"

import os
import os.path
import time
import threading
import re

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer

gSynchro = Synchronizer()

class MappingFileCache:

  def __init__( self, defaultFileLifeTime = 600, doNotCache = None ):
    """
    Init with a default lifetime.
    doNotCache is a list of regular expressions--it will be consulted every time a file
      is queued for caching; if the file matches on of the reg.ex., it will not be cached
    """
    cacheExceptions = []
    if doNotCache:
      for pattern in doNotCache:
        cacheExceptions.append(re.compile(pattern))
    
    self.__defaultLifeTime = defaultFileLifeTime
    self.__cacheExceptions = cacheExceptions
    self.__cachedFiles = {}
    self.__alive = True
    self.__purgeThread = threading.Thread( target = self.__purgeExpired )
    self.__purgeThread.start()

  def __purgeExpired( self ):
    while self.__alive:
      time.sleep( self.__defaultLifeTime )
      self.purge()

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
        os.unlink( "%s" % fileName )
      except Exception, e:
        gLogger.error( "Can't delete file %s: %s" % ( fileName, str(e) ) )
      del( self.__cachedFiles[ fileName ] )

  def addToCache( self, fileName, lifeTime = False):
    """
    Adds a new file to the cache. If the file is already there, its timer is reset
    """
    
    # Check to see if the file should be excluded.
    for pattern in self.__cacheExceptions:
      if re.match(pattern, fileName):
        gLogger.verbose('File matches exclusion pattern. Ignoring: %s' % fileName)
        return S_OK(fileName)
        
    if not lifeTime:
      lifeTime = self.__defaultLifeTime
    now = time.time()
    self.__cachedFiles[ fileName ] = [ now, lifeTime ]
    gLogger.verbose('File added to cache. File %s. Lifetime: %d' % (fileName, lifeTime))
    return S_OK(fileName)

  def getFileData( self, fileName ):
    """
    Get the contents of a file previously generated
    This can be used to test to see if the file exists
    If the file isn't in the cache, it is added
    """
      
    try:
      fd = file( "%s" % fileName, "rb" )
      fileData = fd.read()
      fd.close()
    except Exception, e:
      return S_ERROR( "Can't get file %s: %s" % ( fileName, str(e) ) )
      
    # Assume that the file should be added if found
    if fileName not in self.__cachedFiles:
      self.addToCache(fileName)
            
    return S_OK( fileData )
