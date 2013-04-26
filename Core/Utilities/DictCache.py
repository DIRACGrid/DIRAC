# $HeadURL$
__RCSID__ = "$Id$"

import datetime

# DIRAC
from DIRAC.Core.Utilities.LockRing import LockRing

class DictCache:

  def __init__( self, deleteFunction = False ):
    """
    Initialize the dict cache.
      If a delete function is specified it will be invoked when deleting a cached object
    """
    
    self.__lock = LockRing()
    self.__lock.getLock( self.__class__.__name__, recursive = True )
    
    self.__cache = {}
    self.__deleteFunction = deleteFunction

  def exists( self, cKey, validSeconds = 0 ):
    """
    Returns True/False if the key exists for the given number of seconds
      Arguments:
        - cKey : identification key of the record
        - validSeconds : The amount of seconds the key has to be valid for
    """
    self.__lock.acquire( self.__class__.__name__ )
    try:
      #Is the key in the cache?
      if cKey in self.__cache:
        expTime = self.__cache[ cKey ][ 'expirationTime' ]
        #If it's valid return True!
        if expTime > datetime.datetime.now() + datetime.timedelta( seconds = validSeconds ):
          return True
        else:
          #Delete expired
          self.delete( cKey )
      return False
    finally:
      self.__lock.release( self.__class__.__name__ )

  def delete( self, cKey ):
    """
    Delete a key from the cache
      Arguments:
        - cKey : identification key of the record
    """
    self.__lock.acquire( self.__class__.__name__ )
    try:
      if cKey not in self.__cache:
        return
      if self.__deleteFunction:
        self.__deleteFunction( self.__cache[ cKey ][ 'value' ] )
      del( self.__cache[ cKey ] )
    finally:
      self.__lock.release( self.__class__.__name__ )

  def add( self, cKey, validSeconds, value = None ):
    """
    Add a record to the cache
      Arguments:
        - cKey : identification key of the record
        - validSeconds : valid seconds of this record
        - value : value of the record
    """
    if max( 0, validSeconds ) == 0:
      return
    self.__lock.acquire( self.__class__.__name__ )
    try:
      vD = { 'expirationTime' : datetime.datetime.now() + datetime.timedelta( seconds = validSeconds ),
             'value' : value }
      self.__cache[ cKey ] = vD
    finally:
      self.__lock.release( self.__class__.__name__ )

  def get( self, cKey, validSeconds = 0 ):
    """
    Get a record from the cache
      Arguments:
        - cKey : identification key of the record
        - validSeconds : The amount of seconds the key has to be valid for
    """
    self.__lock.acquire( self.__class__.__name__ )
    try:
      #Is the key in the cache?
      if cKey in self.__cache:
        expTime = self.__cache[ cKey ][ 'expirationTime' ]
        #If it's valid return True!
        if expTime > datetime.datetime.now() + datetime.timedelta( seconds = validSeconds ):
          return self.__cache[ cKey ][ 'value' ]
        else:
          #Delete expired
          self.delete( cKey )
      return False
    finally:
      self.__lock.release( self.__class__.__name__ )

  def showContentsInString( self ):
    """
    Return a human readable string to represent the contents
    """
    self.__lock.acquire( self.__class__.__name__ )
    try:
      data = []
      for cKey in self.__cache:
        data.append( "%s:" % str( cKey ) )
        data.append( "\tExp: %s" % self.__cache[ cKey ][ 'expirationTime' ] )
        if self.__cache[ cKey ][ 'value' ]:
          data.append( "\tVal: %s" % self.__cache[ cKey ][ 'value' ] )
      return "\n".join( data )
    finally:
      self.__lock.release( self.__class__.__name__ )

  def getKeys( self, validSeconds = 0 ):
    """
    Get keys for all contents
    """
    self.__lock.acquire( self.__class__.__name__ )
    try:
      keys = []
      limitTime = datetime.datetime.now() + datetime.timedelta( seconds = validSeconds )
      for cKey in self.__cache:
        if self.__cache[ cKey ][ 'expirationTime' ] > limitTime:
          keys.append( cKey )
      return keys
    finally:
      self.__lock.release( self.__class__.__name__ )

  def purgeExpired( self, expiredInSeconds = 0 ):
    """
    Purge all entries that are expired or will be expired in <expiredInSeconds>
    """
    self.__lock.acquire( self.__class__.__name__ )
    try:
      keys = []
      limitTime = datetime.datetime.now() + datetime.timedelta( seconds = expiredInSeconds )
      for cKey in self.__cache:
        if self.__cache[ cKey ][ 'expirationTime' ] < limitTime:
          keys.append( cKey )
      for cKey in keys:
        if self.__deleteFunction:
          self.__deleteFunction( self.__cache[ cKey ][ 'value' ] )
        del( self.__cache[ cKey ] )
    finally:
      self.__lock.release( self.__class__.__name__ )

  def purgeAll( self ):
    """
    Purge all entries
    """
    self.__lock.acquire( self.__class__.__name__ )
    try:
      keys = self.__cache.keys()
      for cKey in keys:
        if self.__deleteFunction:
          self.__deleteFunction( self.__cache[ cKey ][ 'value' ] )
        del( self.__cache[ cKey ] )
    finally:
      self.__lock.release( self.__class__.__name__)
