
import threading
import functools
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton

class ThreadConfig( threading.local ):
  __metaclass__ = DIRACSingleton

  def __init__( self ):
    self.reset()

  def reset( self ):
    self.__DN = False
    self.__group = False
    self.__async = False
    self.__deco = False

  def setDecorator( self, deco ):
    self.__deco = deco

  def getDecorator( self ):
    return self.__deco

  def setDN( self, DN ):
    self.__DN = DN

  def setGroup( self, group ):
    self.__group = group

  def setID( self, DN, group ):
    self.__DN = DN
    self.__group = group

  def getID( self ):
    return ( self.__DN, self.__group )



def threadDeco( method ):

  tc = ThreadConfig()

  @functools.wraps( method )
  def wrapper( *args, **kwargs ):
    """
    THIS IS SPARTAAAAAAAAAA
    """
    deco = tc.getDecorator()
    if not deco:
      return method( *args, **kwargs )
    #Deco is a decorator sooo....
    return deco( method )( *args, **kwargs )

  return wrapper
