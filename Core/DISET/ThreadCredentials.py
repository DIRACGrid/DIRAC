
import threading
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton

class ThreadCredentials( threading.local ):
  __metaclass__ = DIRACSingleton

  def __init__( self ):
    self.reset()

  def reset( self ):
    self.__DN = False
    self.__group = False

  def setDN( self, DN ):
    self.__DN = DN

  def setGroup( self, group ):
    self.__group = group

  def setID( self, DN, group ):
    self.__DN = DN
    self.__group = group

  def getID( self ):
    return ( self.__DN, self.__group )

