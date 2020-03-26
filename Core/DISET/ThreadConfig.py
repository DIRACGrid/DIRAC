
import threading
import functools
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton


class ThreadConfig(threading.local):
  """ This class allows to contain extra information when a call is done on behalf of
      somebody else. Typically, when a host performs the request on behalf of a user.
      It is not used inside DIRAC, but is used in WebAppDIRAC for example

      Note that the class is a singleton, meaning that you share the same object in the whole process,
      however the attributes are thread locals (because of the threading.local inheritance).

      Also, this class has to be populated manually, no Client class will do it for you.

  """

  __metaclass__ = DIRACSingleton

  def __init__(self):
    self.reset()

  def reset(self):
    self.__DN = False
    self.__group = False
    self.__deco = False
    self.__setup = False

  def setDecorator(self, deco):
    self.__deco = deco

  def getDecorator(self):
    return self.__deco

  def setDN(self, DN):
    self.__DN = DN

  def getDN(self):
    return self.__DN

  def setGroup(self, group):
    self.__group = group

  def getGroup(self):
    return self.__group

  def setID(self, DN, group):
    self.__DN = DN
    self.__group = group

  def getID(self):
    return (self.__DN, self.__group)

  def setSetup(self, setup):
    self.__setup = setup

  def getSetup(self):
    return self.__setup

  def dump(self):
    return (self.__DN, self.__group, self.__setup)

  def load(self, tp):
    if tp[0]:
      self.__DN = tp[0]
    if tp[1]:
      self.__group = tp[1]
    if tp[2]:
      self.__setup = tp[2]


def threadDeco(method):

  tc = ThreadConfig()

  @functools.wraps(method)
  def wrapper(*args, **kwargs):
    """
    THIS IS SPARTAAAAAAAAAA
    """
    deco = tc.getDecorator()
    if not deco:
      return method(*args, **kwargs)
    # Deco is a decorator sooo....
    return deco(method)(*args, **kwargs)

  return wrapper
