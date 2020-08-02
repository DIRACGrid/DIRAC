from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
    """ Reset extra information
    """
    self.__DN = False
    self.__ID = False
    self.__group = False
    self.__deco = False
    self.__setup = False

  def setDecorator(self, deco):
    """ Set decorator

        :param deco: decorator
    """
    self.__deco = deco

  def getDecorator(self):
    """ Return decorator

        :return: decorator
    """
    return self.__deco

  def setDN(self, DN):
    """ Set DN

        :param str DN: DN
    """
    self.__DN = DN

  def getDN(self):
    """ Return DN

        :return: str
    """
    return self.__DN

  def setGroup(self, group):
    """ Set group

        :param str group: group name
    """
    self.__group = group

  def getGroup(self):
    """ Return group name

        :return: str
    """
    return self.__group

  def setID(self, ID):
    """ Set user ID

        :param str ID: user ID
    """
    self.__ID = ID

  def getID(self):
    """ Return user ID

        :return: str
    """
    return self.__ID

  def setSetup(self, setup):
    """ Set setup name

        :param str setup: setup name
    """
    self.__setup = setup

  def getSetup(self):
    """ Return setup name

        :return: str
    """
    return self.__setup

  def dump(self):
    """ Return extra information

        :return: tuple
    """
    return (self.__DN, self.__group, self.__setup, self.__ID)

  def load(self, tp):
    """ Save extra information

        :param tuple tp: contain DN, group name, setup name, ID
    """
    self.__ID = tp[3] or self.__ID
    self.__DN = tp[0] or self.__DN
    self.__group = tp[1] or self.__group
    self.__setup = tp[2] or self.__setup


def threadDeco(method):
  """ Tread decorator

      :param method: method

      :return: wrapped method
  """
  tc = ThreadConfig()

  @functools.wraps(method)
  def wrapper(*args, **kwargs):
    """ Wrapper

        :return: wrapped method
    """
    deco = tc.getDecorator()
    if not deco:
      return method(*args, **kwargs)
    # Deco is a decorator sooo....
    return deco(method)(*args, **kwargs)

  return wrapper
