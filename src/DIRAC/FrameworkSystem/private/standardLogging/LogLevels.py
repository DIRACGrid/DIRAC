"""
LogLevels wrapper
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import logging


class LogLevels(object):
  """
  Wrapper of the old LogLevels class.
  LogLevels is used to integrate custom levels to logging: verbose, notice and always.

  It is useful to make conversion string-integer.
  In fact, logging use only integers while the oldgLogger used strings, so we need a converter.
  Example: log.setLevel(logging.ALWAYS) in 'logging' become log.setLevel("always") in gLogger.
  We keep the string form because there are many and many calls with string levels.

  There is a method named getLevelName in 'logging' which could do almost the same job, but with logging,
  we can not return all level names in one time
  and the method getLevelName does not return errors or None values when a level
  does not exist. So at the moment, the LogLevels class is useful.
  """

  DEBUG = logging.DEBUG
  VERBOSE = 15
  INFO = logging.INFO
  WARN = logging.WARNING
  NOTICE = 35
  ERROR = logging.ERROR
  ALWAYS = 45
  FATAL = logging.CRITICAL

  __levelDict = {"DEBUG": DEBUG,
                 "VERBOSE": VERBOSE,
                 "INFO": INFO,
                 "WARN": WARN,
                 "NOTICE": NOTICE,
                 "ERROR": ERROR,
                 "ALWAYS": ALWAYS,
                 "FATAL": FATAL}

  @classmethod
  def getLevelValue(cls, sName):
    """
    Get a level value from a level name.
    We could use logging.getLevelName() to get the level value but it is less simple.

    :param str sName: level name
    :return: a level value according to a level name
    """
    return cls.__levelDict.get(sName.upper())

  @classmethod
  def getLevel(cls, level):
    """
    Get a level name from a level value.
    We could use logging.getLevelName() to get the level value but it is less simple.

    :param int level: level value
    :return: a level name according to a level value
    """
    for lev in cls.__levelDict:
      if cls.__levelDict[lev] == level:
        return lev
    return None

  @classmethod
  def getLevelNames(cls):
    """
    :return: all level names available in the wrapper
    """
    return list(cls.__levelDict)

  @classmethod
  def getLevels(cls):
    """
    :return: the level dictionary. Must no be redefined
    """
    return cls.__levelDict
