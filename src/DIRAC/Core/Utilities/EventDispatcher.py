from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

import threading
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer
from DIRAC.FrameworkSystem.Client.Logger import gLogger

gEventSync = Synchronizer()


class EventDispatcher(object):

  def __init__(self):
    self.__events = {}
    self.__processingEvents = set()

  @gEventSync
  def registerEvent(self, eventName):
    if eventName in self.__events:
      return
    self.__events[eventName] = []

  @gEventSync
  def addListener(self, eventName, functor):
    if eventName not in self.__events:
      return S_ERROR("Event %s is not registered" % eventName)
    if functor in self.__events[eventName]:
      return S_OK()
    self.__events[eventName].append(functor)
    return S_OK()

  @gEventSync
  def removeListener(self, eventName, functor):
    if eventName not in self.__events:
      return S_ERROR("Event %s is not registered" % eventName)
    if functor not in self.__events[eventName]:
      return S_OK()
    iPos = self.__events[eventName].find(functor)
    del(self.__events[eventName][iPos])
    return S_OK()

  def isEventBeingProcessed(self, eventName):
    return eventName in self.__processingEvents

  def getRegisteredEvents(self):
    return sorted(self.__events)

  def triggerEvent(self, eventName, params=False, threaded=False):
    if threaded:
      th = threading.Thread(target=self.__realTrigger, args=(eventName, params))
      th.setDaemon(1)
      th.start()
      return S_OK(0)
    return self.__realTrigger(eventName, params)

  def __realTrigger(self, eventName, params):
    gEventSync.lock()
    try:
      if eventName not in self.__events:
        return S_ERROR("Event %s is not registered" % eventName)
      if eventName in self.__processingEvents:
        return S_OK(0)
      eventFunctors = list(self.__events[eventName])
      self.__processingEvents.add(eventName)
    finally:
      gEventSync.unlock()
    finalResult = S_OK()
    for functor in eventFunctors:
      try:
        result = functor(eventName, params)
      except Exception:
        gLogger.exception("Listener %s for event %s raised an exception" % (functor.__name__, eventName))
        continue
      if not isinstance(result, dict) or 'OK' not in result:
        gLogger.error(
            "Listener for event did not return a S_OK/S_ERROR structure", "%s %s" %
            (functor.__name__, eventName))
        continue
      if not result['OK']:
        finalResult = result
        break
    gEventSync.lock()
    try:
      self.__processingEvents.discard(eventName)
    finally:
      try:
        gEventSync.unlock()
      except BaseException:
        pass
    if not finalResult['OK']:
      return finalResult
    return S_OK(len(eventFunctors))


gEventDispatcher = EventDispatcher()
