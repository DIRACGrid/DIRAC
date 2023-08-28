import threading
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer
from DIRAC.FrameworkSystem.Client.Logger import gLogger

gEventSync = Synchronizer()


class EventDispatcher:
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
            return S_ERROR(f"Event {eventName} is not registered")
        if functor in self.__events[eventName]:
            return S_OK()
        self.__events[eventName].append(functor)
        return S_OK()

    @gEventSync
    def removeListener(self, eventName, functor):
        if eventName not in self.__events:
            return S_ERROR(f"Event {eventName} is not registered")
        if functor not in self.__events[eventName]:
            return S_OK()
        iPos = self.__events[eventName].find(functor)
        del self.__events[eventName][iPos]
        return S_OK()

    def isEventBeingProcessed(self, eventName):
        return eventName in self.__processingEvents

    def getRegisteredEvents(self):
        return sorted(self.__events)

    def triggerEvent(self, eventName, params=False, threaded=False):
        if threaded:
            th = threading.Thread(target=self.__realTrigger, args=(eventName, params))
            th.daemon = True
            th.start()
            return S_OK(0)
        return self.__realTrigger(eventName, params)

    def __realTrigger(self, eventName, params):
        gEventSync.lock()
        try:
            if eventName not in self.__events:
                return S_ERROR(f"Event {eventName} is not registered")
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
                gLogger.exception(f"Listener {functor.__name__} for event {eventName} raised an exception")
                continue
            if not isinstance(result, dict) or "OK" not in result:
                gLogger.error(
                    "Listener for event did not return a S_OK/S_ERROR structure",
                    f"{functor.__name__} {eventName}",
                )
                continue
            if not result["OK"]:
                finalResult = result
                break
        gEventSync.lock()
        try:
            self.__processingEvents.discard(eventName)
        finally:
            try:
                gEventSync.unlock()
            except Exception:
                pass
        if not finalResult["OK"]:
            return finalResult
        return S_OK(len(eventFunctors))


gEventDispatcher = EventDispatcher()
