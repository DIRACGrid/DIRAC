import threading
import functools
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton


class ThreadConfig(threading.local, metaclass=DIRACSingleton):
    """This class allows to contain extra information when a call is done on behalf of
    somebody else. Typically, when a host performs the request on behalf of a user.
    It is not used inside DIRAC, but is used in WebAppDIRAC for example

    Note that the class is a singleton, meaning that you share the same object in the whole process,
    however the attributes are thread locals (because of the threading.local inheritance).

    Also, this class has to be populated manually, no Client class will do it for you.
    """

    def __init__(self):
        self.reset()

    def reset(self):
        """Reset extra information"""
        self.__DN = False
        self.__group = False
        self.__deco = False
        self.__setup = False

    def setDecorator(self, deco):
        """Set decorator

        :param deco: decorator
        """
        self.__deco = deco

    def getDecorator(self):
        """Return decorator

        :return: decorator
        """
        return self.__deco

    def setDN(self, DN):
        """Set DN

        :param str DN: DN
        """
        self.__DN = DN

    def getDN(self):
        """Return DN

        :return: str
        """
        return self.__DN

    def setGroup(self, group):
        """Set group

        :param str group: group name
        """
        self.__group = group

    def getGroup(self):
        """Return group name

        :return: str
        """
        return self.__group

    def setID(self, DN, group):
        """Set user ID

        :param str DN: user DN
        :param str group: user group
        """
        self.__DN = DN
        self.__group = group

    def getID(self):
        """Return user ID

        :return: tuple
        """
        return (self.__DN, self.__group)

    def setSetup(self, setup):
        """Set setup name

        :param str setup: setup name
        """
        self.__setup = setup

    def getSetup(self):
        """Return setup name

        :return: str
        """
        return self.__setup

    def dump(self):
        """Return extra information

        :return: tuple
        """
        return (self.__DN, self.__group, self.__setup)

    def load(self, tp):
        """Save extra information

        :param tuple tp: contain DN, group name, setup name
        """
        if tp[0]:
            self.__DN = tp[0]
        if tp[1]:
            self.__group = tp[1]
        if tp[2]:
            self.__setup = tp[2]


def threadDeco(method):
    """Tread decorator

    :param method: method

    :return: wrapped method
    """
    tc = ThreadConfig()

    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        """Wrapper

        :return: wrapped method
        """
        deco = tc.getDecorator()
        if not deco:
            return method(*args, **kwargs)
        # Deco is a decorator sooo....
        return deco(method)(*args, **kwargs)

    return wrapper
