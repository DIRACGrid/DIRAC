""" Utility Class for threaded agents (e.g. TransformationAgent)
    Mostly for logging
"""
import time
from DIRAC import gLogger

AGENT_NAME = ""


class TransformationAgentsUtilities:
    """logging utilities for threaded TS agents"""

    def __init__(self):
        """c'tor"""
        self.debug = False

    def __prefixForLogging(self, transID, method, reftime):
        """get the thread number"""
        if reftime is not None:
            method += " (%.1f seconds)" % (time.time() - reftime)
        try:
            return "[%s] " % transID + AGENT_NAME + "." + method
        except NameError:
            return ""

    def _logVerbose(self, message, param="", method="execute", transID="None", reftime=None):
        """verbose"""
        if self.debug:
            gLogger.getSubLogger("(V) " + self.__prefixForLogging(transID, method, reftime)).info(message, param)
        else:
            gLogger.getSubLogger(self.__prefixForLogging(transID, method, reftime)).verbose(message, param)

    def _logDebug(self, message, param="", method="execute", transID="None", reftime=None):
        """debug"""
        gLogger.getSubLogger(self.__prefixForLogging(transID, method, reftime)).debug(message, param)

    def _logInfo(self, message, param="", method="execute", transID="None", reftime=None):
        """info"""
        gLogger.getSubLogger(self.__prefixForLogging(transID, method, reftime)).info(message, param)

    def _logWarn(self, message, param="", method="execute", transID="None", reftime=None):
        """warn"""
        gLogger.getSubLogger(self.__prefixForLogging(transID, method, reftime)).warn(message, param)

    def _logError(self, message, param="", method="execute", transID="None", reftime=None):
        """error"""
        gLogger.getSubLogger(self.__prefixForLogging(transID, method, reftime)).error(message, param)

    def _logException(self, message, param="", lException=False, method="execute", transID="None", reftime=None):
        """exception"""
        gLogger.getSubLogger(self.__prefixForLogging(transID, method, reftime)).exception(message, param, lException)

    def _logFatal(self, message, param="", method="execute", transID="None", reftime=None):
        """error"""
        gLogger.getSubLogger(self.__prefixForLogging(transID, method, reftime)).fatal(message, param)

    def _transTaskName(self, transID, taskID):  # pylint: disable=no-self-use
        """Construct the task name from the transformation and task ID"""
        return str(transID).zfill(8) + "_" + str(taskID).zfill(8)

    def _parseTaskName(self, taskName):  # pylint: disable=no-self-use
        """Split a task name into transformation and taskID"""
        try:
            return (int(x) for x in taskName.split("_"))
        except ValueError:
            return (0, 0)
