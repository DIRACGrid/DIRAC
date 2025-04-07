""" DIRAC API Base Class """

import pprint
import sys

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
from DIRAC.Core.Security.ProxyInfo import formatProxyInfoAsString, getProxyInfo
from DIRAC.Core.Utilities.Version import getCurrentVersion


def _printFormattedDictList(dictList, fields, uniqueField, orderBy):
    """Will print ordered the supplied field of a list of dictionaries

    :param list dictList: list of dictionaries
    :param list fields: fields
    :param str uniqueField: unique field
    :param str orderBy: ordered
    """
    orderDict = {}
    fieldWidths = {}
    dictFields = {}
    for myDict in dictList:
        for field in fields:
            fieldValue = myDict[field]
            if field not in fieldWidths:
                fieldWidths[field] = len(str(field))
            if len(str(fieldValue)) > fieldWidths[field]:
                fieldWidths[field] = len(str(fieldValue))
        orderValue = myDict[orderBy]
        if orderValue not in orderDict:
            orderDict[orderValue] = []
        orderDict[orderValue].append(myDict[uniqueField])
        dictFields[myDict[uniqueField]] = myDict
    headString = f"{fields[0].ljust(fieldWidths[fields[0]] + 5)}"
    for field in fields[1:]:
        headString = f"{headString} {field.ljust(fieldWidths[field] + 5)}"
    print(headString)
    for orderValue in sorted(orderDict):
        uniqueFields = orderDict[orderValue]
        for uniqueField in sorted(uniqueFields):
            myDict = dictFields[uniqueField]
            outStr = f"{str(myDict[fields[0]]).ljust(fieldWidths[fields[0]] + 5)}"
            for field in fields[1:]:
                outStr = f"{outStr} {str(myDict[field]).ljust(fieldWidths[field] + 5)}"
            print(outStr)


# TODO: some of these can just be functions, and moved out of here


class API:
    """An utilities class for APIs"""

    #############################################################################

    def __init__(self):
        """c'tor"""
        self._printFormattedDictList = _printFormattedDictList
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.pPrint = pprint.PrettyPrinter()
        # Global error dictionary
        self.errorDict = {}
        self.diracInfo = getCurrentVersion()["Value"]
        self._siteSet = set(getSites().get("Value", []))

    #############################################################################

    def __getstate__(self):
        """Return a copied dictionary containing all the attributes of the API.
            Called when pickling the object. Also used in copy.deepcopy.

        :return: dictionary of attributes
        """
        from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging

        state = dict(self.__dict__)
        # Replace the Logging instance by its name because it is not copyable
        # because of the thread locks
        if isinstance(state["log"], Logging):
            state["log"] = state["log"].getSubName()
        # In Python 3 PrettyPrinter can't be pickled
        del state["pPrint"]
        return state

    def __setstate__(self, state):
        """Parameter the Job with an attributes dictionary.
            Called when un-pickling the object.

        :params state: attributes dictionary
        """
        self.__dict__.update(state)
        # Build the Logging instance again because it can not be in the dictionary
        # due to the thread locks
        if isinstance(state["log"], str):
            self.log = gLogger.getSubLogger(state["log"])
        self.pPrint = pprint.PrettyPrinter()

    #############################################################################

    def _errorReport(self, error, message=None):
        """Internal function to return errors and exit with an S_ERROR()

        :param str error: error
        :param str message: message

        :return: S_ERROR(str)
        """
        if not message:
            message = error

        self.log.warn(error)
        return S_ERROR(message)

    #############################################################################

    def _prettyPrint(self, myObject):
        """Helper function to pretty print an object.

        :param object myObject: an object to pring
        """
        print(self.pPrint.pformat(myObject))

    #############################################################################

    def _checkSiteIsValid(self, site):
        """Internal function to check that a site name is valid."""
        if isinstance(site, (list, set, dict)):
            site = set(site) - self._siteSet
            if not site:
                return S_OK()
        elif site in self._siteSet:
            return S_OK()
        return self._reportError(f"Specified site {str(site)} is not in list of defined sites")

    #############################################################################

    def _getCurrentUser(self):
        """Get current user

        :return: S_OK(dict)/S_ERROR()
        """
        res = getProxyInfo(False, False)
        if not res["OK"]:
            return self._errorReport("No proxy found in local environment", res["Message"])
        proxyInfo = res["Value"]
        gLogger.debug(formatProxyInfoAsString(proxyInfo))
        if "group" not in proxyInfo:
            return self._errorReport("Proxy information does not contain the group", res["Message"])
        res = getDNForUsername(proxyInfo["username"])
        if not res["OK"]:
            return self._errorReport("Failed to get proxies for user", res["Message"])
        return S_OK(proxyInfo["username"])

    #############################################################################

    def _reportError(self, message, name="", **kwargs):
        """Internal Function. Gets caller method name and arguments, formats the
        information and adds an error to the global error dictionary to be
        returned to the user.

        :param str message: message
        :param str name: name

        :return: S_ERROR(str)
        """
        className = name
        if not name:
            className = __name__
        methodName = sys._getframe(1).f_code.co_name
        arguments = []
        for key in kwargs:
            if kwargs[key]:
                arguments.append(f"{key} = {kwargs[key]} ( {type(kwargs[key])} )")
        finalReport = f"""Problem with {className}.{methodName}() call:
Arguments: {'/'.join(arguments)}
Message: {message}
"""
        if methodName in self.errorDict:
            tmp = self.errorDict[methodName]
            tmp.append(finalReport)
            self.errorDict[methodName] = tmp
        else:
            self.errorDict[methodName] = [finalReport]
        self.log.verbose(finalReport)
        return S_ERROR(finalReport)
