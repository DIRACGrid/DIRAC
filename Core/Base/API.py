""" DIRAC API Base Class """

from __future__ import print_function
import six
import pprint
import sys

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo import getProxyInfo, formatProxyInfoAsString
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNsForUsername
from DIRAC.Core.Utilities.Version import getCurrentVersion

__RCSID__ = '$Id$'

COMPONENT_NAME = 'API'


def _printFormattedDictList(dictList, fields, uniqueField, orderBy):
  """ Will print ordered the supplied field of a list of dictionaries

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
  headString = "%s" % fields[0].ljust(fieldWidths[fields[0]] + 5)
  for field in fields[1:]:
    headString = "%s %s" % (headString, field.ljust(fieldWidths[field] + 5))
  print(headString)
  for orderValue in sorted(orderDict.keys()):
    uniqueFields = orderDict[orderValue]
    for uniqueField in sorted(uniqueFields):
      myDict = dictFields[uniqueField]
      outStr = "%s" % str(myDict[fields[0]]).ljust(fieldWidths[fields[0]] + 5)
      for field in fields[1:]:
        outStr = "%s %s" % (outStr, str(myDict[field]).ljust(fieldWidths[field] + 5))
      print(outStr)


# TODO: some of these can just be functions, and moved out of here

class API(object):
  """ An utilities class for APIs
  """

  #############################################################################

  def __init__(self):
    """ c'tor
    """
    self._printFormattedDictList = _printFormattedDictList
    self.log = gLogger.getSubLogger(COMPONENT_NAME)
    self.section = COMPONENT_NAME
    self.pPrint = pprint.PrettyPrinter()
    # Global error dictionary
    self.errorDict = {}
    self.setup = gConfig.getValue('/DIRAC/Setup', 'Unknown')
    self.diracInfo = getCurrentVersion()['Value']

  #############################################################################

  def __getstate__(self):
    """ Return a copied dictionary containing all the attributes of the API.
        Called when pickling the object. Also used in copy.deepcopy.

        :return: dictionary of attributes
    """
    from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging
    state = dict(self.__dict__)
    # Replace the Logging instance by its name because it is not copyable
    # because of the thread locks
    if isinstance(state['log'], Logging):
      state['log'] = state['log'].getSubName()
    return state

  def __setstate__(self, state):
    """ Parameter the Job with an attributes dictionary.
        Called when un-pickling the object.

        :params state: attributes dictionary
    """
    self.__dict__.update(state)
    # Build the Logging instance again because it can not be in the dictionary
    # due to the thread locks
    if isinstance(state['log'], six.string_types):
      self.log = gLogger.getSubLogger(state['log'])

  #############################################################################

  def _errorReport(self, error, message=None):
    """ Internal function to return errors and exit with an S_ERROR()

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
    """ Helper function to pretty print an object.

        :param myObject: an object
    """
    print(self.pPrint.pformat(myObject))

  #############################################################################

  def _getCurrentUser(self):
    """ Get current user

        :return: S_OK(dict)/S_ERROR()
    """
    res = getProxyInfo(False, False)
    if not res['OK']:
      return self._errorReport('No proxy found in local environment', res['Message'])
    proxyInfo = res['Value']
    gLogger.debug(formatProxyInfoAsString(proxyInfo))
    if 'group' not in proxyInfo:
      return self._errorReport('Proxy information does not contain the group', res['Message'])
    result = getDNsForUsername(proxyInfo['username'])
    if not result['OK']:
      return self._errorReport('Failed to get proxies for user', result['Message'])
    if not result['Value']:
      return self._errorReport('Failed to get proxies for user', "No DNs found for %s" % proxyInfo['username'])
    return S_OK(proxyInfo['username'])

  #############################################################################

  def _reportError(self, message, name='', **kwargs):
    """ Internal Function. Gets caller method name and arguments, formats the
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
        arguments.append('%s = %s ( %s )' % (key, kwargs[key], type(kwargs[key])))
    finalReport = """Problem with %s.%s() call:
Arguments: %s
Message: %s
""" % (className, methodName, '/'.join(arguments), message)
    if methodName in self.errorDict:
      tmp = self.errorDict[methodName]
      tmp.append(finalReport)
      self.errorDict[methodName] = tmp
    else:
      self.errorDict[methodName] = [finalReport]
    self.log.verbose(finalReport)
    return S_ERROR(finalReport)
