"""
   DIRAC return dictionary

   Message values are converted to string

   keys are converted to string
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import traceback
from DIRAC.Core.Utilities.DErrno import strerror


def S_ERROR(*args, **kwargs):
  """ return value on error condition

  Arguments are either Errno and ErrorMessage or just ErrorMessage fro backward compatibility

  :param int errno: Error number
  :param string message: Error message
  :param list callStack: Manually override the CallStack attribute better performance
  """
  callStack = kwargs.pop("callStack", None)

  result = {"OK": False, "Errno": 0, "Message": ""}

  message = ''
  if args:
    if isinstance(args[0], six.integer_types):
      result['Errno'] = args[0]
      if len(args) > 1:
        message = args[1]
    else:
      message = args[0]

  if result['Errno']:
    message = "%s ( %s : %s)" % (strerror(result['Errno']), result['Errno'], message)
  result["Message"] = message

  if callStack is None:
    try:
      callStack = traceback.format_stack()
      callStack.pop()
    except BaseException:
      callStack = []

  result["CallStack"] = callStack

  # print "AT >>> S_ERROR", result['OK'], result['Errno'], result['Message']
  # for item in result['CallStack']:
  #  print item

  return result


def S_OK(value=None):
  """ return value on success

  :param value: value of the 'Value'
  :return: dictionary { 'OK' : True, 'Value' : value }
  """
  return {'OK': True, 'Value': value}


def isReturnStructure(unk):

  if not isinstance(unk, dict):
    return False
  if 'OK' not in unk:
    return False
  if unk['OK']:
    if 'Value' not in unk:
      return False
  else:
    if 'Message' not in unk:
      return False
  return True


def reprReturnErrorStructure(struct, full=False):
  errorNumber = struct.get("Errno", 0)
  message = struct.get("Message", '')
  if errorNumber:
    reprStr = "%s ( %s : %s)" % (strerror(errorNumber), errorNumber, message)
  else:
    reprStr = message

  if full:
    callStack = struct.get("CallStack")
    if callStack:
      reprStr += "\n" + "".join(callStack)

  return reprStr


def returnSingleResult(dictRes):
  """ Transform the S_OK{Successful/Failed} dictionary convention into
      an S_OK/S_ERROR return. To be used when a single returned entity
      is expected from a generally bulk call.

      :param dictRes: S_ERROR or S_OK( "Failed" : {}, "Successful" : {})
      :returns: S_ERROR or S_OK(value)

      The following rules are applied:

      - if dictRes is an S_ERROR: returns it as is
      - we start by looking at the Failed directory
      - if there are several items in a dictionary, we return the first one
      - if both dictionaries are empty, we return S_ERROR
      - For an item in Failed, we return S_ERROR
      - Far an item in Successful we return S_OK

      Behavior examples (would be perfect unit test :-) )::

        {'Message': 'Kaput', 'OK': False} -> {'Message': 'Kaput', 'OK': False}
        {'OK': True, 'Value': {'Successful': {}, 'Failed': {'a': 1}}} -> {'Message': '1', 'OK': False}
        {'OK': True, 'Value': {'Successful': {'b': 2}, 'Failed': {}}} -> {'OK': True, 'Value': 2}
        {'OK': True, 'Value': {'Successful': {'b': 2}, 'Failed': {'a': 1}}} -> {'Message': '1', 'OK': False}
        {'OK': True, 'Value': {'Successful': {'b': 2}, 'Failed': {'a': 1, 'c': 3}}} -> {'Message': '1', 'OK': False}
        {'OK': True, 'Value': {'Successful': {'b': 2, 'd': 4}, 'Failed': {}}} -> {'OK': True, 'Value': 2}
        {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}} ->
            {'Message': 'returnSingleResult: Failed and Successful dictionaries are empty', 'OK': False}
   """

  # if S_ERROR was returned, we return it as well
  if not dictRes['OK']:
    return dictRes
  # if there is a Failed, we return the first one in an S_ERROR
  if "Failed" in dictRes['Value'] and len(dictRes['Value']['Failed']):
    errorMessage = list(dictRes['Value']['Failed'].values())[0]
    if isinstance(errorMessage, dict):
      if isReturnStructure(errorMessage):
        return errorMessage
      else:
        return S_ERROR(str(errorMessage))
    return S_ERROR(errorMessage)
  # if there is a Successful, we return the first one in an S_OK
  elif "Successful" in dictRes['Value'] and len(dictRes['Value']['Successful']):
    return S_OK(list(dictRes['Value']['Successful'].values())[0])
  else:
    return S_ERROR("returnSingleResult: Failed and Successful dictionaries are empty")
