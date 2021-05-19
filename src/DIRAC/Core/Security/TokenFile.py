""" Collection of utilities for dealing with security files (i.e. token files)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import json
import stat
import tempfile

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.Locations import getTokenLocation


def readTokenFromFile(fileName=None):
  """ Read token from a file

      :param str fileName: filename to read

      :return: S_OK(dict)/S_ERROR()
  """
  if not fileName:
    fileName = getTokenLocation() or os.environ.get('DIRAC_TOKEN_FILE', "/tmp/JWTup_u%d" % os.getuid())
  try:
    with open(fileName, 'r') as f:
      data = f.read()
    return S_OK(json.loads(data))
  except Exception as e:
    return S_ERROR('Cannot read token.')


def writeToTokenFile(tokenContents, fileName=False):
  """ Write a token string to file

      :param str tokenContents: token as string
      :param str fileName: filename to dump to

      :return: S_OK(str)/S_ERROR()
  """
  if not fileName:
    try:
      fd, tokenLocation = tempfile.mkstemp()
      os.close(fd)
    except IOError:
      return S_ERROR(DErrno.ECTMPF)
    fileName = tokenLocation
  try:
    with open(fileName, 'wb') as fd:
      fd.write(tokenContents)
  except Exception as e:
    return S_ERROR(DErrno.EWF, " %s: %s" % (fileName, repr(e).replace(',)', ')')))
  try:
    os.chmod(fileName, stat.S_IRUSR | stat.S_IWUSR)
  except Exception as e:
    return S_ERROR(DErrno.ESPF, "%s: %s" % (fileName, repr(e).replace(',)', ')')))
  return S_OK(fileName)


def writeTokenDictToTokenFile(tokenDict, fileName=None):
  """ Write a token dict to file

      :param dict tokenDict: dict object to dump to file
      :param str fileName: filename to dump to

      :return: S_OK(str)/S_ERROR()
  """
  if not fileName:
    fileName = getTokenLocation() or os.environ.get('DIRAC_TOKEN_FILE', "/tmp/JWTup_u%d" % os.getuid())
  try:
    retVal = json.dumps(tokenDict)
  except Exception as e:
    return S_ERROR('Cannot read token.')
  return writeToTokenFile(retVal, fileName)


def writeTokenDictToTemporaryFile(tokenDict):
  """ Write a token dict to a temporary file

      :param dict tokenDict: dict object to dump to file

      :return: S_OK(str)/S_ERROR() -- contain file name
  """
  try:
    fd, tokenLocation = tempfile.mkstemp()
    os.close(fd)
  except IOError:
    return S_ERROR(DErrno.ECTMPF)
  retVal = writeTokenDictToTokenFile(tokenDict, tokenLocation)
  if not retVal['OK']:
    try:
      os.unlink(tokenLocation)
    except Exception:
      pass
    return retVal
  return S_OK(tokenLocation)
