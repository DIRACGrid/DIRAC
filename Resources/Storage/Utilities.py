""" Storage plug-ins related utilities
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import errno

from DIRAC import S_OK, S_ERROR


def checkArgumentFormat(path):
  """ returns {'/this/is/an/lfn.1':False, '/this/is/an/lfn.2':False ...}
  """

  if isinstance(path, six.string_types):
    return S_OK({path: False})
  elif isinstance(path, list):
    return S_OK(dict([(url, False) for url in path if isinstance(url, six.string_types)]))
  elif isinstance(path, dict):
    returnDict = path.copy()
    return S_OK(returnDict)
  else:
    return S_ERROR(errno.EINVAL, "Utils.checkArgumentFormat: Supplied path is not of the correct format.")
