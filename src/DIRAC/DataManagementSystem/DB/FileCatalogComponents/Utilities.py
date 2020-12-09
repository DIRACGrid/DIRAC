""" DIRAC FileCatalog utilities
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString


def getIDSelectString(ids):
  """
  :param ids: input IDs - can be single int, list or tuple or a SELECT string
  :return: Select string
  """
  if isinstance(ids, six.string_types) and ids.lower().startswith('select'):
    idString = ids
  elif isinstance(ids, six.integer_types):
    idString = '%d' % ids
  elif isinstance(ids, (tuple, list)):
    idString = intListToString(ids)
  else:
    return S_ERROR('Illegal fileID')

  return S_OK(idString)
