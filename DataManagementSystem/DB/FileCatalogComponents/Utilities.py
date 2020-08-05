""" DIRAC FileCatalog utilities
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString


def getIDSelectString(ids):
  """
  :param ids: input IDs - can be single int, list or tuple or a SELECT string
  :return: Select string
  """
  if isinstance(ids, basestring) and ids.lower().startswith('select'):
    idString = ids
  elif isinstance(ids, (int, long)):
    idString = '%d' % ids
  elif isinstance(ids, (tuple, list)):
    idString = intListToString(ids)
  else:
    return S_ERROR('Illegal fileID')

  return S_OK(idString)
