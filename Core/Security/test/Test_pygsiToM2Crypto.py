""" This contains unit tests to make sure that the migration between PyGSI and M2Crypto is as smooth as possible"""

import sys

from pytest import mark, approx, raises
parametrize = mark.parametrize


def deimportModule(modName):
  """ utility function to force a reimport of module

      :param modName: name of the module to remove
  """

  for mod in list(sys.modules):
    if mod == modName or mod.startswith('%s.' % modName):
      sys.modules.pop(mod)


def test_generate_proxy():
  pass
