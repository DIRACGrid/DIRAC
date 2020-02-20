""" This contains unit tests to make sure that the migration between PyGSI and M2Crypto is as smooth as possible"""

import sys
import os

import importlib

from pytest import mark, fixture
parametrize = mark.parametrize


def deimportModule(modName):
  """ utility function to force a reimport of module

      :param modName: name of the module to remove
  """

  for mod in list(sys.modules):
    if mod == modName or mod.startswith('%s.' % modName):
      sys.modules.pop(mod)

  # from DIRAC.Core.Security.m2crypto.X509Chain import X509Chain, isPUSPdn
  # from DIRAC.Core.Security.m2crypto.X509CRL import X509CRL
  # from DIRAC.Core.Security.m2crypto.X509Request import X509Request
  #
  # from DIRAC.Core.Security.m2crypto.X509Certificate import X509Certificate, \
  #     DN_MAPPING, DOMAIN_COMPONENT_OID,\
  #     LIMITED_PROXY_OID, ORGANIZATIONAL_UNIT_NAME_OID,\
  #     VOMS_EXTENSION_OID, VOMS_FQANS_OID, VOMS_GENERIC_ATTRS_OID


@fixture(scope='function')
def set_env():
  """ Fixture to clean before and after the DIRAC import as well as the environment
    variable DIRAC_USE_M2CRYPTO
  """
  # Cleaning module and unsetting env before
  deimportModule('DIRAC')
  os.environ.pop('DIRAC_USE_M2CRYPTO', None)

  yield

  #  Cleaning module and unsetting env after
  deimportModule('DIRAC')
  os.environ.pop('DIRAC_USE_M2CRYPTO', None)


@parametrize('DIRAC_USE_M2CRYPTO', (None, 'NO', 'ANY', 'YES'))
@parametrize('x509Module', ('X509Chain', 'X509Certificate', 'X509Request', 'X509CRL'))
def test_dynamic_import(DIRAC_USE_M2CRYPTO, x509Module, set_env):
  """ Given various value of DIRAC_USE_M2CRYPTO and various
      class, make sure that the basic import still works.
      It basically is a test of the __init__.py in DIRAC.Core.Security
  """

  fullModuleName = 'DIRAC.Core.Security.' + x509Module

  # if DIRAC_USE_M2CRYPTO is None, we test the default case
  # where no env variable is set
  if DIRAC_USE_M2CRYPTO:
    os.environ['DIRAC_USE_M2CRYPTO'] = DIRAC_USE_M2CRYPTO

  expectedSubPackage = 'm2crypto'
  if DIRAC_USE_M2CRYPTO in ('ANY', 'NO'):
    expectedSubPackage = 'pygsi'

  importlib.import_module(fullModuleName)

  assert expectedSubPackage in sys.modules[fullModuleName].__file__
