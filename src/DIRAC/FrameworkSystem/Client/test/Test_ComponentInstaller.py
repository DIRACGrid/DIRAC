""" (pytest) unit test for ComponentInstall.py
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.FrameworkSystem.Client.ComponentInstaller import ComponentInstaller

ci = ComponentInstaller()


def test_getAvailableDatabases():
  res = ci.getAvailableDatabases([])
  assert res['OK'] is True
  assert 'JobDB' in res['Value']
  assert res['Value']['JobDB']['System'] == 'WorkloadManagement'


def test_getSoftwareComponents():
  res = ci.getSoftwareComponents([])
  assert res['OK'] is True
  assert 'Services' in res['Value']
