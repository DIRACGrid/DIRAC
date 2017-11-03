""" Unit tests for the Dirac interface module
"""
# pylint: disable=no-member, protected-access

import unittest

from DIRAC.Interfaces.API.Dirac import Dirac


class DiracTestCases(unittest.TestCase):
  """ Dirac API test cases
  """
  def setUp(self):
    self.dirac = Dirac()

  def tearDown(self):
    pass

  def test_basicJob(self):
    jdl = "Parameter=Value;Parameter2=Value2"
    ret = self.dirac._Dirac__getJDLParameters(jdl)
    self.assertTrue(ret['OK'])
    self.assertIn('Parameter', ret['Value'])
    self.assertEqual('Value', ret['Value']['Parameter'])
    self.assertIn('Parameter2', ret['Value'])
    self.assertEqual('Value2', ret['Value']['Parameter2'])
