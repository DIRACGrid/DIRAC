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

  def test_JobJob(self):
    from DIRAC.Interfaces.API.Job import Job
    job = Job(stdout='printer', stderr='/dev/null')
    ret = self.dirac._Dirac__getJDLParameters(job)
    self.assertTrue(ret['OK'])
    self.assertEqual('printer', ret['Value']['StdOutput'])
    self.assertEqual('/dev/null', ret['Value']['StdError'])
