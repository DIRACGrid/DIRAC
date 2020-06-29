""" This is a test of the parametric job generation tools
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest

from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import generateParametricJobs, \
    getParameterVectorLength
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd

TEST_JDL_NO_PARAMETERS = """
[
  Executable = "my_executable";
  Arguments = "%s";
  JobName = "Test_%n";
]
"""

TEST_JDL_SIMPLE = """
[
  Executable = "my_executable";
  Arguments = "%s";
  JobName = "Test_%n";
  Parameters = { "a", "b", "c" }
]
"""

TEST_JDL_SIMPLE_BUNCH = """
[
  Executable = "my_executable";
  Arguments = "%s";
  JobName = "Test_%n";
  Parameters = 3;
  ParameterStart = 5;
]
"""

TEST_JDL_SIMPLE_PROGRESSION = """
[
  Executable = "my_executable";
  Arguments = "%s";
  JobName = "Test_%n";
  Parameters = 3;
  ParameterStart = 1;
  ParameterStep = 1;
  ParameterFactor = 2;
]
"""

TEST_JDL_MULTI = """
[
  Executable = "my_executable";
  Arguments = "%(A)s %(B)s";
  JobName = "Test_%n";
  Parameters = 3;
  ParameterStart.A = 1;
  ParameterStep.A = 1;
  ParameterFactor.A = 2;
  Parameters.B = { "a","b","c" };
]
"""

TEST_JDL_MULTI_BAD = """
[
  Executable = "my_executable";
  Arguments = "%(A)s %(B)s";
  JobName = "Test_%n";
  Parameters = 3;
  ParameterStart.A = 1;
  ParameterStep.A = 1;
  ParameterFactor.A = 2;
  Parameters.B = { "a","b","c","d" };
]
"""


class TestParametricUtilityCase(unittest.TestCase):

  def test_Simple(self):

    clad = ClassAd(TEST_JDL_SIMPLE)
    result = getParameterVectorLength(clad)
    self.assertTrue(result['OK'])
    nParam = result['Value']

    self.assertEqual(nParam, 3)

    result = generateParametricJobs(clad)
    self.assertTrue(result['OK'])

    jobDescList = result['Value']
    self.assertEqual(nParam, len(jobDescList))

    # Check the definition of the 2nd job
    jobClassAd = ClassAd(jobDescList[1])
    self.assertEqual(jobClassAd.getAttributeString('Arguments'), 'b')
    self.assertEqual(jobClassAd.getAttributeString('JobName'), 'Test_1')

  def test_SimpleBunch(self):

    clad = ClassAd(TEST_JDL_SIMPLE_BUNCH)
    result = getParameterVectorLength(clad)
    self.assertTrue(result['OK'])
    nParam = result['Value']

    self.assertEqual(nParam, 3)

    result = generateParametricJobs(clad)
    self.assertTrue(result['OK'])

    jobDescList = result['Value']
    self.assertEqual(nParam, len(jobDescList))

    # Check the definition of the 2nd job
    jobClassAd = ClassAd(jobDescList[1])
    self.assertEqual(jobClassAd.getAttributeString('Arguments'), '5')
    self.assertEqual(jobClassAd.getAttributeString('JobName'), 'Test_1')

  def test_SimpleProgression(self):

    clad = ClassAd(TEST_JDL_SIMPLE_PROGRESSION)
    result = getParameterVectorLength(clad)
    self.assertTrue(result['OK'])
    nParam = result['Value']

    self.assertEqual(nParam, 3)

    result = generateParametricJobs(clad)
    self.assertTrue(result['OK'])

    jobDescList = result['Value']
    self.assertEqual(nParam, len(jobDescList))

    # Check the definition of the 2nd job
    jobClassAd = ClassAd(jobDescList[1])
    self.assertEqual(jobClassAd.getAttributeString('Arguments'), '3')
    self.assertEqual(jobClassAd.getAttributeString('JobName'), 'Test_1')

  def test_Multi(self):

    clad = ClassAd(TEST_JDL_MULTI)
    result = getParameterVectorLength(clad)
    self.assertTrue(result['OK'])
    nParam = result['Value']

    self.assertEqual(nParam, 3)

    result = generateParametricJobs(clad)
    self.assertTrue(result['OK'])

    jobDescList = result['Value']
    self.assertEqual(nParam, len(jobDescList))

    # Check the definition of the 2nd job
    jobClassAd = ClassAd(jobDescList[1])
    self.assertEqual(jobClassAd.getAttributeString('Arguments'), '3 b')
    self.assertEqual(jobClassAd.getAttributeString('JobName'), 'Test_1')

  def test_MultiBad(self):

    clad = ClassAd(TEST_JDL_MULTI_BAD)
    result = getParameterVectorLength(clad)
    self.assertTrue(not result['OK'])

  def test_NoParameters(self):

    clad = ClassAd(TEST_JDL_NO_PARAMETERS)
    result = getParameterVectorLength(clad)
    self.assertTrue(result['OK'])
    nParam = result['Value']
    self.assertTrue(nParam is None)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestParametricUtilityCase)
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( xxxx ) )

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
