""" This is a test of the creation of the json dump file
"""

import unittest

from DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer import PilotCStoJSONSynchronizer


class PilotCStoJSONSynchronizerTestCase(unittest.TestCase):
  """ Base class for the PilotCStoJSONSynchronizer test cases
  """

  def setUp(self):
    pass

  def tearDown(self):
    pass


class Test_PilotCStoJSONSynchronizer_getDNs(PilotCStoJSONSynchronizerTestCase):

  def test_succes(self):
    # res = pilotWrapperScript()
    pass


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PilotCStoJSONSynchronizerTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_PilotCStoJSONSynchronizer_getDNs))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
