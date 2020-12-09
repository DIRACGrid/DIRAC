from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import unittest

from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.DISET.RequestHandler import RequestHandler


class ObjectLoaderMainSuccessScenario(unittest.TestCase):

  def setUp(self):
    self.ol = ObjectLoader()

  def __check(self, result):
    if not result['OK']:
      self.fail(result['Message'])
    return result['Value']

  def test_load(self):
    self.__check(self.ol.loadObject("Core.Utilities.List", 'fromChar'))
    self.__check(self.ol.loadObject("Core.Utilities.ObjectLoader", "ObjectLoader"))
    dataFilter = self.__check(self.ol.getObjects("WorkloadManagementSystem.Service", ".*Handler"))
    dataClass = self.__check(self.ol.getObjects("WorkloadManagementSystem.Service", parentClass=RequestHandler))
    self.assertEqual(sorted(dataFilter), sorted(dataClass))

#############################################################################
# Test Suite run
#############################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ObjectLoaderMainSuccessScenario)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
