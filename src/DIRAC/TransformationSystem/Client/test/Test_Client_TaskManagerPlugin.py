""" unit tests for Transformation Clients
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=missing-docstring,blacklisted-name,invalid-name

import unittest
import importlib
from mock import MagicMock

from DIRAC import S_OK, gLogger
from DIRAC.TransformationSystem.Client.TaskManagerPlugin import TaskManagerPlugin


class opsHelperFakeUser(object):
  def getValue(self, foo='', bar=''):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3']
    # This is for 'JobTypeMapping/<jobType>/AutoAddedSites
    elif foo.endswith('AutoAddedSites'):
      return bar
    # and this is for 'JobTypeMapping/<jobType>/Exclude
    return ['PAK', 'Ferrara', 'Bologna', 'Paris', 'Hospital']

  def getOptionsDict(self, foo=''):
    # The only call to this method is for /JobTypeMapping/<type>/Allow
    return {'OK': True, 'Value': {'Paris': 'IN2P3'}}


class opsHelperFakeUser2(object):
  def getValue(self, foo='', bar=''):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ''
    # This is for 'JobTypeMapping/<jobType>/AutoAddedSites
    elif foo.endswith('AutoAddedSites'):
      return bar
    # and this is for 'JobTypeMapping/<jobType>/Exclude
    return ['PAK', 'Ferrara', 'Bologna', 'Paris', 'CERN', 'IN2P3', 'Hospital']

  def getOptionsDict(self, foo=''):
    return {'OK': True, 'Value': {'Paris': 'IN2P3', 'CERN': 'CERN', 'IN2P3': 'IN2P3'}}


class opsHelperFakeDataReco(object):
  def getValue(self, foo='', bar=''):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3']
    # This is for 'JobTypeMapping/<jobType>/AutoAddedSites
    elif foo.endswith('AutoAddedSites'):
      return bar
    # and this is for 'JobTypeMapping/<jobType>/Exclude
    return ['PAK', 'Ferrara', 'CERN', 'IN2P3', 'Hospital']

  def getOptionsDict(self, foo=''):
    return {'OK': True, 'Value': {'Ferrara': 'CERN', 'IN2P3': 'IN2P3, CERN'}}


class opsHelperFakeHospital(object):
  def getValue(self, foo='', bar=''):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3']
    # This is for 'JobTypeMapping/<jobType>/AutoAddedSites
    elif foo.endswith('AutoAddedSites'):
      return []
    # and this is for 'JobTypeMapping/<jobType>/Exclude
    return ['ALL']

  def getOptionsDict(self, foo=''):
    return {'OK': True, 'Value': {'Hospital': 'CERN, IN2P3'}}


class opsHelperFakeMerge(object):
  def getValue(self, foo='', bar=''):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3', 'Hospital']
    # This is for 'JobTypeMapping/<jobType>/AutoAddedSites
    elif foo.endswith('AutoAddedSites'):
      return bar
    # and this is for 'JobTypeMapping/<jobType>/Exclude
    return ['ALL']

  def getOptionsDict(self, foo=''):
    return {'OK': False, 'Message': 'JobTypeMapping/MCSimulation/Allow in Operations does not exist'}


class opsHelperFakeMerge2(object):
  def getValue(self, foo='', bar=''):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ''
    # This is for 'JobTypeMapping/<jobType>/AutoAddedSites
    elif foo.endswith('AutoAddedSites'):
      return bar
    # and this is for 'JobTypeMapping/<jobType>/Exclude
    return ['ALL']

  def getOptionsDict(self, foo=''):
    return {'OK': True, 'Value': {'CERN': 'CERN', 'IN2P3': 'IN2P3'}}


class opsHelperFakeMC(object):
  def getValue(self, foo='', bar=''):
    if foo == 'JobTypeMapping/AutoAddedSites':
      return ['CERN', 'IN2P3']
    # This is for 'JobTypeMapping/<jobType>/AutoAddedSites
    elif foo.endswith('AutoAddedSites'):
      return bar
    # and this is for 'JobTypeMapping/<jobType>/Exclude
    return ['']

  def getOptionsDict(self, foo=''):
    return {'OK': False, 'Message': 'JobTypeMapping/MCSimulation/Allow in Operations does not exist'}


def getSitesFake():
  return S_OK(['Ferrara', 'Bologna', 'Paris', 'CSCS', 'PAK', 'CERN', 'IN2P3', 'Hospital'])


def getSitesForSE(ses):
  if ses == ['CERN-DST'] or ses == 'CERN-DST':
    return S_OK(['CERN'])
  elif ses == ['IN2P3-DST'] or ses == 'IN2P3-DST':
    return S_OK(['IN2P3'])
  elif ses == ['CSCS-DST'] or ses == 'CSCS-DST':
    return S_OK(['CSCS'])
  elif ses == ['CERN-DST', 'CSCS-DST'] or ses == 'CERN-DST,CSCS-DST':
    return S_OK(['CERN', 'CSCS'])

#############################################################################


class ClientsTestCase(unittest.TestCase):
  """ Base class for the clients test cases
  """

  def setUp(self):

    gLogger.setLevel('DEBUG')

    self.mockTransClient = MagicMock()
    self.mockTransClient.setTaskStatusAndWmsID.return_value = {'OK': True}

    self.WMSClientMock = MagicMock()
    self.jobMonitoringClient = MagicMock()
    self.mockReqClient = MagicMock()

    self.jobMock = MagicMock()
    self.jobMock2 = MagicMock()
    mockWF = MagicMock()
    mockPar = MagicMock()
    mockWF.findParameter.return_value = mockPar
    mockPar.getValue.return_value = 'MySite'

    self.jobMock2.workflow = mockWF
    self.jobMock2.setDestination.return_value = {'OK': True}
    self.jobMock.workflow.return_value = ''
    self.jobMock.return_value = self.jobMock2

    self.maxDiff = None

  def tearDown(self):
    pass

#############################################################################


class TaskManagerPluginSuccess(ClientsTestCase):

  def test__BySE(self):

    ourPG = importlib.import_module('DIRAC.TransformationSystem.Client.TaskManagerPlugin')
    ourPG.getSitesForSE = getSitesForSE
    p_o = TaskManagerPlugin('BySE', operationsHelper=MagicMock())

    p_o.params = {'Site': '', 'TargetSE': ''}
    res = p_o.run()
    self.assertEqual(res, set([]))

    p_o.params = {'Site': 'ANY', 'TargetSE': ''}
    res = p_o.run()
    self.assertEqual(res, set([]))

    p_o.params = {'TargetSE': 'Unknown'}
    res = p_o.run()
    self.assertEqual(res, set([]))

    p_o.params = {'Site': 'Site1;Site2', 'TargetSE': ''}
    res = p_o.run()
    self.assertEqual(res, set([]))

    p_o.params = {'TargetSE': 'CERN-DST'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN']))

    p_o.params = {'TargetSE': 'IN2P3-DST'}
    res = p_o.run()
    self.assertEqual(res, set(['IN2P3']))

    p_o.params = {'TargetSE': 'CERN-DST,CSCS-DST'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN', 'CSCS']))

    p_o.params = {'TargetSE': ['CERN-DST', 'CSCS-DST']}
    res = p_o.run()
    self.assertEqual(res, set(['CERN', 'CSCS']))

  def test__ByJobType(self):

    ourPG = importlib.import_module('DIRAC.TransformationSystem.Client.TaskManagerPlugin')
    ourPG.getSites = getSitesFake
    ourPG.getSitesForSE = getSitesForSE

    # "User" case - 1
    p_o = TaskManagerPlugin('ByJobType', operationsHelper=opsHelperFakeUser())

    p_o.params = {'Site': '', 'TargetSE': 'CERN-DST', 'JobType': 'User'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN', 'CSCS', 'IN2P3']))

    p_o.params = {'Site': '', 'TargetSE': 'IN2P3-DST', 'JobType': 'User'}
    res = p_o.run()
    self.assertEqual(res, set(['IN2P3', 'Paris', 'CSCS', 'CERN']))

    p_o.params = {'Site': '', 'TargetSE': ['CERN-DST', 'CSCS-DST'], 'JobType': 'User'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN', 'CSCS', 'IN2P3']))

    # "User" case - 2
    p_o = TaskManagerPlugin('ByJobType', operationsHelper=opsHelperFakeUser2())

    p_o.params = {'Site': '', 'TargetSE': 'CERN-DST', 'JobType': 'User'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN', 'CSCS']))

    p_o.params = {'Site': '', 'TargetSE': 'IN2P3-DST', 'JobType': 'User'}
    res = p_o.run()
    self.assertEqual(res, set(['IN2P3', 'Paris', 'CSCS']))

    p_o.params = {'Site': '', 'TargetSE': ['CERN-DST', 'CSCS-DST'], 'JobType': 'User'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN', 'CSCS']))

    # "DataReconstruction" case
    p_o = TaskManagerPlugin('ByJobType', operationsHelper=opsHelperFakeDataReco())

    p_o.params = {'Site': '', 'TargetSE': 'CERN-DST', 'JobType': 'DataReconstruction'}
    res = p_o.run()
    self.assertEqual(res, set(['Bologna', 'Ferrara', 'Paris', 'CSCS', 'IN2P3', 'CERN']))

    p_o.params = {'Site': '', 'TargetSE': 'IN2P3-DST', 'JobType': 'DataReconstruction'}
    res = p_o.run()
    self.assertEqual(res, set(['Bologna', 'Paris', 'CSCS', 'IN2P3']))

    # "Hospital" case: all sites go to hospital only
    p_o = TaskManagerPlugin('ByJobType', operationsHelper=opsHelperFakeHospital())

    p_o.params = {'Site': '', 'TargetSE': 'CERN-DST', 'JobType': 'DataReconstruction'}
    res = p_o.run()
    self.assertEqual(res, set(['Hospital']))

    p_o.params = {'Site': '', 'TargetSE': 'IN2P3-DST', 'JobType': 'DataReconstruction'}
    res = p_o.run()
    self.assertEqual(res, set(['Hospital']))

    # "Merge" case - 1
    p_o = TaskManagerPlugin('ByJobType', operationsHelper=opsHelperFakeMerge())

    p_o.params = {'Site': '', 'TargetSE': 'CERN-DST', 'JobType': 'Merge'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN']))

    p_o.params = {'Site': '', 'TargetSE': 'IN2P3-DST', 'JobType': 'Merge'}
    res = p_o.run()
    self.assertEqual(res, set(['IN2P3']))

    # "Merge" case - 2
    p_o = TaskManagerPlugin('ByJobType', operationsHelper=opsHelperFakeMerge2())

    p_o.params = {'Site': '', 'TargetSE': 'CERN-DST', 'JobType': 'Merge'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN']))

    p_o.params = {'Site': '', 'TargetSE': 'IN2P3-DST', 'JobType': 'Merge'}
    res = p_o.run()
    self.assertEqual(res, set(['IN2P3']))

    # "MC" case
    p_o = TaskManagerPlugin('ByJobType', operationsHelper=opsHelperFakeMC())

    p_o.params = {'Site': '', 'TargetSE': '', 'JobType': 'MC'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN', 'IN2P3', 'Bologna', 'Ferrara', 'Paris', 'CSCS', 'PAK', 'Hospital']))

    p_o.params = {'Site': '', 'TargetSE': '', 'JobType': 'MC'}
    res = p_o.run()
    self.assertEqual(res, set(['CERN', 'IN2P3', 'Bologna', 'Ferrara', 'Paris', 'CSCS', 'PAK', 'Hospital']))


#############################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TaskManagerPluginSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
