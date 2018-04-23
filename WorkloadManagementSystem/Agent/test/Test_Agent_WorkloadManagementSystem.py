""" Test class for WMS agents
"""

# pylint: disable=protected-access

# imports
import unittest
from mock import MagicMock, patch

from DIRAC import gLogger

# sut
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector

mockAM = MagicMock()
mockGC = MagicMock()
mockGC.getValue.return_value = 'TestSetup'
mockGCReply = MagicMock()
mockGCReply.return_value = 'TestSetup'
mockOPSObject = MagicMock()
mockOPSObject.getValue.return_value = '123'
mockOPSReply = MagicMock()
mockOPSReply.return_value = '123'

mockOPS = MagicMock()
mockOPS.return_value = mockOPSObject
# mockOPS.Operations = mockOPSObject
mockPM = MagicMock()
mockPM.requestToken.return_value = {'OK': True, 'Value': ('token', 1)}
mockPMReply = MagicMock()
mockPMReply.return_value = {'OK': True, 'Value': ('token', 1)}

mockCSGlobalReply = MagicMock()
mockCSGlobalReply.return_value = 'TestSetup'
mockResourcesReply = MagicMock()
mockResourcesReply.return_value = {'OK': True, 'Value': ['x86_64-slc6', 'x86_64-slc5']}


gLogger.setLevel('DEBUG')


class AgentsTestCase(unittest.TestCase):
  """ Base class for the Agents test cases
  """

  def setUp(self):
    pass

  def tearDown(self):
    pass


class SiteDirectorBaseSuccess(AgentsTestCase):
  """ Testing the single methods of SiteDirector
  """

  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gConfig.getValue", side_effect=mockGCReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.Operations", side_effect=mockOPS)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gProxyManager.requestToken", side_effect=mockPMReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule.__init__", new=mockAM)
  def test__getPilotOptions(self, _patch1, _patch2, _patch3, _patch4):
    """ Testing SiteDirector()._getPilotOptions()
    """
    sd = SiteDirector()
    sd.log = gLogger
    sd.am_getOption = mockAM
    sd.log.setLevel('DEBUG')
    sd.queueDict = {'aQueue': {'CEName': 'aCE',
                               'QueueName': 'aQueue',
                               'ParametersDict': {'CPUTime': 12345,
                                                  'Community': 'lhcb',
                                                  'OwnerGroup': ['lhcb_user'],
                                                  'Setup': 'LHCb-Production',
                                                  'Site': 'LCG.CERN.cern',
                                                  'SubmitPool': ''}}}
    res = sd._getPilotOptions('aQueue', 10)
    self.assertEqual(res[0], ['-S TestSetup', '-V 123', '-l 123', '-r 1,2,3', '-g 123',
                              '-o /Security/ProxyToken=token', '-M 1', '-C T,e,s,t,S,e,t,u,p',
                              '-e 1,2,3', '-N aCE', '-Q aQueue', '-n LCG.CERN.cern'])
    self.assertEqual(res[1], 1)

  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gConfig.getValue", side_effect=mockGCReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.CSGlobals.getSetup", side_effect=mockCSGlobalReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.Resources.getCompatiblePlatforms",
         side_effect=mockResourcesReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule.__init__", new=mockAM)
  def test__ifAndWhereToSubmit(self, _patch1, _patch2, _patch3, _patch4):
    """ Testing SiteDirector()._ifAndWhereToSubmit()
    """
    sd = SiteDirector()
    sd.log = gLogger
    sd.am_getOption = mockAM
    sd.log.setLevel('DEBUG')
    sd.matcherClient = MagicMock()
    submit, _anySite, _jobSites, _testSites = sd._ifAndWhereToSubmit()
    self.assertTrue(submit)

  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule.__init__", new=mockAM)
  def test__allowedToSubmit(self, _patch1):
    """ Testing SiteDirector()._allowedToSubmit()
    """
    sd = SiteDirector()
    sd.log = gLogger
    sd.am_getOption = mockAM
    sd.log.setLevel('DEBUG')
    sd.queueDict = {'aQueue': {'Site': 'LCG.CERN.cern',
                               'CEName': 'aCE',
                               'QueueName': 'aQueue',
                               'ParametersDict': {'CPUTime': 12345,
                                                  'Community': 'lhcb',
                                                  'OwnerGroup': ['lhcb_user'],
                                                  'Setup': 'LHCb-Production',
                                                  'Site': 'LCG.CERN.cern',
                                                  'SubmitPool': ''}}}
    submit = sd._allowedToSubmit('aQueue', True, set(['LCG.CERN.cern']), set())
    self.assertFalse(submit)

    sd.siteMaskList = ['LCG.CERN.cern', 'DIRAC.CNAF.it']
    submit = sd._allowedToSubmit('aQueue', True, set(['LCG.CERN.cern']), set())
    self.assertTrue(submit)

    sd.rssFlag = True
    submit = sd._allowedToSubmit('aQueue', True, set(['LCG.CERN.cern']), set())
    self.assertFalse(submit)

    sd.ceMaskList = ['aCE', 'anotherCE']
    submit = sd._allowedToSubmit('aQueue', True, set(['LCG.CERN.cern']), set())
    self.assertTrue(submit)

  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.gConfig.getValue", side_effect=mockGCReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.CSGlobals.getSetup", side_effect=mockCSGlobalReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.Resources.getCompatiblePlatforms",
         side_effect=mockResourcesReply)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule", side_effect=mockAM)
  @patch("DIRAC.WorkloadManagementSystem.Agent.SiteDirector.AgentModule.__init__", new=mockAM)
  def test__submitPilotsToQueue(self, _patch1, _patch2, _patch3, _patch4):
    """ Testing SiteDirector()._submitPilotsToQueue()
    """
    sd = SiteDirector()
    sd.log = gLogger
    sd.am_getOption = mockAM
    sd.log.setLevel('DEBUG')
    sd.rpcMatcher = MagicMock()
    sd.rssClient = MagicMock()
    sd.workingDirectory = ''
    sd.queueDict = {'aQueue': {'Site': 'LCG.CERN.cern',
                               'CEName': 'aCE',
                               'CEType': 'SSH',
                               'QueueName': 'aQueue',
                               'ParametersDict': {'CPUTime': 12345,
                                                  'Community': 'lhcb',
                                                  'OwnerGroup': ['lhcb_user'],
                                                  'Setup': 'LHCb-Production',
                                                  'Site': 'LCG.CERN.cern',
                                                  'SubmitPool': ''}}}
    sd.queueSlots = {'aQueue': {'AvailableSlots': 10}}
    res = sd._submitPilotsToQueue(1, MagicMock(), 'aQueue')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0], 0)

#############################################################################
# Test Suite run
#############################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(AgentsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SiteDirectorBaseSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
