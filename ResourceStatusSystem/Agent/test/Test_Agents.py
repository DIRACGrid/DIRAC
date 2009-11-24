import unittest
import sys
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
import DIRAC.ResourceStatusSystem.test.fake_AgentModule
import DIRAC.ResourceStatusSystem.test.fake_rsDB
import DIRAC.ResourceStatusSystem.test.fake_Logger

class AgentsTestCase(unittest.TestCase):
  """ Base class for the Agents test cases
  """
  def setUp(self):
    sys.modules["DIRAC.LoggingSystem.Client.Logger"] = DIRAC.ResourceStatusSystem.test.fake_Logger
    sys.modules["DIRAC.Core.Base.AgentModule"] = DIRAC.ResourceStatusSystem.test.fake_AgentModule
    sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"] = DIRAC.ResourceStatusSystem.test.fake_rsDB
    
    from DIRAC.ResourceStatusSystem.Agent.RS2HistoryAgent import RS2HistoryAgent
    self.rs2hAgent = RS2HistoryAgent("", "")

    from DIRAC.ResourceStatusSystem.Agent.RSInspectorAgent import RSInspectorAgent
    self.rsIAgent = RSInspectorAgent("", "")

    from DIRAC.ResourceStatusSystem.Agent.SSInspectorAgent import SSInspectorAgent
    self.ssIAgent = SSInspectorAgent("", "")

    from DIRAC.ResourceStatusSystem.Agent.SeSInspectorAgent import SeSInspectorAgent
    self.sesIAgent = SeSInspectorAgent("", "")

        
class RS2HistorySuccess(AgentsTestCase):

  def test_initialize(self):
    res = self.rs2hAgent.initialize()
    self.assert_(res['OK'])

  def test_execute(self):
    self.rs2hAgent.initialize()
    res = self.rs2hAgent.execute()
    self.assert_(res['OK'])


class RSInspectorSuccess(AgentsTestCase):

  def test_initialize(self):
    res = self.rsIAgent.initialize()
    self.assert_(res['OK'])

  def test_execute(self):
    self.rsIAgent.initialize()
    res = self.rsIAgent.execute()
    self.assert_(res['OK'])


class SSInspectorSuccess(AgentsTestCase):

  def test_initialize(self):
    res = self.ssIAgent.initialize()
    self.assert_(res['OK'])

  def test_execute(self):
    self.ssIAgent.initialize()
    res = self.ssIAgent.execute()
    self.assert_(res['OK'])

class SeSInspectorSuccess(AgentsTestCase):

  def test_initialize(self):
    res = self.sesIAgent.initialize()
    self.assert_(res['OK'])

  def test_execute(self):
    self.sesIAgent.initialize()
    res = self.sesIAgent.execute()
    self.assert_(res['OK'])




if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(AgentsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RS2HistorySuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RSInspectorSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SSInspectorSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SeSInspectorSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
