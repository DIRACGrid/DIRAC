.. _testing_agents:

=====================================
Testing an agent while developing it
=====================================

An agent can be tested in 2 ways: either with a unit test, or with an integration test. One does not exclude the other.

Agents can be very complex. So, deciding how you approach test is very much dependent on what's the code inside the agent itself.

First, tackling the integration test: in DIRAC/tests there's no integration test involving agents.
That's because an integration test for an agent simply means "start it, and look in how it goes".
There's not much else that can be done, maybe the only thing would be to test that "execute()" returns S_OK()

So, what can be wrote down are integration tests::

.. code-block:: python

   import unittest, importlib
   from mock import MagicMock, patch

   class MyAgentTestCase(unittest.TestCase):

   def setUp( self ):
     self.mockAM = MagicMock()
     self.agent = importlib.import_module('LHCbDIRAC.TransformationSystem.Agent.MCSimulationTestingAgent')
     self.agent.AgentModule = self.mockAM
     self.agent = MCSimulationTestingAgent()
     self.agent.log = gLogger
     self.agent.log.setLevel('DEBUG')

   def tearDown(self):
     pass

   def test_myTest(self):
     bla


   if __name__ == '__main__':
     suite = unittest.defaultTestLoader.loadTestsFromTestCase(MyAgentTestCase)
     testResult = unittest.TextTestResult(verbosity = 2).run(suite)
