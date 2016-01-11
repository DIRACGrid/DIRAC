"""Test the DataRecoveryAgent"""

import unittest

from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR

from DIRAC.TransformationSystem.Agent.DataRecoveryAgent import DataRecoveryAgent

__RCSID__ = "$Id$"


class TestDRA(unittest.TestCase):
  """Test the DataRecoveryAgent"""
  dra = None

  @patch("DIRAC.Core.Base.AgentModule.PathFinder", new=Mock())
  @patch("DIRAC.ConfigurationSystem.Client.PathFinder.getSystemInstance", new=Mock())
  @patch("DIRAC.TransformationSystem.Agent.DataRecoveryAgent.ReqClient", new=Mock())
  def setUp(self):
    self.dra = DataRecoveryAgent(agentName="ILCTransformationSystem/DataRecoveryAgent", loadName="TestDRA")

  def tearDown(self):
    pass

  @patch("DIRAC.Core.Base.AgentModule.PathFinder", new=Mock())
  @patch("DIRAC.ConfigurationSystem.Client.PathFinder.getSystemInstance", new=Mock())
  @patch("DIRAC.TransformationSystem.Agent.DataRecoveryAgent.ReqClient", new=Mock())
  def test_init(self):
    """test for DataRecoveryAgent initialisation...................................................."""
    res = DataRecoveryAgent(agentName="ILCTransformationSystem/DataRecoveryAgent", loadName="TestDRA")
    self.assertIsInstance(res, DataRecoveryAgent)

  def test_beginExecution(self):
    """test for DataRecoveryAgent beginExecution...................................................."""
    res = self.dra.beginExecution()
    self.assertIn("MCReconstruction", self.dra.transformationTypes)
    self.assertFalse(self.dra.enabled)
    self.assertTrue(res['OK'])

  @patch("DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient.getTransformations",
         new=Mock(return_value=S_OK([dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd")]))
         )
  def test_getEligibleTransformations_success(self):
    """test for DataRecoveryAgent getEligibleTransformations success................................"""
    res = self.dra.getEligibleTransformations(status="Active", typeList=['TestProds'])
    self.assertTrue(res['OK'])
    self.assertIsInstance(res['Value'], dict)
    vals = res['Value']
    self.assertIn("1234", vals)
    self.assertIsInstance(vals['1234'], tuple)
    self.assertEqual(("TestProd", "TestProd12"), vals["1234"])

  @patch("DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient.getTransformations",
         new=Mock(return_value=S_ERROR("No can Do")))
  def test_getEligibleTransformations_faild(self):
    """test for DataRecoveryAgent getEligibleTransformation failure................................."""
    res = self.dra.getEligibleTransformations(status="Active", typeList=['TestProds'])
    self.assertFalse(res['OK'])
    self.assertEqual("No can Do", res['Message'])


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestDRA)
  TESTRESULT = unittest.TextTestRunner(verbosity=2).run(SUITE)
