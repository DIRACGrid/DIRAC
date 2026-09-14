import unittest

from unittest.mock import MagicMock

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP


class PolicySystemTestCase(unittest.TestCase):
    """Base class for the PDP - PEP test cases"""

    def setUp(self):
        gLogger.setLevel("DEBUG")

        self.RSMock = MagicMock()
        self.RMMock = MagicMock()
        self.RMMock.selectStatusElement.return_value = {"OK": True, "Value": "bla"}
        self.mockPDP = MagicMock()


class PEPSuccess(PolicySystemTestCase):
    def test_enforce(self):
        pep = PEP(
            {"ResourceStatusClient": self.RSMock, "ResourceManagementClient": self.RMMock, "SiteStatus": self.RMMock}
        )
        pep.pdp = self.mockPDP
        res = pep.enforce(None)
        self.assertTrue(res["OK"])

        decisionParams = {}
        res = pep.enforce(decisionParams)
        self.assertTrue(res["OK"])

        decisionParams = {"element": "Site", "name": "Site1"}
        decParamsPDP = dict(decisionParams)
        decParamsPDP["active"] = "active"
        self.mockPDP.takeDecision.return_value = {
            "OK": True,
            "Value": {
                "policyCombinedResult": {
                    "PolicyType": ["", ""],
                    "PolicyAction": [("aa", "bb")],
                    "Status": "S",
                    "Reason": "testReason",
                },
                "singlePolicyResults": [
                    {"Status": "Active", "PolicyName": "SAM_CE_Policy", "Reason": "SAM:ok"},
                    {
                        "Status": "Banned",
                        "PolicyName": "DT_Policy_Scheduled",
                        "Reason": "DT:OUTAGE in 1 hours",
                        "EndDate": "2010-02-16 15:00:00",
                    },
                ],
                "decisionParams": decParamsPDP,
            },
        }
        res = pep.enforce(decisionParams)
        self.assertTrue(res["OK"])

        decisionParams = {"element": "Resource", "name": "StorageElement", "statusType": "ReadAccess"}
        res = pep.enforce(decisionParams)
        self.assertTrue(res["OK"])


class PDPCombinePoliciesTest(PolicySystemTestCase):
    """Test cases for PDP._combineSinglePolicyResults, especially edge cases with None status"""

    def test_combineSinglePolicyResults_with_none_status(self):
        """Test that _combineSinglePolicyResults handles None status without raising KeyError.

        This reproduces the bug where decisionParams['status'] is None, causing
        StateMachine.setState to raise KeyError when accessing self.states[None].
        """
        pdp = PDP()
        pdp.setup(
            {
                "element": "Resource",
                "name": "testResource",
                "elementType": "ComputingElement",
                "statusType": "ReadAccess",
                "status": None,  # This is the problematic case
                "reason": None,
                "tokenOwner": None,
            }
        )

        singlePolicyResults = [{"Status": "Active", "Reason": "TestReason", "Policy": {"name": "TestPolicy"}}]

        # This should not raise KeyError
        result = pdp._combineSinglePolicyResults(singlePolicyResults)
        self.assertTrue(result["OK"], f"Expected OK=True, got: {result}")

    def test_combineSinglePolicyResults_with_valid_status(self):
        """Test that _combineSinglePolicyResults works correctly with a valid status"""
        pdp = PDP()
        pdp.setup(
            {
                "element": "Resource",
                "name": "testResource",
                "elementType": "ComputingElement",
                "statusType": "ReadAccess",
                "status": "Active",
                "reason": None,
                "tokenOwner": None,
            }
        )

        singlePolicyResults = [{"Status": "Active", "Reason": "TestReason", "Policy": {"name": "TestPolicy"}}]

        result = pdp._combineSinglePolicyResults(singlePolicyResults)
        self.assertTrue(result["OK"], f"Expected OK=True, got: {result}")
        self.assertEqual(result["Value"]["Status"], "Active")

    def test_combineSinglePolicyResults_empty_results(self):
        """Test that _combineSinglePolicyResults handles empty results"""
        pdp = PDP()
        pdp.setup(
            {
                "element": "Resource",
                "name": "testResource",
                "elementType": "ComputingElement",
                "statusType": "ReadAccess",
                "status": None,
                "reason": None,
                "tokenOwner": None,
            }
        )

        result = pdp._combineSinglePolicyResults([])
        self.assertTrue(result["OK"], f"Expected OK=True, got: {result}")
        self.assertEqual(result["Value"]["Status"], "Unknown")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(PolicySystemTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PEPSuccess))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PDPCombinePoliciesTest))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
