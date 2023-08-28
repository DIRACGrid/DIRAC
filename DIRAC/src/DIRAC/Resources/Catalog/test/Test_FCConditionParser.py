""" Testing the FCConditionPaserClass
"""
import unittest
from DIRAC.Resources.Catalog.FCConditionParser import FCConditionParser


class TestLogicEvaluation(unittest.TestCase):
    """Tests all the logic evaluation"""

    def setUp(self):
        self.fcp = FCConditionParser()
        self.lfns = ["/lhcb/lfn1", "/lhcb/lfn2"]

    def test_01_simpleParse(self):
        """Test the parse of a single plugin"""

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=False")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

    def test_02_notLogic(self):
        """Testing the ! operator"""

        res = self.fcp("catalogName", "operationName", self.lfns, condition="!Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="!Dummy=False")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

    def test_03_andLogic(self):
        """Testing the & operator"""

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=True & Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=False & Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=True & Dummy=False")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

    def test_04_orLogic(self):
        """Testing the | operator"""

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=True | Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=False | Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=False | Dummy=False")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

    def test_05_priority(self):
        """Testing the priority of operators"""

        res = self.fcp("catalogName", "operationName", self.lfns, condition="!Dummy=False & Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="!Dummy=True | Dummy=False")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=True & Dummy=False | Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=True | Dummy=False & Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="!Dummy=True | Dummy=False & Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="!Dummy=True | !Dummy=False & Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="!Dummy=True | !Dummy=False & !Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="[!Dummy=False] & Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="![Dummy=False] & Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="![Dummy=False & Dummy=True]")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="[Dummy=True | Dummy=False] & Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=True | [Dummy=False & Dummy=True]")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=False | [Dummy=False & Dummy=True]")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

    def test_06_errors(self):
        """Testing different error situation"""

        # Error in the plugin
        res = self.fcp("catalogName", "operationName", self.lfns, condition="Dummy=CantParse")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

        # Non existing plugin
        res = self.fcp("catalogName", "operationName", self.lfns, condition="NonExistingPlugin=something")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

        # Error in the grammar
        res = self.fcp("catalogName", "operationName", self.lfns, condition="[Dummy=True")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to False
        for lfn in self.lfns:
            self.assertTrue(not res["Value"]["Successful"][lfn], res)

    def test_07_noCondition(self):
        """Testing different error situation"""

        # Non condition given
        res = self.fcp("catalogName", "operationName", self.lfns, condition="")

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)

        # Can't retrive conditions
        # It so happen that it will all be True
        res = self.fcp("catalogName", "operationName", self.lfns, condition=None)

        self.assertTrue(res["OK"], res)
        # We expect all the lfn to be to True
        for lfn in self.lfns:
            self.assertTrue(res["Value"]["Successful"][lfn], res)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestLogicEvaluation)

    unittest.TextTestRunner(verbosity=2).run(suite)
