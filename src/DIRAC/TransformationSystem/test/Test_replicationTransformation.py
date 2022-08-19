"""Test the dirac-transformation-replication script and helper"""
import unittest

from unittest.mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR

from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation
from DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters import Params

GET_VOMS = "DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters.getVOMSVOForGroup"
GET_PROXY = "DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters.getProxyInfo"


def getProxyMock(success=True):
    """return value for getProxy"""
    if success:
        return Mock(
            return_value=S_OK(
                {
                    "groupProperties": ["ProductionManagement"],
                    "group": "clic_prod",
                }
            )
        )
    return Mock(return_value=S_ERROR("Failed"))


def opMock():
    """return mock for config operations"""
    opmock = Mock()
    opmock.getOptionsDict.return_value = S_OK({"trans": "ProdID"})
    opmock.getValue.return_value = "ProdID"
    return Mock(return_value=opmock)


class TestMoving(unittest.TestCase):
    """Test the creation of moving transformation"""

    def setUp(self):
        self.tClientMock = Mock()
        # self.tClientMock.createTransformationInputDataQuery.return_value = S_OK()
        self.tClientMock.createTransformationMetaQuery.return_value = S_OK()
        self.tMock = Mock(return_value=self.tClientMock)

    def tearDown(self):
        pass

    def test_createRepl_Broadcast(self):
        """test creating transformation"""
        tSE = "Target-SRM"
        sSE = "Source-SRM"
        prodID = 12345
        trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
        with patch(trmodule + ".getTransformation", new=Mock(return_value=S_OK({}))), patch(
            trmodule + ".addTransformation", new=Mock(return_value=S_OK())
        ), patch(trmodule + "._Transformation__setSE", new=Mock(return_value=S_OK())):
            ret = createDataTransformation("Moving", tSE, sSE, "prodID", prodID, enable=True)
        self.assertTrue(ret["OK"], ret.get("Message", ""))
        self.assertEqual(ret["Value"].getPlugin().get("Value"), "Broadcast")
        self.assertEqual(ret["Value"].inputMetaQuery, {"prodID": prodID})

    def test_createRepl_NoSource(self):
        """test creating transformation"""
        tSE = "Target-SRM"
        sSE = ""
        prodID = 12345
        trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
        with patch(trmodule + ".getTransformation", new=Mock(return_value=S_OK({}))), patch(
            trmodule + ".addTransformation", new=Mock(return_value=S_OK())
        ), patch(trmodule + "._Transformation__setSE", new=Mock(return_value=S_OK())):
            ret = createDataTransformation("Moving", tSE, sSE, "prodID", prodID, enable=True)
        self.assertTrue(ret["OK"], ret.get("Message", ""))
        self.assertEqual(ret["Value"].getPlugin().get("Value"), "Broadcast")

    def test_createRepl_Dry(self):
        """test creating transformation"""
        tSE = "Target-SRM"
        sSE = "Source-SRM"
        prodID = 12345
        trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
        with patch(trmodule + ".getTransformation", new=Mock(return_value=S_OK({}))), patch(
            trmodule + ".addTransformation", new=Mock(return_value=S_OK())
        ), patch(trmodule + "._Transformation__setSE", new=Mock(return_value=S_OK())):
            ret = createDataTransformation("Moving", tSE, sSE, "prodID", prodID, enable=False, extraData={})
        self.assertTrue(ret["OK"], ret.get("Message", ""))

    def test_createRepl_2(self):
        """test creating transformation"""
        tSE = "Target-SRM"
        sSE = "Source-SRM"
        prodID = 12345
        trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
        with patch(trmodule + ".getTransformation", new=Mock(return_value=S_OK({}))), patch(
            trmodule + ".addTransformation", new=Mock(return_value=S_OK())
        ), patch(trmodule + "._Transformation__setSE", new=Mock(return_value=S_OK())):
            ret = createDataTransformation("Moving", tSE, sSE, "prodID", prodID, extraname="extraName", enable=True)
        self.assertTrue(ret["OK"], ret.get("Message", ""))

    def test_createRepl_BadFlavour(self):
        """test creating transformation"""
        tSE = "Target-SRM"
        sSE = "Source-SRM"
        prodID = 12345
        trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
        with patch(trmodule + ".getTransformation", new=Mock(return_value=S_OK({}))), patch(
            trmodule + ".addTransformation", new=Mock(return_value=S_OK())
        ), patch(trmodule + "._Transformation__setSE", new=Mock(return_value=S_OK())):
            ret = createDataTransformation("Smelly", tSE, sSE, "prodID", prodID, extraname="extraName", enable=True)
        self.assertFalse(ret["OK"], ret.get("Message", ""))
        self.assertIn("Unsupported flavour", ret["Message"])

    def test_createRepl_SEFail_1(self):
        """test creating transformation"""
        tSE = "Target-SRM"
        sSE = "Source-SRM"
        prodID = 12345
        trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
        with patch(trmodule + ".getTransformation", new=Mock(return_value=S_OK({}))), patch(
            trmodule + ".addTransformation", new=Mock(return_value=S_OK())
        ), patch(trmodule + "._Transformation__setSE", new=Mock(side_effect=(S_OK(), S_ERROR()))):
            ret = createDataTransformation("Moving", tSE, sSE, "prodID", prodID, enable=True)
        self.assertFalse(ret["OK"], str(ret))
        self.assertIn("TargetSE not valid", ret["Message"])

    def test_createRepl_SEFail_2(self):
        """test creating transformation"""
        tSE = "Target-SRM"
        sSE = "Source-SRM"
        prodID = 12345
        trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
        with patch(trmodule + ".getTransformation", new=Mock(return_value=S_OK({}))), patch(
            trmodule + ".addTransformation", new=Mock(return_value=S_OK())
        ), patch(trmodule + "._Transformation__setSE", new=Mock(side_effect=(S_ERROR(), S_ERROR()))):
            ret = createDataTransformation("Moving", tSE, sSE, "prodID", prodID, enable=True)
        self.assertFalse(ret["OK"], str(ret))
        self.assertIn("SourceSE not valid", ret["Message"])

    def test_createRepl_addTrafoFail_(self):
        """test creating transformation"""
        tSE = "Target-SRM"
        sSE = "Source-SRM"
        prodID = 12345
        trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
        with patch(trmodule + ".getTransformation", new=Mock(return_value=S_OK({}))), patch(
            trmodule + ".addTransformation", new=Mock(return_value=S_ERROR("Cannot add Trafo"))
        ), patch(trmodule + "._Transformation__setSE", new=Mock(return_value=S_OK())):
            ret = createDataTransformation("Moving", tSE, sSE, "prodID", prodID, enable=True)
        self.assertFalse(ret["OK"], str(ret))
        self.assertIn("Cannot add Trafo", ret["Message"])

    def test_createRepl_createTrafoFail_(self):
        """test creating transformation"""
        tSE = "Target-SRM"
        sSE = "Source-SRM"
        prodID = 12345
        trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
        with patch(trmodule + ".getTransformation", new=Mock(return_value=S_OK({}))), patch(
            trmodule + ".addTransformation", new=Mock(return_value=S_ERROR("Failed to add IMQ"))
        ), patch(trmodule + "._Transformation__setSE", new=Mock(return_value=S_OK())):
            ret = createDataTransformation("Moving", tSE, sSE, "prodID", prodID, enable=True)
        self.assertFalse(ret["OK"], str(ret))
        self.assertIn("Failed to add IMQ", ret["Message"])


class TestParams(unittest.TestCase):
    """Test the parameters for the moving creation script"""

    def setUp(self):
        self.arguments = []
        self.sMock = Mock()
        self.sMock.getPositionalArgs.return_value = self.arguments
        self.params = Params()

    def tearDown(self):
        pass

    @patch(GET_PROXY, new=getProxyMock())
    @patch(GET_VOMS, new=Mock(return_value="clic"))
    def test_checkSettings(self):
        self.arguments = ["12345", "TargetSE"]
        self.sMock.getPositionalArgs.return_value = self.arguments
        ret = self.params.checkSettings(self.sMock)
        self.assertTrue(ret["OK"], ret.get("Message", ""))
        self.assertEqual(self.params.metaValues, ["12345"])
        self.assertEqual(self.params.sourceSE, "")
        self.assertEqual(self.params.targetSE, ["TargetSE"])

    @patch(GET_PROXY, new=getProxyMock())
    @patch(GET_VOMS, new=Mock(return_value="clic"))
    def test_setMetadata(self):
        ret = self.params.setMetadata("Datatype:GEN, Energy: 124")
        self.assertTrue(ret["OK"], ret.get("Message", ""))
        self.assertEqual(self.params.extraData, {"Datatype": "GEN", "Energy": "124"})

    @patch(GET_PROXY, new=getProxyMock())
    @patch(GET_VOMS, new=Mock(return_value="clic"))
    def test_checkSettings_FailArgumentSize(self):
        self.arguments = ["12345", "TargetSE", "Foo"]
        self.sMock.getPositionalArgs.return_value = self.arguments
        ret = self.params.checkSettings(self.sMock)
        self.assertFalse(ret["OK"], str(ret))
        self.assertTrue(any("ERROR: Wrong number of arguments" in msg for msg in self.params.errorMessages))

    @patch(GET_PROXY, new=getProxyMock(False))
    @patch(GET_VOMS, new=Mock(return_value="clic"))
    def test_FailProxy(self):
        self.arguments = ["12345", "TargetSE"]
        self.sMock.getPositionalArgs.return_value = self.arguments
        ret = self.params.checkSettings(self.sMock)
        self.assertFalse(ret["OK"], str(ret))
        self.assertTrue(
            any("ERROR: No Proxy" in msg for msg in self.params.errorMessages), str(self.params.errorMessages)
        )

    @patch(GET_PROXY, new=getProxyMock(True))
    @patch(GET_VOMS, new=Mock(return_value=""))
    def test_FailProxy2(self):
        self.arguments = ["12345", "TargetSE"]
        self.sMock.getPositionalArgs.return_value = self.arguments
        ret = self.params.checkSettings(self.sMock)
        self.assertFalse(ret["OK"], str(ret))
        self.assertTrue(
            any("ERROR: ProxyGroup" in msg for msg in self.params.errorMessages), str(self.params.errorMessages)
        )

    def test_setExtraName(self):
        ret = self.params.setExtraname("extraName")
        self.assertTrue(ret["OK"], ret.get("Message", ""))
        self.assertEqual("extraName", self.params.extraname)
