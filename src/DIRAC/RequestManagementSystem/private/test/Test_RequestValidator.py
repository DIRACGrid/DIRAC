""" :mod: RequestValidatorTests
    =======================

    .. module: RequestValidatorTests
    :synopsis: test cases for RequestValidator
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for RequestValidator
"""
import unittest
from unittest.mock import MagicMock as Mock, patch

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

# SUT
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator


########################################################################
class RequestValidatorTests(unittest.TestCase):
    """
    .. class:: RequestValidatorTests

    """

    def setUp(self):
        """test setup"""
        self.request = Request()
        self.operation = Operation()
        self.file = File()

    def tearDown(self):
        """test tear down"""
        del self.request
        del self.operation
        del self.file

    @patch("DIRAC.ConfigurationSystem.Client.PathFinder.getSystemInstance", new=Mock())
    def testValidator(self):
        """validator test"""

        # create validator
        validator = RequestValidator()
        self.assertEqual(isinstance(validator, RequestValidator), True)

        # RequestName not set
        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])
        self.request.RequestName = "test_request"

        # # no operations
        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])
        self.request.addOperation(self.operation)

        # # type not set
        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])
        self.operation.Type = "ReplicateAndRegister"

        # # files not present
        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])
        self.operation.addFile(self.file)

        # # targetSE not set
        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])
        self.operation.TargetSE = "CERN-USER"

        # # missing LFN
        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])
        self.file.LFN = "/a/b/c"

        # # no ownerDN
        # force no owner DN because it takes the one of the current user
        self.request.OwnerDN = ""
        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])
        self.request.OwnerDN = "foo/bar=baz"

        # # no owner group
        # same, force it
        self.request.OwnerGroup = ""
        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])
        self.request.OwnerGroup = "dirac_user"

        # Checksum set, ChecksumType not set
        self.file.Checksum = "abcdef"
        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])

        # ChecksumType set, Checksum not set
        self.file.Checksum = ""
        self.file.ChecksumType = "adler32"

        ret = validator.validate(self.request)
        self.assertFalse(ret["OK"])

        # both set
        self.file.Checksum = "abcdef"
        self.file.ChecksumType = "adler32"
        ret = validator.validate(self.request)
        self.assertEqual(ret, {"OK": True, "Value": None})

        # both unset
        self.file.Checksum = ""
        self.file.ChecksumType = None
        ret = validator.validate(self.request)
        self.assertEqual(ret, {"OK": True, "Value": None})

        # all OK
        ret = validator.validate(self.request)
        self.assertEqual(ret, {"OK": True, "Value": None})


# test suite execution
if __name__ == "__main__":
    gTestLoader = unittest.TestLoader()
    gSuite = gTestLoader.loadTestsFromTestCase(RequestValidatorTests)
    gSuite = unittest.TestSuite([gSuite])
    unittest.TextTestRunner(verbosity=3).run(gSuite)
