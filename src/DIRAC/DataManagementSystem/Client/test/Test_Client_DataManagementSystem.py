""" Unit test for ConsistencyInspector
"""
import unittest
import datetime
from unittest.mock import MagicMock

from DIRAC import gLogger
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock

# sut
from DIRAC.DataManagementSystem.Client.ConsistencyInspector import ConsistencyInspector


class UtilitiesTestCase(unittest.TestCase):
    def setUp(self):

        gLogger.setLevel("DEBUG")

        self.lfnDict = {
            "aa.raw": {
                "aa.raw": {"FileType": "RAW", "RunNumber": 97019},
                "/lhcb/1_2_1.Semileptonic.dst": {"FileType": "SEMILEPTONIC.DST"},
            },
            "cc.raw": {
                "cc.raw": {"FileType": "RAW", "RunNumber": 97019},
                "/lhcb/1_1.semileptonic.dst": {"FileType": "SEMILEPTONIC.DST"},
            },
        }

        dmMock = MagicMock()
        dicMock = MagicMock()

        self.ci = ConsistencyInspector(transClient=MagicMock(), dm=dmMock, fc=fc_mock, dic=dicMock)
        self.ci.fileType = ["SEMILEPTONIC.DST", "LOG", "RAW"]
        self.ci.fileTypesExcluded = ["LOG"]
        self.ci.prod = 0
        self.maxDiff = None


class ConsistencyInspectorSuccess(UtilitiesTestCase):
    def test_getReplicasPresence(self):
        res = self.ci.getReplicasPresence(["/this/is/dir1/file1.txt", "/this/is/dir1/file2.foo.bar"])
        self.assertEqual(tuple(map(set, res)), ({"/this/is/dir1/file1.txt", "/this/is/dir1/file2.foo.bar"}, set()))

    def test__selectByFileType(self):
        lfnDict = {
            "aa.raw": {
                "bb.raw": {"FileType": "RAW", "RunNumber": 97019},
                "bb.log": {"FileType": "LOG"},
                "/bb/pippo/aa.dst": {"FileType": "DST"},
                "/lhcb/1_2_1.Semileptonic.dst": {"FileType": "SEMILEPTONIC.DST"},
            },
            "cc.raw": {
                "dd.raw": {"FileType": "RAW", "RunNumber": 97019},
                "bb.log": {"FileType": "LOG"},
                "/bb/pippo/aa.dst": {"FileType": "LOG"},
                "/lhcb/1_1.semileptonic.dst": {"FileType": "SEMILEPTONIC.DST"},
            },
        }

        res = self.ci._selectByFileType(lfnDict)

        lfnDictExpected = {
            "aa.raw": {
                "/lhcb/1_2_1.Semileptonic.dst": {"FileType": "SEMILEPTONIC.DST"},
                "bb.raw": {"RunNumber": 97019, "FileType": "RAW"},
            },
            "cc.raw": {
                "dd.raw": {"RunNumber": 97019, "FileType": "RAW"},
                "/lhcb/1_1.semileptonic.dst": {"FileType": "SEMILEPTONIC.DST"},
            },
        }
        self.assertEqual(res, lfnDictExpected)

        lfnDict = {"aa.raw": {"/bb/pippo/aa.dst": {"FileType": "LOG"}, "bb.log": {"FileType": "LOG"}}}
        res = self.ci._selectByFileType(lfnDict)
        lfnDictExpected = {}
        self.assertEqual(res, lfnDictExpected)

    def test__getFileTypesCount(self):
        lfnDict = {"aa.raw": {"bb.log": {"FileType": "LOG"}, "/bb/pippo/aa.dst": {"FileType": "DST"}}}
        res = self.ci._getFileTypesCount(lfnDict)
        resExpected = {"aa.raw": {"DST": 1, "LOG": 1}}
        self.assertEqual(res, resExpected)

        lfnDict = {
            "aa.raw": {
                "bb.log": {"FileType": "LOG"},
                "/bb/pippo/aa.dst": {"FileType": "DST"},
                "/bb/pippo/cc.dst": {"FileType": "DST"},
            }
        }
        res = self.ci._getFileTypesCount(lfnDict)
        resExpected = {"aa.raw": {"DST": 2, "LOG": 1}}
        self.assertEqual(res, resExpected)

    # def test__catalogDirectoryToSE(self):
    #   lfnDir = ['/this/is/dir1/', '/this/is/dir2/']
    #
    #   res = self.ci.catalogDirectoryToSE(lfnDir)
    #   self.assertTrue(res['OK'])


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(UtilitiesTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ConsistencyInspectorSuccess))
    testResult = unittest.TextTestRunner(verbosity=3).run(suite)
