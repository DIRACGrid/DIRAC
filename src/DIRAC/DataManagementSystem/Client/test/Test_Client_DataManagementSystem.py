""" Unit test for ConsistencyInspector
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import datetime
from mock import MagicMock

from DIRAC import gLogger
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock

# sut
from DIRAC.DataManagementSystem.Client.ConsistencyInspector import ConsistencyInspector


class UtilitiesTestCase(unittest.TestCase):

  def setUp(self):

    gLogger.setLevel('DEBUG')

    self.lfnDict = {'aa.raw': {'aa.raw': {'FileType': 'RAW', 'RunNumber': 97019},
                               '/lhcb/1_2_1.Semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'}},
                    'cc.raw': {'cc.raw': {'FileType': 'RAW', 'RunNumber': 97019},
                               '/lhcb/1_1.semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'}}
                    }

    dmMock = MagicMock()
    dicMock = MagicMock()

    self.ci = ConsistencyInspector(transClient=MagicMock(), dm=dmMock, fc=fc_mock, dic=dicMock)
    self.ci.fileType = ['SEMILEPTONIC.DST', 'LOG', 'RAW']
    self.ci.fileTypesExcluded = ['LOG']
    self.ci.prod = 0
    self.maxDiff = None


class ConsistencyInspectorSuccess(UtilitiesTestCase):

  def test_getReplicasPresence(self):
    res = self.ci.getReplicasPresence(['/this/is/dir1/file1.txt', '/this/is/dir1/file2.foo.bar'])
    self.assertEqual(tuple(map(set, res)), ({'/this/is/dir1/file1.txt', '/this/is/dir1/file2.foo.bar'}, set()))

  def test__selectByFileType(self):
    lfnDict = {'aa.raw': {'bb.raw': {'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log': {'FileType': 'LOG'},
                          '/bb/pippo/aa.dst': {'FileType': 'DST'},
                          '/lhcb/1_2_1.Semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'}},
               'cc.raw': {'dd.raw': {'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log': {'FileType': 'LOG'},
                          '/bb/pippo/aa.dst': {'FileType': 'LOG'},
                          '/lhcb/1_1.semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'}}
               }

    res = self.ci._selectByFileType(lfnDict)

    lfnDictExpected = {'aa.raw': {'/lhcb/1_2_1.Semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'},
                                  'bb.raw': {'RunNumber': 97019, 'FileType': 'RAW'}
                                  },
                       'cc.raw': {'dd.raw': {'RunNumber': 97019, 'FileType': 'RAW'},
                                  '/lhcb/1_1.semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'}
                                  }
                       }
    self.assertEqual(res, lfnDictExpected)

    lfnDict = {'aa.raw': {'/bb/pippo/aa.dst': {'FileType': 'LOG'},
                          'bb.log': {'FileType': 'LOG'}
                          }
               }
    res = self.ci._selectByFileType(lfnDict)
    lfnDictExpected = {}
    self.assertEqual(res, lfnDictExpected)

  def test__getFileTypesCount(self):
    lfnDict = {'aa.raw': {'bb.log': {'FileType': 'LOG'},
                          '/bb/pippo/aa.dst': {'FileType': 'DST'}}}
    res = self.ci._getFileTypesCount(lfnDict)
    resExpected = {'aa.raw': {'DST': 1, 'LOG': 1}}
    self.assertEqual(res, resExpected)

    lfnDict = {'aa.raw': {'bb.log': {'FileType': 'LOG'},
                          '/bb/pippo/aa.dst': {'FileType': 'DST'},
                          '/bb/pippo/cc.dst': {'FileType': 'DST'}}}
    res = self.ci._getFileTypesCount(lfnDict)
    resExpected = {'aa.raw': {'DST': 2, 'LOG': 1}}
    self.assertEqual(res, resExpected)

  # def test__catalogDirectoryToSE(self):
  #   lfnDir = ['/this/is/dir1/', '/this/is/dir2/']
  #
  #   res = self.ci.catalogDirectoryToSE(lfnDir)
  #   self.assertTrue(res['OK'])

  def test__getCatalogDirectoryContents(self):
    lfnDirs = ['/this/is/dir1/', '/this/is/dir2/']

    res = self.ci._getCatalogDirectoryContents(lfnDirs)
    self.assertTrue(res['OK'])

    resExpected = {'Metadata': {'/this/is/dir1/file1.txt': {'MetaData': {'Checksum': '7149ed85',
                                                                         'ChecksumType': 'Adler32',
                                                                         'CreationDate': datetime.datetime(
                                                                             2014, 12, 4, 12, 16, 56),
                                                                         'FileID': 156301805,
                                                                         'GID': 2695,
                                                                         'GUID': '6A5C6C86-AD7B-E411-9EDB',
                                                                         'Mode': 436,
                                                                         'ModificationDate': datetime.datetime(
                                                                             2014, 12, 4, 12, 16, 56),
                                                                         'Owner': 'phicharp',
                                                                         'OwnerGroup': 'lhcb_prod',
                                                                         'Size': 206380531,
                                                                         'Status': 'AprioriGood',
                                                                         'Type': 'File',
                                                                         'UID': 19503}},
                                '/this/is/dir1/file2.foo.bar': {'MetaData': {'Checksum': '7149ed86',
                                                                             'ChecksumType': 'Adler32',
                                                                             'CreationDate': datetime.datetime(
                                                                                 2014, 12, 4, 12, 16, 56),
                                                                             'FileID': 156301805,
                                                                             'GID': 2695,
                                                                             'GUID': '6A5C6C86-AD7B-E411-9EDB',
                                                                             'Mode': 436,
                                                                             'ModificationDate': datetime.datetime(
                                                                                 2014, 12, 4, 12, 16, 56),
                                                                             'Owner': 'phicharp',
                                                                             'OwnerGroup': 'lhcb_prod',
                                                                             'Size': 206380532,
                                                                             'Status': 'AprioriGood',
                                                                             'Type': 'File',
                                                                             'UID': 19503}},
                                '/this/is/dir2/subdir1/file3.pippo': {'MetaData': {'Checksum': '7149ed86',
                                                                                   'ChecksumType': 'Adler32',
                                                                                   'CreationDate': datetime.datetime(
                                                                                       2014, 12, 4, 12, 16, 56),
                                                                                   'FileID': 156301805,
                                                                                   'GID': 2695,
                                                                                   'GUID': '6A5C6C86-AD7B-E411-9EDB',
                                                                                   'Mode': 436,
                                                                                   'ModificationDate':
                                                                                       datetime.datetime(
                                                                                       2014, 12, 4, 12, 16, 56),
                                                                                   'Owner': 'phicharp',
                                                                                   'OwnerGroup': 'lhcb_prod',
                                                                                   'Size': 206380532,
                                                                                   'Status': 'AprioriGood',
                                                                                   'Type': 'File',
                                                                                   'UID': 19503}}},
                   'Replicas': {'/this/is/dir1/file1.txt':
                                {'SE1': 'smr://srm.SE1.ch:8443/srm/v2/server?SFN=/this/is/dir1/file1.txt',
                                 'SE2': 'smr://srm.SE2.fr:8443/srm/v2/server?SFN=/this/is/dir1/file1.txt'},
                                '/this/is/dir1/file2.foo.bar':
                                {'SE1': 'smr://srm.SE1.ch:8443/srm/v2/server?SFN=/this/is/dir1/file2.foo.bar',
                                 'SE3': 'smr://srm.SE3.es:8443/srm/v2/server?SFN=/this/is/dir1/file2.foo.bar'}}}

    self.assertEqual(res['Value'], resExpected)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UtilitiesTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ConsistencyInspectorSuccess))
  testResult = unittest.TextTestRunner(verbosity=3).run(suite)
