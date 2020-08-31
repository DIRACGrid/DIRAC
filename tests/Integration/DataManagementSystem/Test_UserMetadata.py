"""
Test of multi-VO user metadata handling. Assumes a running Dirac instance with the (master?) FileCatalog
"""
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

import unittest
import os
import sys
import os.path
import traceback

from DIRAC.Core.Base.Script import parseCommandLine

parseCommandLine()

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC import gConfig

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup

try:
  res = getProxyInfo()
  if not res['OK']:
    raise Exception(res['Message'])

  proxyInfo = res['Value']
  username = proxyInfo['username']
  vo = ''
  if 'group' in proxyInfo:
    vo = getVOForGroup(proxyInfo['group'])

  DESTINATION_PATH = '/%s/test/unit-test/FC-user-metadata/' % vo

except Exception as e:  # pylint: disable=broad-except
  print(repr(e))
  sys.exit(2)


def random_dd(outfile, size_mb):
  import os
  with open(outfile, 'wb') as f:
    for i in range(int(size_mb * 2**20 / 512)):
      f.write(os.urandom(512))


class TestUserMetadataBasicTestCase(unittest.TestCase):
  def setUp(self):
    self.dirac = Dirac()
    csAPI = CSAPI()

    self.lfn5 = os.path.join(DESTINATION_PATH, 'test_file_10MB_v5.bin')
    self.dir5 = os.path.dirname(self.lfn5)
    # local file, for now:
    self.fname = os.path.basename(self.lfn5)
    random_dd(self.fname, 10)
    self.diracSE = 'SE-1'
    try:
      self.fc = FileCatalogClient("DataManagement/MultiVOFileCatalog")
    except Exception:
      self.fail(" FileCatalog(['MultiVOFileCatalog']) raised Exception unexpectedly!\n" + traceback.format_exc())
      return
    # add a replica
    self.fileadded = self.dirac.addFile(self.lfn5, self.fname, self.diracSE)
    self.assertTrue(self.fileadded['OK'])

  def tearDown(self):
    # meta index -r
    result = self.fc.deleteMetadataField('MetaInt6')
    self.assertTrue(result['OK'])
    result = self.fc.deleteMetadataField('TestDirectory6')
    self.assertTrue(result['OK'])
    # remove the MultiVOFileCatalog
    self.fc.removeCatalog('MultiVOFileCatalog')
    # delete a sole replica: dirac-dms-remove-files
    result = self.dirac.removeFile(self.lfn5)
    self.assertTrue(result['OK'])
    os.remove(self.fname)


class testMetadata(TestUserMetadataBasicTestCase):
  def test_verifyCatalogConfiguration(self):
    fileMetadataOption = gConfig.getOption('Systems/DataManagement/Production/Services/MultiVOFileCatalog/FileMetadata')
    dirMetadataOption = gConfig.getOption(
        'Systems/DataManagement/Production/Services/MultiVOFileCatalog/DirectoryMetadata')
    self.assertTrue(fileMetadataOption['OK'])
    self.assertEqual(fileMetadataOption['Value'], 'MultiVOFileMetadata')
    self.assertTrue(dirMetadataOption['OK'])
    self.assertEqual(dirMetadataOption['Value'], 'MultiVODirectoryMetadata')

  def test_fileCatalogClient(self):
    try:
      #  MultiVOFileCatalog instantiation test only
      fc = FileCatalogClient("DataManagement/MultiVOFileCatalog")
    except Exception:
      self.fail(" FileCatalogClient('DataManagement/MultiVOFileCatalog') raised ExceptionType unexpectedly!")

  def test_isFileAdded(self):
    self.assertTrue(self.fileadded['OK'])
    result = self.dirac.getLfnMetadata(self.lfn5)
    self.assertTrue(result['OK'])
    self.assertTrue(self.lfn5 in result['Value']['Successful'])
    self.assertEqual(result['Value']['Failed'], {})

  def test_metaIndex(self):
    # meta index -f
    result = self.fc.addMetadataField('MetaInt6', 'INT', metaType='-f')
    self.assertTrue(result['OK'])
    self.assertNotEqual(result['Value'], 'Already exists')
    self.assertTrue(result['Value'].startswith('Added new metadata:'))

   # meta index -d
    result = self.fc.addMetadataField('TestDirectory6', 'INT', metaType='-d')
    self.assertTrue(result['OK'])
    self.assertNotEqual(result['Value'], 'Already exists')
    self.assertTrue(result['Value'].startswith('Added new metadata:'))

   # meta show
    result = self.fc.getMetadataFields()
    self.assertTrue(result['OK'])
    self.assertDictContainsSubset({'MetaInt6': 'INT'}, result['Value']['FileMetaFields'])
    self.assertDictContainsSubset({'TestDirectory6': 'INT'}, result['Value']['DirectoryMetaFields'])

   # meta set
    metaDict6 = {'MetaInt6': 13}
    result = self.fc.setMetadata(self.lfn5, metaDict6)
    self.assertTrue(result['OK'])

    metaDirDict6 = {'TestDirectory6': 126}
    result = self.fc.setMetadata(self.dir5, metaDirDict6)
    self.assertTrue(result['OK'])

    # find (files)
    result = self.fc.findFilesByMetadata(metaDict6)
    self.assertTrue(result['OK'])
    self.assertIn(self.lfn5, result['Value'])

    # find (directories)
    result = self.fc.findDirectoriesByMetadata(metaDirDict6, path='/')
    self.assertTrue(result['OK'])
    self.assertIn(self.dir5, result['Value'].values())

    # API call only
    result = self.fc.getFileUserMetadata(self.lfn5)
    self.assertTrue(result['OK'])
    self.assertDictContainsSubset({'MetaInt6': 13}, result['Value'])
    # file: expect a failure
    result = self.fc.getDirectoryUserMetadata(self.lfn5)
    self.assertFalse(result['OK'])

    # directory
    result = self.fc.getDirectoryUserMetadata(self.dir5)
    self.assertTrue(result['OK'])
    self.assertDictContainsSubset({'TestDirectory6': 126}, result['Value'])

    # finally remove
    # meta remove lfn5 MetaInt6
    path = self.lfn5
    metadata = ['MetaInt6']
    metaDict = {path: metadata}
    result = self.fc.removeMetadata(metaDict)
    self.assertTrue(result['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestUserMetadataBasicTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(testMetadata))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
