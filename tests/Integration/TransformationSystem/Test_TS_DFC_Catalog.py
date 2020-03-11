""" This is an integration test using both the TSCatalog and the FileCatalog plugins

    It supposes that the TransformationDB and the FileCatalogDB are present
    It supposes the TransformationManager and that DataManagement/FileCatalog services running

    The TSCatalog and FileCatalog plugins must be configured
    in the Resources section and set in the Operations CatalogList, e.g.:

    Operations
    {
        Services
        {
            Catalogs
            {
		CatalogList = FileCatalog, TSCatalog
		FileCatalog
		{
		    CatalogType = FileCatalog
		    AccessType = Read-Write
		    Status = Active
		    CatalogURL = DataManagement/FileCatalog
		}
		TSCatalog
		{
		    CatalogType = TSCatalog
		    AccessType = Write
		    Status = Active
		    CatalogURL = Transformation/TransformationManager
		}
            }
	}
    }
"""

# pylint: disable=invalid-name,wrong-import-position

import unittest
import os
import sys

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from DIRAC import gLogger
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.DataManager import DataManager


class TestTSDFCCatalogTestCase(unittest.TestCase):

  def setUp(self):
    self.transClient = TransformationClient()
    self.fc = FileCatalog()
    self.dm = DataManager()
    self.metaCatalog = 'FileCatalog'
    gLogger.setLevel('DEBUG')


class TransformationClientChainID(TestTSDFCCatalogTestCase):

  def test_inputDataQueries(self):
    # ## Add metadata fields to the DFC (directory level)
    MDFieldDict = {'particle': 'VARCHAR(128)', 'zenith': 'int'}
    for MDField in MDFieldDict:
      MDFieldType = MDFieldDict[MDField]
      res = self.fc.addMetadataField(MDField, MDFieldType)
      self.assertTrue(res['OK'])

    # Create a directory in the DFC and set the directory metadata
    dirpath1 = '/dir1'
    res = self.fc.createDirectory(dirpath1)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Successful'][dirpath1][self.metaCatalog], True)

    MDdict1 = {'particle': 'gamma_diffuse', 'zenith': 20}
    res = self.fc.setMetadata(dirpath1, MDdict1)
    self.assertTrue(res['OK'])

    # Add a first file to all catalog plugins and check that it's not added to the TS Catalog
    filename = 'file1'
    lfn1 = os.path.join(dirpath1, filename)
    fileTuple = (lfn1, 'destUrl', 0, 'ALPHA-Disk', 'D41D8CD9-8F00-B204-E980-0998ECF8427E', '001')

    res = self.dm.registerFile(fileTuple)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Successful'][lfn1][self.metaCatalog], True)
    self.assertEqual(res['Value']['Successful'][lfn1]['TSCatalog'], False)

    # Create a transformation having a query that matches the file metadata
    MDdict1b = {'particle': 'gamma_diffuse', 'zenith': {"<=": 20}}
    res = self.transClient.addTransformation(
        'transformationName',
        'description',
        'longDescription',
        'MCSimulation',
        'Standard',
        'Manual',
        '',
        inputMetaQuery=MDdict1b)

    self.assertTrue(res['OK'])
    transID = res['Value']

    # Verify that the created file is added to the transformation
    res = self.transClient.getTransformationFiles({'TransformationID': transID})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][0]['LFN'], lfn1)

    # Add a second file having the same metadata and check that it's added also to the TS Catalog
    filename = 'file2'
    lfn2 = os.path.join(dirpath1, filename)
    fileTuple = (lfn2, 'destUrl', 0, 'ALPHA-Disk', 'D41D8CD9-8F00-B204-E980-0998ECF8427E', '001')
    res = self.dm.registerFile(fileTuple)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Successful'][lfn2][self.metaCatalog], True)
    self.assertEqual(res['Value']['Successful'][lfn2]['TSCatalog'], True)

    # Verify that the second file has been automatically added to the transformation
    res = self.transClient.getTransformationFiles({'TransformationID': transID})
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][1]['LFN'], lfn2)

    # Add a third file having different metadata not matching the transformation query
    # and check that it's not added to the TS Catalog
    dirpath2 = '/dir2'
    res = self.fc.createDirectory(dirpath2)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Successful'][dirpath2][self.metaCatalog], True)

    MDdict2 = {'particle': 'gamma_diffuse', 'zenith': 40}
    res = self.fc.setMetadata(dirpath2, MDdict2)
    self.assertTrue(res['OK'])

    fileName = 'file3'
    lfn3 = os.path.join(dirpath2, fileName)
    fileTuple = (lfn3, 'destUrl', 0, 'ALPHA-Disk', 'D41D8CD9-8F00-B204-E980-0998ECF8427E', '001')
    res = self.dm.registerFile(fileTuple)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Successful'][lfn3][self.metaCatalog], True)
    self.assertEqual(res['Value']['Successful'][lfn3]['TSCatalog'], False)

    # Verify that the third file has not been added to the the transformation
    res = self.transClient.getTransformationFiles({'TransformationID': transID})
    self.assertTrue(res['OK'])
    for ires in res['Value']:
      self.assertNotEqual(ires['LFN'], lfn3)

    # Delete the transformation
    res = self.transClient.deleteTransformation(transID)
    self.assertTrue(res['OK'])

    # Create another transformation having a query not matching none of the files added to the DFC
    MDdict3 = {'particle': 'gamma', 'zenith': 60}
    res = self.transClient.addTransformation(
        'transformationName',
        'description',
        'longDescription',
        'MCSimulation',
        'Standard',
        'Manual',
        '',
        inputMetaQuery=MDdict3)
    self.assertTrue(res['OK'])
    transID = res['Value']

    # Verify that no files have been added to the transformation
    res = self.transClient.getTransformationFiles({'TransformationID': transID})
    self.assertEqual(len(res['Value']), 0)

    # Delete the transformation
    res = self.transClient.deleteTransformation(transID)
    self.assertTrue(res['OK'])

    # Create another transformation with no InputMetaQuery defined
    res = self.transClient.addTransformation(
        'transformationName',
        'description',
        'longDescription',
        'MCSimulation',
        'Standard',
        'Manual',
        '')
    self.assertTrue(res['OK'])
    transID = res['Value']

    # Verify that no files have been added to the transformation
    res = self.transClient.getTransformationFiles({'TransformationID': transID})
    self.assertEqual(len(res['Value']), 0)

    # Delete the transformation
    res = self.transClient.deleteTransformation(transID)
    self.assertTrue(res['OK'])

    # Remove files from DFC and TSCatalog
    res = self.fc.removeFile(lfn1)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Successful'][lfn1][self.metaCatalog], True)
    self.assertEqual(res['Value']['Successful'][lfn1]['TSCatalog'], True)
    res = self.fc.removeFile(lfn2)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Successful'][lfn2][self.metaCatalog], True)
    self.assertEqual(res['Value']['Successful'][lfn2]['TSCatalog'], True)
    res = self.fc.removeFile(lfn3)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Successful'][lfn3][self.metaCatalog], True)
    self.assertEqual(res['Value']['Successful'][lfn3]['TSCatalog'], 'File does not exist')

    # Remove directories from DFC
    dirlist = [dirpath1, dirpath2]
    res = self.fc.removeDirectory(dirlist)
    self.assertTrue(res['OK'])

    # Remove metadata fields from DFC
    for MDField in MDFieldDict:
      res = self.fc.deleteMetadataField(MDField)
      self.assertTrue(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestTSDFCCatalogTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransformationClientChainID))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
