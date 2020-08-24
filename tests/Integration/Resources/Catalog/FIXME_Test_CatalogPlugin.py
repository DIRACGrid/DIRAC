#! /usr/bin/env python

# FIXME: it has to be seen if this is any useful
# FIXME: to bring back to life

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
from DIRAC.Core.Base.Script                             import parseCommandLine
parseCommandLine()
from DIRAC.Resources.Catalog.FileCatalog                import FileCatalog
from DIRAC.Core.Utilities.File                          import makeGuid
from DIRAC.Core.Utilities.Adler                         import stringAdler
from types                                              import *
import unittest,time,os,shutil,sys

if len(sys.argv) < 2:
  print('Usage: TestCatalogPlugIn.py CatalogClient')
  sys.exit()
else:
  catalogClientToTest = sys.argv[1]

class CatalogPlugInTestCase(unittest.TestCase):
  """ Base class for the CatalogPlugin test case """

  def setUp(self):
    self.fullMetadata = ['Status', 'ChecksumType', 'OwnerRole', 'CreationDate', 'Checksum', 'ModificationDate', 'OwnerDN', 'Mode', 'GUID', 'Size']
    self.dirMetadata = self.fullMetadata + ['NumberOfSubPaths']
    self.fileMetadata = self.fullMetadata + ['NumberOfLinks']

    self.catalog = FileCatalog(catalogs=[catalogClientToTest])
    valid = self.catalog.isOK()
    self.assertTrue(valid)
    self.destDir = '/lhcb/test/unit-test/TestCatalogPlugin'
    self.link = "%s/link" % self.destDir

    # Clean the existing directory
    self.cleanDirectory()
    res = self.catalog.createDirectory(self.destDir)
    returnValue = self.parseResult(res,self.destDir)

    # Register some files to work with
    self.numberOfFiles = 2
    self.files = []
    for i in xrange(self.numberOfFiles):
      lfn = "%s/testFile_%d" % (self.destDir,i)
      res = self.registerFile(lfn)
      self.assertTrue(res)
      self.files.append(lfn)

  def registerFile(self,lfn):
    pfn = 'protocol://host:port/storage/path%s' % lfn
    size = 10000000
    se = 'DIRAC-storage'
    guid = makeGuid()
    adler = stringAdler(guid)
    fileDict = {}
    fileDict[lfn] = {'PFN':pfn,'Size':size,'SE':se,'GUID':guid,'Checksum':adler}
    res = self.catalog.addFile(fileDict)
    return self.parseResult(res,lfn)

  def parseResult(self,res,path):
    self.assertTrue(res['OK'])
    self.assertTrue(res['Value'])
    self.assertTrue(res['Value']['Successful'])
    self.assertTrue(path in res['Value']['Successful'])
    return res['Value']['Successful'][path]

  def parseError(self,res,path):
    self.assertTrue(res['OK'])
    self.assertTrue(res['Value'])
    self.assertTrue(res['Value']['Failed'])
    self.assertTrue(path in res['Value']['Failed'])
    return res['Value']['Failed'][path]

  def cleanDirectory(self):
    res = self.catalog.exists(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    if not returnValue:
      return
    res = self.catalog.listDirectory(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    toRemove = list(returnValue['Files'])
    if toRemove:
      self.purgeFiles(toRemove)
    res = self.catalog.removeDirectory(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    self.assertTrue(returnValue)

  def purgeFiles(self,lfns):
    for lfn in lfns:
      res = self.catalog.getReplicas(lfn,True)
      replicas = self.parseResult(res,lfn)
      for se,pfn in replicas.items():
        repDict = {}
        repDict[lfn] = {'PFN':pfn,'SE':se}
        res = self.catalog.removeReplica(repDict)
        self.parseResult(res,lfn)
      res = self.catalog.removeFile(lfn)
      self.parseResult(res,lfn)

  def tearDown(self):
    self.cleanDirectory()

class FileTestCase(CatalogPlugInTestCase):

  def test_isFile(self):
    # Test isFile with a file
    res = self.catalog.isFile(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertTrue(returnValue)
    # Test isFile for missing path
    res = self.catalog.isFile(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")
    # Test isFile with a directory
    res = self.catalog.isFile(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    self.assertFalse(returnValue)

  def test_getFileMetadata(self):
    # Test getFileMetadata with a file
    res = self.catalog.getFileMetadata(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertEqual(returnValue['Status'],'-')
    self.assertEqual(returnValue['Size'],10000000)
    self.metadata = ['Status', 'ChecksumType', 'NumberOfLinks', 'CreationDate', 'Checksum', 'ModificationDate', 'Mode', 'GUID', 'Size']
    for key in self.metadata:
      self.assertTrue(key in returnValue)
    # Test getFileMetadata for missing path
    res = self.catalog.getFileMetadata(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")
    # Test getFileMetadata with a directory
    res = self.catalog.getFileMetadata(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    self.assertEqual(returnValue['Status'],'-')
    self.assertEqual(returnValue['Size'],0)
    self.metadata = ['Status', 'ChecksumType', 'NumberOfLinks', 'CreationDate', 'Checksum', 'ModificationDate', 'Mode', 'GUID', 'Size']
    for key in self.metadata:
      self.assertTrue(key in returnValue)

  def test_getFileSize(self):
    # Test getFileSize with a file
    res = self.catalog.getFileSize(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertEqual(returnValue,10000000)
    # Test getFileSize for missing path
    res = self.catalog.getFileSize(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")
    # Test getFileSize with a directory
    res = self.catalog.getFileSize(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    self.assertEqual(returnValue,0)

  def test_getReplicas(self):
    # Test getReplicas with a file
    res = self.catalog.getReplicas(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertEqual(returnValue.keys(),['DIRAC-storage'])
    self.assertEqual(returnValue.values(),['protocol://host:port/storage/path%s' % self.files[0]])
    # Test getReplicas for missing path
    res = self.catalog.getReplicas(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")
    # Test getReplicas with a directory
    res = self.catalog.getReplicas(self.destDir)
    error = self.parseError(res,self.destDir)
    # TODO return an error (currently 'File has zero replicas')
    #self.assertEqual(error,"Supplied path not a file")

  def test_getReplicaStatus(self):
    # Test getReplicaStatus with a file with existing replica
    replicaDict = {}
    replicaDict[self.files[0]] = 'DIRAC-storage'
    res = self.catalog.getReplicaStatus(replicaDict)
    returnValue = self.parseResult(res,self.files[0])
    self.assertEqual(returnValue,'U')
    # Test getReplicaStatus with a file with non-existing replica
    replicaDict = {}
    replicaDict[self.files[0]] = 'Missing'
    res = self.catalog.getReplicaStatus(replicaDict)
    error = self.parseError(res,self.files[0])
    self.assertEqual(error,"No replica at supplied site")
    # Test getReplicaStatus for missing path
    res = self.catalog.getReplicaStatus(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")
    # Test getReplicaStatus with a directory
    res = self.catalog.getReplicas(self.destDir)
    error = self.parseError(res,self.destDir)
    # TODO return an error (currently 'File has zero replicas')
    #self.assertEqual(error,"Supplied path not a file")

  def test_exists(self):
    # Test exists with a file
    res = self.catalog.exists(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertTrue(returnValue)
    # Test exists for missing path
    res = self.catalog.exists(self.files[0][:-1])
    returnValue = self.parseResult(res,self.files[0][:-1])
    self.assertFalse(returnValue)
    # Test exists with a directory
    res = self.catalog.exists(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    self.assertTrue(returnValue)

  def test_addReplica(self):
    # Test getReplicas with a file
    res = self.catalog.getReplicas(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertEqual(returnValue.keys(),['DIRAC-storage'])
    self.assertEqual(returnValue.values(),['protocol://host:port/storage/path%s' % self.files[0]])
    # Test the addReplica with a file
    registrationDict = {}
    registrationDict[self.files[0]] = {'SE':'DIRAC-storage2','PFN':'protocol2://host:port/storage/path%s' % self.files[0]}
    res = self.catalog.addReplica(registrationDict)
    returnValue = self.parseResult(res,self.files[0])
    self.assertTrue(returnValue)
    # Check the addReplica worked correctly
    res = self.catalog.getReplicas(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertEqual(sorted(returnValue.keys()),sorted(['DIRAC-storage','DIRAC-storage2']))
    self.assertEqual(sorted(returnValue.values()),sorted(['protocol://host:port/storage/path%s' % self.files[0], 'protocol2://host:port/storage/path%s' % self.files[0]]))
    # Test the addReplica with a non-existant file
    registrationDict = {}
    registrationDict[self.files[0][:-1]] = {'SE':'DIRAC-storage3','PFN':'protocol3://host:port/storage/path%s' % self.files[0]}
    res = self.catalog.addReplica(registrationDict)
    error = self.parseError(res,self.files[0][:-1])
    # TODO When the master fails it should return an error in FileCatalog
    #self.assertEqual(error,"No such file or directory")

  def test_setReplicaStatus(self):
    # Test setReplicaStatus with a file
    lfnDict = {}
    lfnDict[self.files[0]] = {'PFN': 'protocol://host:port/storage/path%s' % self.files[0],'SE':'DIRAC-storage' ,'Status':'P'}
    res = self.catalog.setReplicaStatus(lfnDict)
    returnValue = self.parseResult(res,self.files[0])
    self.assertTrue(returnValue)
    # Check the setReplicaStatus worked correctly
    res = self.catalog.getReplicas(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertFalse(returnValue)
    #time.sleep(2)
    # Test setReplicaStatus with a file
    lfnDict = {}
    lfnDict[self.files[0]] = {'PFN': 'protocol://host:port/storage/path%s' % self.files[0],'SE':'DIRAC-storage' ,'Status':'U'}
    res = self.catalog.setReplicaStatus(lfnDict)
    returnValue = self.parseResult(res,self.files[0])
    self.assertTrue(returnValue)
    # Check the setReplicaStatus worked correctly
    res = self.catalog.getReplicas(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertEqual(returnValue.keys(),['DIRAC-storage'])
    self.assertEqual(returnValue.values(),['protocol://host:port/storage/path%s' % self.files[0]])
    # Test setReplicaStatus with non-existant file
    lfnDict = {}
    lfnDict[self.files[0][:-1]] = {'PFN': 'protocol://host:port/storage/path%s' % self.files[0][:-1],'SE':'DIRAC-storage' ,'Status':'U'}
    res = self.catalog.setReplicaStatus(lfnDict)
    error = self.parseError(res,self.files[0][:-1])
    # TODO When the master fails it should return an error in FileCatalog
    #self.assertEqual(error,"No such file or directory")

  def test_setReplicaHost(self):
    # Test setReplicaHost with a file
    lfnDict = {}
    lfnDict[self.files[0]] = {'PFN': 'protocol://host:port/storage/path%s' % self.files[0],'SE':'DIRAC-storage' ,'NewSE':'DIRAC-storage2'}
    res = self.catalog.setReplicaHost(lfnDict)
    returnValue = self.parseResult(res,self.files[0])
    self.assertTrue(returnValue)
    # Check the setReplicaHost worked correctly
    res = self.catalog.getReplicas(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertEqual(returnValue.keys(),['DIRAC-storage2'])
    self.assertEqual(returnValue.values(),['protocol://host:port/storage/path%s' % self.files[0]])
    # Test setReplicaHost with non-existant file
    lfnDict = {}
    lfnDict[self.files[0][:-1]] = {'PFN': 'protocol://host:port/storage/path%s' % self.files[0][:-1],'SE':'DIRAC-storage' ,'NewSE':'DIRAC-storage2'}
    res = self.catalog.setReplicaHost(lfnDict)
    error = self.parseError(res,self.files[0][:-1])
    # TODO When the master fails it should return an error in FileCatalog
    #self.assertEqual(error,"No such file or directory")

class DirectoryTestCase(CatalogPlugInTestCase):

  def test_isDirectory(self):
    # Test isDirectory with a directory
    res = self.catalog.isDirectory(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    self.assertTrue(returnValue)
    # Test isDirectory with a file
    res = self.catalog.isDirectory(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertFalse(returnValue)
    # Test isDirectory for missing path
    res = self.catalog.isDirectory(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")

  def test_getDirectoryMetadata(self):
    # Test getDirectoryMetadata with a directory
    res = self.catalog.getDirectoryMetadata(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    self.assertEqual(returnValue['Status'],'-')
    self.assertEqual(returnValue['Size'],0)
    self.assertEqual(returnValue['NumberOfSubPaths'],self.numberOfFiles)
    for key in self.dirMetadata:
      self.assertTrue(key in returnValue)
    # Test getDirectoryMetadata with a file
    res = self.catalog.getDirectoryMetadata(self.files[0])
    returnValue = self.parseResult(res,self.files[0])
    self.assertEqual(returnValue['Status'],'-')
    self.assertEqual(returnValue['Size'],10000000)
    for key in self.dirMetadata:
      self.assertTrue(key in returnValue)
    # Test getDirectoryMetadata for missing path
    res = self.catalog.getDirectoryMetadata(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")

  def test_listDirectory(self):
    # Test listDirectory for directory
    res = self.catalog.listDirectory(self.destDir,True)
    returnValue = self.parseResult(res,self.destDir)
    self.assertEqual(returnValue.keys(),['Files','SubDirs','Links'])
    self.assertFalse(returnValue['SubDirs'])
    self.assertFalse(returnValue['Links'])
    self.assertEqual(sorted(returnValue['Files'].keys()),sorted(self.files))
    directoryFiles = returnValue['Files']
    for lfn,fileDict in directoryFiles.items():
      self.assertTrue('Replicas' in fileDict)
      self.assertEqual(len(fileDict['Replicas']),1)
      self.assertTrue('MetaData' in fileDict)
      for key in self.fileMetadata:
        self.assertTrue(key in fileDict['MetaData'])
    # Test listDirectory for a file
    res = self.catalog.listDirectory(self.files[0],True)
    error = self.parseError(res,self.files[0])
    self.assertEqual(error,"Not a directory")
    # Test listDirectory for missing path
    res = self.catalog.listDirectory(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")

  def test_getDirectoryReplicas(self):
    # Test getDirectoryReplicas for directory
    res = self.catalog.getDirectoryReplicas(self.destDir,True)
    returnValue = self.parseResult(res,self.destDir)
    self.assertTrue(self.files[0] in returnValue)
    fileReplicas = returnValue[self.files[0]]
    self.assertEqual(fileReplicas.keys(),['DIRAC-storage'])
    self.assertEqual(fileReplicas.values(),['protocol://host:port/storage/path%s' % self.files[0]])
    # Test getDirectoryReplicas for a file
    res = self.catalog.getDirectoryReplicas(self.files[0],True)
    error = self.parseError(res,self.files[0])
    self.assertEqual(error,"Not a directory")
    # Test getDirectoryReplicas for missing path
    res = self.catalog.getDirectoryReplicas(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")

  def test_getDirectorySize(self):
    # Test getDirectorySize for directory
    res = self.catalog.getDirectorySize(self.destDir)
    returnValue = self.parseResult(res,self.destDir)
    for key in ['Files','TotalSize','SubDirs','ClosedDirs','SiteUsage']:
      self.assertTrue(key in returnValue)
    self.assertEqual(returnValue['Files'],self.numberOfFiles)
    self.assertEqual(returnValue['TotalSize'],(self.numberOfFiles*10000000))
    #TODO create a sub dir, check, close it, check
    self.assertFalse(returnValue['SubDirs'])
    self.assertFalse(returnValue['ClosedDirs'])
    usage = returnValue['SiteUsage']
    self.assertEqual(usage.keys(),['DIRAC-storage'])
    self.assertEqual(usage['DIRAC-storage']['Files'],self.numberOfFiles)
    self.assertEqual(usage['DIRAC-storage']['Size'],(self.numberOfFiles*10000000))
    # Test getDirectorySize for a file
    res = self.catalog.getDirectorySize(self.files[0])
    error = self.parseError(res,self.files[0])
    self.assertEqual(error,"Not a directory")
    # Test getDirectorySize for missing path
    res = self.catalog.getDirectorySize(self.files[0][:-1])
    error = self.parseError(res,self.files[0][:-1])
    self.assertEqual(error,"No such file or directory")

class LinkTestCase(CatalogPlugInTestCase):
  #'createLink','removeLink','isLink','readLink'
  pass

class DatasetTestCase(CatalogPlugInTestCase):
  #'removeDataset','removeFileFromDataset','createDataset'
  pass

if __name__ == '__main__':
  #TODO getDirectoryMetadata and getFileMetadata should be merged
  #TODO Fix the return structure of write operations from FileCatalog
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(FileTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FileTestCase))
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
