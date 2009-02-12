#! /usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.Core.Utilities.File import getSize
import unittest,time,os,shutil,sys,types

if len(sys.argv) < 2:
  print 'Usage: TestStoragePlugIn.py StorageElement'
  sys.exit()
else:
  storageElementToTest = sys.argv[1]

class StorageElementTestCase(unittest.TestCase):
  """ Base class for the StorageElement test cases
  """
  def setUp(self):
    self.storageElement = StorageElement(storageElementToTest)
    self.localSourceFile = "/etc/group"
    self.localFileSize = getSize(self.localSourceFile)
    self.destDirectory = "/lhcb/test/unit-test/TestStorageElement"
    self.alternativeDestFileName = "testFile.%s" % time.time()
    self.alternativeLocal = "/tmp/storageElementTestFile.%s" % time.time()

  def tearDown(self):
    destinationDir = self.storageElement.getPfnForLfn(self.destDirectory)['Value']
    res = self.storageElement.removeDirectory(destinationDir,recursive=True,singleDirectory=True)
    self.assert_(res['OK'])

class GetInfoTestCase(StorageElementTestCase):

  def test_dump(self):
    print '\n\n#########################################################################\n\n\t\t\tDump test\n'
    self.storageElement.dump()

  def test_isValid(self):
    print '\n\n#########################################################################\n\n\t\t\tIs valid test\n'
    res = self.storageElement.isValid()
    self.assert_(res['OK'])

  def test_getRemoteProtocols(self):
    print '\n\n#########################################################################\n\n\t\t\tGet remote protocols test\n'
    res = self.storageElement.getRemoteProtocols()
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.ListType)

  def test_getLocalProtocols(self):
    print '\n\n#########################################################################\n\n\t\t\tGet local protocols test\n'
    res = self.storageElement.getLocalProtocols()
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.ListType)

  def test_getProtocols(self):
    print '\n\n#########################################################################\n\n\t\t\tGet protocols test\n'
    res = self.storageElement.getProtocols()
    self.assert_(res['OK'])
    self.assertEqual(type(res['Value']),types.ListType)

  def test_isLocalSE(self):
    print '\n\n#########################################################################\n\n\t\t\tIs local SE test\n'
    res = self.storageElement.isLocalSE()
    self.assert_(res['OK'])
    self.assertFalse(res['Value'])

  def test_getStorageElementOption(self):
    print '\n\n#########################################################################\n\n\t\t\tGet storage element option test\n'
    res = self.storageElement.getStorageElementOption('StorageBackend')
    self.assert_(res['OK'])
    self.assertEqual(res['Value'],'Castor')

  def test_getStorageParameters(self):
    print '\n\n#########################################################################\n\n\t\t\tGet storage parameters test\n'
    res = self.storageElement.getStorageParameters('SRM2')
    self.assert_(res['OK'])
    resDict = res['Value']
    self.assertEqual(resDict['Protocol'],'srm')
    self.assertEqual(resDict['SpaceToken'], 'LHCb_RAW')
    self.assertEqual(resDict['WSUrl'], '/srm/managerv2?SFN=')
    self.assertEqual(resDict['Host'], 'srm-lhcb.cern.ch')
    self.assertEqual(resDict['Path'], '/castor/cern.ch/grid')
    self.assertEqual(resDict['ProtocolName'],'SRM2')
    self.assertEqual(resDict['Port'],'8443')

class FileTestCases(StorageElementTestCase):

  def test_putFile(self):
    print '\n\n#########################################################################\n\n\t\t\tPut file test\n'

    destinationFilePath = '%s/testFile.%s' % (self.destDirectory,time.time())
    pfnForLfnRes = self.storageElement.getPfnForLfn(destinationFilePath)
    destinationPfn = pfnForLfnRes['Value']
    fileDict = {destinationPfn:self.localSourceFile}
    putFileRes = self.storageElement.putFile(fileDict,singleFile=True)
    # Now remove the destination file
    removeFileRes = self.storageElement.removeFile(destinationPfn,singleFile=True)

    # Check that the put was done correctly
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value'])
    self.assertEqual(putFileRes['Value'],self.localFileSize)
    # Check that the removal was done correctly
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value'])

  """

  def test_getFile(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file test\n'
    putFileRes = self.storageElement.putFile(self.localSourceFile,self.destDirectory,alternativeFileName=self.alternativeDestFileName)
    destFile = putFileRes['Value']
    getFileRes = self.storageElement.getFile(destFile,self.localFileSize,localPath=self.alternativeLocal)
    removeFileRes = self.storageElement.removeFile(destFile)
    if os.path.exists(self.alternativeLocal):
      os.remove(self.alternativeLocal)

    # Check that the put was done correctly
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value'])
    # Check that the get was done correctly
    self.assert_(getFileRes['OK'])   
    self.assert_(getFileRes['Value'])
    self.assertEqual(getFileRes['Value'],self.alternativeLocal)    
    # Check that the removal was done correctly
    self.assert_(removeFileRes['OK'])   
    self.assert_(removeFileRes['Value'])    
    self.assert_(removeFileRes['Value'].has_key('Successful'))
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    self.assert_(removeFileRes['Value']['Successful'][destFile])

  def test_getFileMetadata(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file metadata test\n'
    putFileRes = self.storageElement.putFile(self.localSourceFile,self.destDirectory,alternativeFileName=self.alternativeDestFileName)
    destFile = putFileRes['Value']
    getFileMetadataRes = self.storageElement.getFileMetadata(destFile)
    removeFileRes = self.storageElement.removeFile(destFile)

    # Check that the put was done correctly
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value'])
    # Check that the metadata was done correctly
    self.assert_(getFileMetadataRes['OK'])
    self.assert_(getFileMetadataRes['Value'])
    self.assert_(getFileMetadataRes['Value'].has_key('Successful'))
    self.assert_(getFileMetadataRes['Value']['Successful'].has_key(destFile))    
    metadataDict = getFileMetadataRes['Value']['Successful'][destFile]
    self.assert_(metadataDict['Cached'])
    self.assertFalse(metadataDict['Migrated'])
    self.assertEqual(metadataDict['Size'],self.localFileSize) 
    # Check that the removal was done correctly
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value'])
    self.assert_(removeFileRes['Value'].has_key('Successful'))
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    self.assert_(removeFileRes['Value']['Successful'][destFile])

  def test_getFileSize(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file size test\n'
    putFileRes = self.storageElement.putFile(self.localSourceFile,self.destDirectory,alternativeFileName=self.alternativeDestFileName)
    destFile = putFileRes['Value']
    getFileSizeRes = self.storageElement.getFileSize(destFile)
    removeFileRes = self.storageElement.removeFile(destFile)
    
    # Check that the put was done correctly
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value'])
    # Check that the metadata was done correctly
    self.assert_(getFileSizeRes['OK'])
    self.assert_(getFileSizeRes['Value'])
    self.assert_(getFileSizeRes['Value'].has_key('Successful'))
    self.assert_(getFileSizeRes['Value']['Successful'].has_key(destFile))
    self.assertEqual(getFileSizeRes['Value']['Successful'][destFile],self.localFileSize)
    # Check that the removal was done correctly
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value'])
    self.assert_(removeFileRes['Value'].has_key('Successful'))
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    self.assert_(removeFileRes['Value']['Successful'][destFile])

  def test_prestageFile(self):
    print '\n\n#########################################################################\n\n\t\t\tPrestage file test\n'
    putFileRes = self.storageElement.putFile(self.localSourceFile,self.destDirectory,alternativeFileName=self.alternativeDestFileName)
    destFile = putFileRes['Value']
    prestageFileRes = self.storageElement.prestageFile(destFile)
    removeFileRes = self.storageElement.removeFile(destFile)
    
    # Check that the put was done correctly
    self.assert_(putFileRes['OK'])   
    self.assert_(putFileRes['Value'])
    # Check that prestage was issued correctly
    self.assert_(prestageFileRes['OK'])   
    self.assert_(prestageFileRes['Value'])
    self.assert_(prestageFileRes['Value'].has_key('Successful'))
    self.assert_(prestageFileRes['Value']['Successful'].has_key(destFile))
    self.assert_(prestageFileRes['Value']['Successful'][destFile])
    # Check that the removal was done correctly
    self.assert_(removeFileRes['OK'])   
    self.assert_(removeFileRes['Value'])
    self.assert_(removeFileRes['Value'].has_key('Successful'))
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    self.assert_(removeFileRes['Value']['Successful'][destFile])

  def test_getAccessUrl(self):
    print '\n\n#########################################################################\n\n\t\t\tGet access url test\n'
    putFileRes = self.storageElement.putFile(self.localSourceFile,self.destDirectory,alternativeFileName=self.alternativeDestFileName)
    destFile = putFileRes['Value']
    getAccessRes = self.storageElement.getAccessUrl(destFile)
    removeFileRes = self.storageElement.removeFile(destFile)

    # Check that the put was done correctly
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value'])
    # Check that prestage was issued correctly
    self.assert_(getAccessRes['OK'])
    self.assert_(getAccessRes['Value'])
    self.assert_(getAccessRes['Value'].has_key('Successful'))
    self.assert_(getAccessRes['Value']['Successful'].has_key(destFile))
    self.assert_(getAccessRes['Value']['Successful'][destFile])
    # Check that the removal was done correctly
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value'])
    self.assert_(removeFileRes['Value'].has_key('Successful'))
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    self.assert_(removeFileRes['Value']['Successful'][destFile])

  """

class DirectoryTestCases(StorageElementTestCase):

  def test_createDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tCreate directory test\n'
    directory = "%s/%s" % (self.destDirectory,'createDirectoryTest')
    createDirRes =  self.storageElement.createDirectory(directory)
    
    # Check that the creation was done correctly
    self.assert_(createDirRes['OK'])
    self.assert_(createDirRes['Value'])
    self.assert_(createDirRes['Value'].has_key('Successful')) 
    self.assert_(createDirRes['Value']['Successful'].has_key(directory))
    self.assert_(createDirRes['Value']['Successful'][directory])
    destDir = createDirRes['Value']['Successful'][directory]
    # Remove the directory
    removeDirRes = self.storageElement.removeDirectory(destDir)
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value'])
    self.assert_(removeDirRes['Value'].has_key('Successful'))
    self.assert_(removeDirRes['Value']['Successful'].has_key(destDir))

  def test_listDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tList directory test\n'
    directory = "%s/%s" % (self.destDirectory,'listDirectoryTest')
    createDirRes =  self.storageElement.createDirectory(directory)
    putFileRes = self.storageElement.putFile(self.localSourceFile,directory,alternativeFileName=self.alternativeDestFileName)

    # Check that the creation was done correctly
    self.assert_(createDirRes['OK'])
    self.assert_(createDirRes['Value'])
    self.assert_(createDirRes['Value'].has_key('Successful'))
    self.assert_(createDirRes['Value']['Successful'].has_key(directory))
    self.assert_(createDirRes['Value']['Successful'][directory])
    destDir = createDirRes['Value']['Successful'][directory]
    # Check that the put was done correctly
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value'])
    destFile = putFileRes['Value']
    # Check that we can list the directory
    listDirRes = self.storageElement.listDirectory(destDir)
    self.assert_(listDirRes['OK'])
    self.assert_(listDirRes['Value'])
    self.assert_(listDirRes['Value'].has_key('Successful'))
    self.assert_(listDirRes['Value']['Successful'].has_key(destDir))
    self.assert_(listDirRes['Value']['Successful'][destDir].has_key('Files'))
    self.assert_(listDirRes['Value']['Successful'][destDir]['Files'].has_key(destFile))
    fileMetadata = listDirRes['Value']['Successful'][destDir]['Files'][destFile]
    self.assert_(fileMetadata['Cached'])
    self.assertFalse(fileMetadata['Migrated'])
    self.assertEqual(fileMetadata['Size'],self.localFileSize)

    # Remove the directory
    removeDirRes = self.storageElement.removeDirectory(destDir)   
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']) 
    self.assert_(removeDirRes['Value'].has_key('Successful'))
    self.assert_(removeDirRes['Value']['Successful'].has_key(destDir))

  def test_getDirectoryMetadata(self):
    print '\n\n#########################################################################\n\n\t\t\tDirectory metadata test\n'
    directory = "%s/%s" % (self.destDirectory,'getDirectoryMetadataTest')
    createDirRes =  self.storageElement.createDirectory(directory)

    # Check that the creation was done correctly
    self.assert_(createDirRes['OK'])
    self.assert_(createDirRes['Value'])
    self.assert_(createDirRes['Value'].has_key('Successful'))
    self.assert_(createDirRes['Value']['Successful'].has_key(directory))
    self.assert_(createDirRes['Value']['Successful'][directory])
    destDir = createDirRes['Value']['Successful'][directory]
    # Check that we can get the directory metadata
    metadataDirRes = self.storageElement.getDirectoryMetadata(destDir)
    self.assert_(metadataDirRes['OK'])
    self.assert_(metadataDirRes['Value'])
    self.assert_(metadataDirRes['Value'].has_key('Successful'))
    self.assert_(metadataDirRes['Value']['Successful'].has_key(destDir))
    self.assert_(metadataDirRes['Value']['Successful'][destDir].has_key('Permissions'))
    # Remove the directory
    removeDirRes = self.storageElement.removeDirectory(destDir)
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value'])
    self.assert_(removeDirRes['Value'].has_key('Successful'))   
    self.assert_(removeDirRes['Value']['Successful'].has_key(destDir))

  def test_getDirectorySize(self):
    print '\n\n#########################################################################\n\n\t\t\tGet directory size test\n'
    directory = "%s/%s" % (self.destDirectory,'getDirectorySizeTest')
    createDirRes =  self.storageElement.createDirectory(directory)
    putFileRes = self.storageElement.putFile(self.localSourceFile,directory,alternativeFileName=self.alternativeDestFileName)
    
    # Check that the creation was done correctly
    self.assert_(createDirRes['OK'])
    self.assert_(createDirRes['Value'])
    self.assert_(createDirRes['Value'].has_key('Successful'))
    self.assert_(createDirRes['Value']['Successful'].has_key(directory))
    self.assert_(createDirRes['Value']['Successful'][directory])
    destDir = createDirRes['Value']['Successful'][directory]
    # Check that the put was done correctly
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value'])
    destFile = putFileRes['Value']
    # Check that we can get the size of the directory
    listDirRes = self.storageElement.getDirectorySize(destDir)
    self.assert_(listDirRes['OK'])  
    self.assert_(listDirRes['Value'])  
    self.assert_(listDirRes['Value'].has_key('Successful'))  
    self.assert_(listDirRes['Value']['Successful'].has_key(destDir))
    dirSizeDict = listDirRes['Value']['Successful'][destDir]
    self.assert_(dirSizeDict.has_key('Files'))
    self.assertEqual(dirSizeDict['Files'],1)
    self.assert_(dirSizeDict.has_key('Size'))
    self.assertEqual(dirSizeDict['Size'],self.localFileSize)
    # Remove the directory
    removeDirRes = self.storageElement.removeDirectory(destDir)
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value'])
    self.assert_(removeDirRes['Value'].has_key('Successful'))
    self.assert_(removeDirRes['Value']['Successful'].has_key(destDir))

if __name__ == '__main__':
  #suite = unittest.defaultTestLoader.loadTestsFromTestCase(GetInfoTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCases))
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FileTestCases))
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(FileTestCases)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

