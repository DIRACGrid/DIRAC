#! /usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.DataManagementSystem.Client.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.File import getSize
import unittest,time,os,shutil,sys
from types import *

if len(sys.argv) < 3:
  print 'Usage: TestStoragePlugIn.py StorageElement protocol'
  sys.exit()
else:
  storageElementToTest = sys.argv[1] 
  protocol = sys.argv[2]

class StoragePlugInTestCase(unittest.TestCase):
  """ Base class for the StoragePlugin test cases
  """
  def setUp(self):

    factory = StorageFactory()
    res = factory.getStorages(storageElementToTest, [protocol])    
    self.assert_(res['OK'])
    storageDetails = res['Value']
    self.storage = storageDetails['StorageObjects'][0]
    self.storage.changeDirectory('lhcb/test/unit-test')
    print '\n\n#########################################################################\n\n\t\t\tCreate Directory test\n'
    destDir = self.storage.getCurrentURL('')['Value']
    res = self.storage.createDirectory(destDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destDir))
    self.assert_(res['Value']['Successful'][destDir])
    self.numberOfFiles = 1

  def tearDown(self):
    remoteDir = self.storage.getCurrentURL('')['Value']
    ignore = self.storage.removeDirectory(remoteDir,True)     

class DirectoryTestCase(StoragePlugInTestCase):

  def test_isDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tIs Directory test\n'
    # Test that we can determine what is a directory
    destDir = self.storage.getCurrentURL('')['Value']
    isDirRes = self.storage.isDirectory(destDir)
    # Test that we can determine that a directory is not a directory
    dummyDir = self.storage.getCurrentURL('NonExistantFile')['Value']
    nonExistantDirRes = self.storage.isDirectory(dummyDir)

    # Check the is directory operation
    self.assert_(isDirRes['OK'])
    self.assert_(isDirRes['Value']['Successful'].has_key(destDir))
    self.assert_(isDirRes['Value']['Successful'][destDir])
    # Check the non existant directory operation
    self.assert_(nonExistantDirRes['OK'])
    self.assert_(nonExistantDirRes['Value']['Successful'].has_key(dummyDir))
    self.assertFalse(nonExistantDirRes['Value']['Successful'][dummyDir])

  def test_putRemoveDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tPut Directory test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('')['Value']
    ignore = self.storage.removeDirectory(remoteDir,True)

    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(self.numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)

    # Check that we can successfully upload the directory to the storage element
    dirDict = {remoteDir:localDir}
    putDirRes = self.storage.putDirectory(dirDict)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir,True)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    shutil.rmtree(localDir)

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    self.assertEqual(putDirRes['Value']['Successful'][remoteDir]['Files'],self.numberOfFiles)
    self.assertEqual(putDirRes['Value']['Successful'][remoteDir]['Size'],self.numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the remove dir operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    self.assertEqual(removeDirRes['Value']['Successful'][remoteDir]['FilesRemoved'],self.numberOfFiles)
    self.assertEqual(removeDirRes['Value']['Successful'][remoteDir]['SizeRemoved'],self.numberOfFiles*sizeOfLocalFile)

  def test_putGetDirectoryMetadata(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Directory Metadata test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('')['Value']
    ignore = self.storage.removeDirectory(remoteDir,True)
    
    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(self.numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)
    
    # Check that we can successfully upload the directory to the storage element
    dirDict = {remoteDir:localDir}
    putDirRes = self.storage.putDirectory(dirDict)

    # Get the directory metadata
    getMetadataRes = self.storage.getDirectoryMetadata(remoteDir)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir,True)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    shutil.rmtree(localDir)

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = putDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],self.numberOfFiles)
    self.assertEqual(resDict['Size'],self.numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the get metadata operation
    self.assert_(getMetadataRes['OK'])
    self.assert_(getMetadataRes['Value']['Successful'].has_key(remoteDir))
    resDict = getMetadataRes['Value']['Successful'][remoteDir]
    self.assert_(resDict.has_key('Permissions'))
    self.assert_(type(resDict['Permissions']) == IntType)
    # Perform the checks for the remove directory operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = removeDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['FilesRemoved'],self.numberOfFiles)
    self.assertEqual(resDict['SizeRemoved'],self.numberOfFiles*sizeOfLocalFile)

  def test_putGetDirectorySize(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Directory Size test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('')['Value']
    ignore = self.storage.removeDirectory(remoteDir,True)

    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(self.numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)
    # Check that we can successfully upload the directory to the storage element
    dirDict = {remoteDir:localDir}
    putDirRes = self.storage.putDirectory(dirDict)
    # Now get the directory size
    getDirSizeRes = self.storage.getDirectorySize(remoteDir)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir,True)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    shutil.rmtree(localDir)

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = putDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],self.numberOfFiles)
    self.assertEqual(resDict['Size'],self.numberOfFiles*sizeOfLocalFile)
    #Now perform the checks for the get directory size operation
    self.assert_(getDirSizeRes['OK'])
    self.assert_(getDirSizeRes['Value']['Successful'].has_key(remoteDir))
    resDict = getDirSizeRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Size'],self.numberOfFiles*sizeOfLocalFile)
    self.assertEqual(resDict['Files'],self.numberOfFiles)
    # Perform the checks for the remove directory operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = removeDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['FilesRemoved'],self.numberOfFiles)
    self.assertEqual(resDict['SizeRemoved'],self.numberOfFiles*sizeOfLocalFile)

  def test_putListDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tList Directory test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('')['Value']
    ignore = self.storage.removeDirectory(remoteDir,True)

    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(self.numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)
    # Check that we can successfully upload the directory to the storage element
    dirDict = {remoteDir:localDir}
    putDirRes = self.storage.putDirectory(dirDict)
    # List the remote directory
    listDirRes = self.storage.listDirectory(remoteDir)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir,True)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    shutil.rmtree(localDir)

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = putDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],self.numberOfFiles)
    self.assertEqual(resDict['Size'],self.numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the list dir operation
    self.assert_(listDirRes['OK'])
    self.assert_(listDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = listDirRes['Value']['Successful'][remoteDir]
    self.assert_(resDict.has_key('SubDirs'))
    self.assert_(resDict.has_key('Files'))
    self.assertEqual(len(resDict['Files'].keys()), self.numberOfFiles)
    # Perform the checks for the remove directory operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = removeDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['FilesRemoved'],self.numberOfFiles)
    self.assertEqual(resDict['SizeRemoved'],self.numberOfFiles*sizeOfLocalFile)

  def test_putGetDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Directory test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('')['Value']
    ignore = self.storage.removeDirectory(remoteDir,True)

    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(self.numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)
    # Check that we can successfully upload the directory to the storage element
    dirDict = {remoteDir:localDir}
    putDirRes = self.storage.putDirectory(dirDict)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    shutil.rmtree(localDir)
    # Check that we can get directories from the storage element
    getDirRes = self.storage.getDirectory(remoteDir,localPath=localDir)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir,True)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    shutil.rmtree(localDir)    

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = putDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],self.numberOfFiles)
    self.assertEqual(resDict['Size'],self.numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the get dir operation
    self.assert_(getDirRes['OK'])
    self.assert_(getDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = getDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],self.numberOfFiles)
    self.assertEqual(resDict['Size'],self.numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the remove directory operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = removeDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['FilesRemoved'],self.numberOfFiles)
    self.assertEqual(resDict['SizeRemoved'],self.numberOfFiles*sizeOfLocalFile)

class FileTestCase(StoragePlugInTestCase):

  def test_putRemoveFile(self):
    print '\n\n#########################################################################\n\n\t\t\tPut and Remove test\n'

    # Make sure that we can actually upload a file properly
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileDict = {destFile:srcFile}
    putFileRes = self.storage.putFile(fileDict)
    # Make sure we are able to remove the file
    removeFileRes = self.storage.removeFile(destFile)

    # Check the successful put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(destFile))
    self.assertEqual(putFileRes['Value']['Successful'][destFile],srcFileSize)
    # Check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    self.assert_(removeFileRes['Value']['Successful'][destFile])

  def test_putGetFile(self):
    print '\n\n#########################################################################\n\n\t\t\tPut and Get test\n'

    # First upload a file to the storage
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    remoteFile = self.storage.getCurrentURL(testFileName)['Value']   
    fileDict = {remoteFile:srcFile}
    putFileRes = self.storage.putFile(fileDict)

    # Then make sure we can get a local copy of the file
    getFileRes = self.storage.getFile(remoteFile)
    # Cleanup the remote mess
    removeFileRes = self.storage.removeFile(remoteFile)
    #Cleanup the mess locally
    os.remove(testFileName)

    # Check the put operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(remoteFile))
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # Check the get operation
    self.assert_(getFileRes['OK'])
    self.assert_(getFileRes['Value']['Successful'].has_key(remoteFile))
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # Check the remove operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(removeFileRes['Value']['Successful'][remoteFile])

  def test_putExistsFile(self):
    print '\n\n#########################################################################\n\n\t\t\tExists test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    remoteFile = self.storage.getCurrentURL(testFileName)['Value']
    fileDict = {remoteFile:srcFile}
    putFileRes = self.storage.putFile(fileDict)
    # Then get the file's existance
    existsFileRes = self.storage.exists(remoteFile)
    # Now remove the file
    removeFileRes = self.storage.removeFile(remoteFile)
    # Check  again that the file exists
    failedExistRes = self.storage.exists(remoteFile)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(remoteFile))
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # Check the exists operation
    self.assert_(existsFileRes['OK'])
    self.assert_(existsFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(existsFileRes['Value']['Successful'][remoteFile])
    # Check the removal operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(remoteFile))
    # Check the failed exists operation
    self.assert_(failedExistRes['OK'])
    self.assert_(failedExistRes['Value']['Successful'].has_key(remoteFile))
    self.assertFalse(failedExistRes['Value']['Successful'][remoteFile])

  def test_putIsFile(self):
    print '\n\n#########################################################################\n\n\t\t\tIs file test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    remoteFile = self.storage.getCurrentURL(testFileName)['Value']
    fileDict = {remoteFile:srcFile}
    putFileRes = self.storage.putFile(fileDict)
    # Check we are able to determine that it is a file
    isFileRes = self.storage.isFile(remoteFile)
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(remoteFile)
    # Check that everything isn't a file
    remoteDir = os.path.dirname(remoteFile)
    failedIsFileRes = self.storage.isFile(remoteDir)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(putFileRes['Value']['Successful'][remoteFile])
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # Check the is file operation
    self.assert_(isFileRes['OK'])
    self.assert_(isFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(isFileRes['Value']['Successful'][remoteFile])
    # check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(remoteFile))
    # Check that the directory is not a file
    self.assert_(failedIsFileRes['OK'])
    self.assert_(failedIsFileRes['Value']['Successful'].has_key(remoteDir))
    self.assertFalse(failedIsFileRes['Value']['Successful'][remoteDir])

  def test_putGetFileMetaData(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file metadata test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    remoteFile = self.storage.getCurrentURL(testFileName)['Value']
    fileDict = {remoteFile:srcFile}
    putFileRes = self.storage.putFile(fileDict)
    # Check that we can get the file metadata
    getMetadataRes = self.storage.getFileMetadata(remoteFile)
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(remoteFile)
    # See what happens with non existant files
    failedMetadataRes = self.storage.getFileMetadata(remoteFile)
    # Check directories are handled properly
    remoteDir = os.path.dirname(remoteFile)
    directoryMetadataRes = self.storage.getFileMetadata(remoteDir)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(putFileRes['Value']['Successful'][remoteFile])
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # Check the get metadata operation
    self.assert_(getMetadataRes['OK'])
    self.assert_(getMetadataRes['Value']['Successful'].has_key(remoteFile))
    fileMetaData = getMetadataRes['Value']['Successful'][remoteFile]
    self.assert_(fileMetaData['Cached'])
    self.assertFalse(fileMetaData['Migrated'])
    self.assertEqual(fileMetaData['Size'],srcFileSize)
    # check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(remoteFile))
    # Check the get metadata for non existant file
    self.assert_(failedMetadataRes['OK'])
    self.assert_(failedMetadataRes['Value']['Failed'].has_key(remoteFile))
    expectedError = "File does not exist"
    self.assert_(expectedError in failedMetadataRes['Value']['Failed'][remoteFile])
    # Check that metadata operation with a directory
    self.assert_(directoryMetadataRes['OK'])
    self.assert_(directoryMetadataRes['Value']['Failed'].has_key(remoteDir))
    expectedError = "Supplied path is not a file"
    self.assert_(expectedError in directoryMetadataRes['Value']['Failed'][remoteDir])

  def test_putGetFileSize(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file size test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    remoteFile = self.storage.getCurrentURL(testFileName)['Value']
    fileDict = {remoteFile:srcFile}  
    putFileRes = self.storage.putFile(fileDict)
    # Check that we can get the file size
    getSizeRes = self.storage.getFileSize(remoteFile)
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(remoteFile)
    # Check non existant files
    failedSizeRes = self.storage.getFileMetadata(remoteFile)
    # Check directories are handled properly
    remoteDir = os.path.dirname(remoteFile)
    directorySizeRes = self.storage.getFileSize(remoteDir)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(putFileRes['Value']['Successful'][remoteFile])
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # Check that we got the file size correctly
    self.assert_(getSizeRes['OK'])
    self.assert_(getSizeRes['Value']['Successful'].has_key(remoteFile))
    self.assertEqual(getSizeRes['Value']['Successful'][remoteFile],srcFileSize)
    # check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(remoteFile))
    # Check the get size with non existant file works properly
    self.assert_(failedSizeRes['OK'])
    self.assert_(failedSizeRes['Value']['Failed'].has_key(remoteFile))
    expectedError = "File does not exist"
    self.assert_(expectedError in failedSizeRes['Value']['Failed'][remoteFile])
    # Check that the passing a directory is handled correctly
    self.assert_(directorySizeRes['OK'])
    self.assert_(directorySizeRes['Value']['Failed'].has_key(remoteDir))
    expectedError = "Supplied path is not a file"
    self.assert_(expectedError in directorySizeRes['Value']['Failed'][remoteDir])

  def test_putPrestageFile(self):
    print '\n\n#########################################################################\n\n\t\t\tFile prestage test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)   
    testFileName = 'testFile.%s' % time.time()
    remoteFile = self.storage.getCurrentURL(testFileName)['Value']
    fileDict = {remoteFile:srcFile}
    putFileRes = self.storage.putFile(fileDict)
    # Check that we can issue a stage request
    prestageRes = self.storage.prestageFile(remoteFile)
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(remoteFile)
    # Check what happens with deleted files
    deletedPrestageRes = self.storage.prestageFile(remoteFile)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(putFileRes['Value']['Successful'][remoteFile])
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # Check the prestage file operation
    self.assert_(prestageRes['OK'])
    self.assert_(prestageRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(prestageRes['Value']['Successful'][remoteFile])
    # Check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(remoteFile))
    # Check that pre-staging non-existant file fails
    self.assert_(deletedPrestageRes['OK'])
    self.assert_(deletedPrestageRes['Value']['Failed'].has_key(remoteFile))
    expectedError = "SRM2Storage.prestageFile: Failed issue stage request. [SE][BringOnline] No such file or directory"
    self.assertEqual(deletedPrestageRes['Value']['Failed'][remoteFile],expectedError)
 
  def test_putFilegetTransportURL(self):
    print '\n\n#########################################################################\n\n\t\t\tGet tURL test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    remoteFile = self.storage.getCurrentURL(testFileName)['Value']
    fileDict = {remoteFile:srcFile}
    putFileRes = self.storage.putFile(fileDict)
    #Check that we can get a turl
    getTurlRes = self.storage.getTransportURL(remoteFile)
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(remoteFile)
    # Try and get a turl for a non existant file
    failedGetTurlRes = self.storage.getTransportURL(remoteFile)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(putFileRes['Value']['Successful'][remoteFile])
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # check the get turl operation
    self.assert_(getTurlRes['OK'])
    self.assert_(getTurlRes['Value']['Successful'].has_key(remoteFile))
    # check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(remoteFile))
    #Check the get turl with non existant file operation
    self.assert_(failedGetTurlRes['OK'])
    self.assert_(failedGetTurlRes['Value']['Failed'].has_key(remoteFile))
    expectedError = "SRM2Storage.getTransportURL: File does not exist."
    self.assertEqual(failedGetTurlRes['Value']['Failed'][remoteFile],expectedError)

  def test_putPinRelease(self):
    print '\n\n#########################################################################\n\n\t\t\tPin and Release test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    remoteFile = self.storage.getCurrentURL(testFileName)['Value']
    fileDict = {remoteFile:srcFile}
    putFileRes = self.storage.putFile(fileDict)
    # Check that we can pin the file
    pinFileRes = self.storage.pinFile(remoteFile)
    srmID=''
    if pinFileRes['OK']:
      if pinFileRes['Value']['Successful'].has_key(remoteFile):
        srmID = pinFileRes['Value']['Successful'][remoteFile]
    # Check that we can release the file
    releaseFileRes = self.storage.releaseFile({remoteFile:srmID})
    # Clean up the mess
    removeFileRes = self.storage.removeFile(remoteFile)
   
    # Check the put file operation 
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(putFileRes['Value']['Successful'][remoteFile])
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # Check the pin file operation
    self.assert_(pinFileRes['OK'])
    self.assert_(pinFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(type(pinFileRes['Value']['Successful'][remoteFile]) in StringTypes)
    # Check the release file operation
    self.assert_(releaseFileRes['OK'])
    self.assert_(releaseFileRes['Value']['Successful'].has_key(remoteFile))
    # check the remove file operation
    self.assert_(removeFileRes['OK'])   
    self.assert_(removeFileRes['Value']['Successful'].has_key(remoteFile))


  def test_putPrestageStatus(self):
    print '\n\n#########################################################################\n\n\t\t\tPrestage status test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    srcFileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time() 
    remoteFile = self.storage.getCurrentURL(testFileName)['Value']
    fileDict = {remoteFile:srcFile}
    putFileRes = self.storage.putFile(fileDict)
    # Check that we can issue a stage request
    prestageRes = self.storage.prestageFile(remoteFile)
    srmID = ''
    if prestageRes['OK']:
      if prestageRes['Value']['Successful'].has_key(remoteFile):
        srmID = prestageRes['Value']['Successful'][remoteFile]
    # Take a quick break to allow the SRM to realise the file is available
    sleepTime = 10
    print 'Sleeping for %s seconds' % sleepTime
    time.sleep(sleepTime)
    # Check that we can monitor the stage request
    prestageStatusRes = self.storage.prestageFileStatus({remoteFile:srmID})        
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(remoteFile)
    
    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(putFileRes['Value']['Successful'][remoteFile])
    self.assertEqual(putFileRes['Value']['Successful'][remoteFile],srcFileSize)
    # Check the prestage file operation
    self.assert_(prestageRes['OK'])
    self.assert_(prestageRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(prestageRes['Value']['Successful'][remoteFile])
    self.assert_(type(prestageRes['Value']['Successful'][remoteFile]) in StringTypes)
    # Check the prestage status operation
    self.assert_(prestageStatusRes['OK'])
    self.assert_(prestageStatusRes['Value']['Successful'].has_key(remoteFile))
    self.assert_(prestageStatusRes['Value']['Successful'][remoteFile])
    # Check the remove file operation   
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(remoteFile))

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(FileTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
  #suite = unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

