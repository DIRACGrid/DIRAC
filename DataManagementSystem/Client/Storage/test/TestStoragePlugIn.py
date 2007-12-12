import unittest,types,time,os,shutil
from DIRAC.DataManagementSystem.Client.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.File import getSize

class StoragePlugInTestCase(unittest.TestCase):
  """ Base class for the StoragePlugin test cases
  """
  def setUp(self):
    factory = StorageFactory()
    res = factory.getStorages('CERN-RAW', ['SRM2'])
    self.assert_(res['OK'])
    storageDetails = res['Value']
    self.storage = storageDetails['StorageObjects'][0]
    self.storage.changeDirectory('lhcb/test/unit-test')

  def test_createUnitTestDir(self):
    print '\n\n#########################################################################\n\n\t\t\tCreate Directory test\n'
    destDir = self.storage.getCurrentURL('')['Value']
    res = self.storage.createDirectory(destDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destDir))
    self.assert_(res['Value']['Successful'][destDir])

class DirectoryTestCase(StoragePlugInTestCase):

  def test_isDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tIs Directory test\n'
    # Test that we can determine what is a directory
    destDir = self.storage.getCurrentURL('')['Value']
    isDirRes = self.storage.isDirectory(destDir)
    # Test that we can determine that a directory is not a directory
    destDir = self.storage.getCurrentURL('NonExistantFile')['Value']
    nonExistantDirRes = self.storage.isDirectory(destDir)

    # Check the is directory operation
    self.assert_(isDirRes['OK'])
    self.assert_(isDirRes['Value']['Successful'].has_key(destDir))
    self.assert_(isDirRes['Value']['Successful'][destDir])
    # Check the non existant directory operation
    self.assert_(nonExistantDirRes['OK'])
    self.assert_(nonExistantDirRes['Value']['Successful'].has_key(destDir))
    self.assertFalse(nonExistantDirRes['Value']['Successful'][destDir])

  def test_putRemoveDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tPut Directory test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']
    ignore = self.storage.removeDirectory(remoteDir)

    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    numberOfFiles = 5
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)
    # Check that we can successfully upload the directory to the storage element
    dirTuple = (localDir,remoteDir)
    putDirRes = self.storage.putDirectory(dirTuple)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = putDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the remove dir operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = removeDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)


  def test_putGetDirectoryMetadata(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Directory Metadata test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']
    ignore = self.storage.removeDirectory(remoteDir)

    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    numberOfFiles = 5
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)
    # Check that we can successfully upload the directory to the storage element
    dirTuple = (localDir,remoteDir)
    putDirRes = self.storage.putDirectory(dirTuple)
    # Get the directory metadata
    getMetadataRes = self.storage.getDirectoryMetadata(remoteDir)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = putDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the get metadata operation
    self.assert_(getMetadataRes['OK'])
    self.assert_(getMetadataRes['Value']['Successful'].has_key(remoteDir))
    resDict = getMetadataRes['Value']['Successful'][remoteDir]
    self.assert_(resDict.has_key('Permissions'))
    self.assertEqual(resDict['Permissions'],493)
    # Perform the checks for the remove directory operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = removeDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)


  def test_putGetDirectorySize(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Directory Size test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']
    ignore = self.storage.removeDirectory(remoteDir)

    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    numberOfFiles = 5
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)
    # Check that we can successfully upload the directory to the storage element
    dirTuple = (localDir,remoteDir)
    putDirRes = self.storage.putDirectory(dirTuple)
    # Now get the directory size
    getDirSizeRes = self.storage.getDirectorySize(remoteDir)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = putDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    #Now perform the checks for the get directory size operation
    self.assert_(getDirSizeRes['OK'])
    self.assert_(getDirSizeRes['Value']['Successful'].has_key(remoteDir))
    resDict = getDirSizeRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    self.assertEqual(resDict['Files'],numberOfFiles)
    # Perform the checks for the remove directory operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = removeDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)


  def test_putListDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tList Directory test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']
    ignore = self.storage.removeDirectory(remoteDir)

    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    numberOfFiles = 5
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)
    # Check that we can successfully upload the directory to the storage element
    dirTuple = (localDir,remoteDir)
    putDirRes = self.storage.putDirectory(dirTuple)
    # List the remote directory
    listDirRes = self.storage.listDirectory(remoteDir)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = putDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the list dir operation
    self.assert_(listDirRes['OK'])
    self.assert_(listDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = listDirRes['Value']['Successful'][remoteDir]
    self.assert_(resDict.has_key('SubDirs'))
    self.assert_(resDict.has_key('Files'))
    self.assertEqual(len(resDict['Files'].keys()), numberOfFiles)
    # Perform the checks for the remove directory operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = removeDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)

  def test_putGetDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Directory test\n'
    # First clean the remote directory incase something was left there
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']
    ignore = self.storage.removeDirectory(remoteDir)

    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    numberOfFiles = 5
    sizeOfLocalFile = getSize(srcFile)
    print 'Creating local directory: %s' % localDir
    if not os.path.exists(localDir):
      os.mkdir(localDir)
    for i in range(numberOfFiles):
      shutil.copy(srcFile,'%s/testFile.%s' % (localDir,time.time()))
      time.sleep(1)
    # Check that we can successfully upload the directory to the storage element
    dirTuple = (localDir,remoteDir)
    putDirRes = self.storage.putDirectory(dirTuple)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)
    # Check that we can get directories from the storage element
    directoryTuple = (remoteDir,localDir)
    getDirRes = self.storage.getDirectory(directoryTuple)
    # Now remove the remove directory
    removeDirRes = self.storage.removeDirectory(remoteDir)
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

    # Perform the checks for the put dir operation
    self.assert_(putDirRes['OK'])
    self.assert_(putDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = putDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the get dir operation
    self.assert_(getDirRes['OK'])
    self.assert_(getDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = getDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    # Perform the checks for the remove directory operation
    self.assert_(removeDirRes['OK'])
    self.assert_(removeDirRes['Value']['Successful'].has_key(remoteDir))
    resDict = removeDirRes['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)

class FileTestCase(StoragePlugInTestCase):

  def test_putRemoveFile(self):
    print '\n\n#########################################################################\n\n\t\t\tPut and Remove test\n'
    # First test that we are able to determine whether the file sizes of the transfer don't match
    srcFile = '/etc/group'
    fileSize = 10 #This is a made up value
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    failedPutFileRes = self.storage.putFile(fileTuple)
    # Now make sure that we can actually upload a file properly
    fileSize = getSize(srcFile)
    fileTuple = (srcFile,destFile,fileSize)
    putFileRes = self.storage.putFile(fileTuple)
    # Make sure we are able to remove the file
    removeFileRes = self.storage.removeFile(destFile)

    # Check the failed put file operation
    self.assert_(failedPutFileRes['OK'])
    self.assert_(failedPutFileRes['Value']['Failed'].has_key(destFile))
    expectedError =  'SRM2Storage.putFile: Source and destination file sizes do not match.'
    self.assertEqual(failedPutFileRes['Value']['Failed'][destFile],expectedError)
    # Check the successful put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(destFile))
    # Check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))

  def test_putGetFile(self):
    print '\n\n#########################################################################\n\n\t\t\tPut and Get test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    putRes = self.storage.putFile(fileTuple)
    # Make sure we can detect when the file transferred incorrectly
    destLocalFile = "%s/%s.%s" % (os.getcwd(),'unitTestFile',time.time())
    wrongSize = 10
    fileTuple = (destFile,destLocalFile,wrongSize)
    failedGetRes = self.storage.getFile(fileTuple)
    # Then make sure we can get a local copy of the file
    destLocalFile = "%s/%s.%s" % (os.getcwd(),'unitTestFile',time.time())
    fileTuple = (destFile,destLocalFile,fileSize)
    getFileRes = self.storage.getFile(fileTuple)
    # Cleanup the remote mess
    removeFileRes = self.storage.removeFile(destFile)
    #Cleanup the mess locally
    os.remove(destLocalFile)

    # Check the put operation
    self.assert_(putRes['OK'])
    self.assert_(putRes['Value']['Successful'].has_key(destFile))
    # Check the failed get operation
    self.assert_(failedGetRes['OK'])
    self.assert_(failedGetRes['Value']['Failed'].has_key(destFile))
    expectedError = 'SRM2Storage.getFile: Source and destination file sizes do not match.'
    self.assertEqual(failedGetRes['Value']['Failed'][destFile],expectedError)
    # Check the get operation
    self.assert_(getFileRes['OK'])
    self.assert_(getFileRes['Value']['Successful'].has_key(destFile))
    # Check the remove operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))

  def test_putExistsFile(self):
    print '\n\n#########################################################################\n\n\t\t\tExists test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    putFileRes = self.storage.putFile(fileTuple)
    # Then get the file's existance
    existsFileRes = self.storage.exists(destFile)
    # Now remove the file
    removeFileRes = self.storage.removeFile(destFile)
    # Check  again that the file exists
    failedExistRes = self.storage.exists(destFile)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(destFile))
    # Check the exists operation
    self.assert_(existsFileRes['OK'])
    self.assert_(existsFileRes['Value']['Successful'].has_key(destFile))
    self.assert_(existsFileRes['Value']['Successful'][destFile])
    # Check the removal operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    # Check the failed exists operation
    self.assert_(failedExistRes['OK'])
    self.assert_(failedExistRes['Value']['Successful'].has_key(destFile))
    self.assertFalse(failedExistRes['Value']['Successful'][destFile])

  def test_putIsFile(self):
    print '\n\n#########################################################################\n\n\t\t\tIs file test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    putFileRes = self.storage.putFile(fileTuple)
    # Check we are able to determine that it is a file
    isFileRes = self.storage.isFile(destFile)
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(destFile)
    # Check that everything isn't a file
    destDir = os.path.dirname(destFile)
    failedIsFileRes = self.storage.isFile(destDir)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(destFile))
    # Check the is file operation
    self.assert_(isFileRes['OK'])
    self.assert_(isFileRes['Value']['Successful'].has_key(destFile))
    self.assert_(isFileRes['Value']['Successful'][destFile])
    # check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    # Check that the directory is not a file
    self.assert_(failedIsFileRes['OK'])
    self.assert_(failedIsFileRes['Value']['Successful'].has_key(destDir))
    self.assertFalse(failedIsFileRes['Value']['Successful'][destDir])

  def test_putGetFileMetaData(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file metadata test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    putFileRes = self.storage.putFile(fileTuple)
    # Check that we can get the file metadata
    getMetadataRes = self.storage.getFileMetadata(destFile)
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(destFile)
    # See what happens with non existant files
    failedMetadataRes = self.storage.getFileMetadata(destFile)
    # Check directories are handled properly
    destDir = os.path.dirname(destFile)
    directoryMetadataRes = self.storage.getFileMetadata(destDir)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(destFile))
    # Check the get metadata operation
    self.assert_(getMetadataRes['OK'])
    self.assert_(getMetadataRes['Value']['Successful'].has_key(destFile))
    fileMetaData = getMetadataRes['Value']['Successful'][destFile]
    self.assert_(fileMetaData['Cached'])
    self.assertFalse(fileMetaData['Migrated'])
    self.assertEqual(fileMetaData['Size'],fileSize)
    # check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    # Check the get metadata for non existant file
    self.assert_(failedMetadataRes['OK'])
    self.assert_(failedMetadataRes['Value']['Failed'].has_key(destFile))
    expectedError = "SRM2Storage.getFileMetadata: File does not exist."
    self.assertEqual(failedMetadataRes['Value']['Failed'][destFile],expectedError)
    # Check that metadata operation with a directory
    self.assert_(directoryMetadataRes['OK'])
    self.assert_(directoryMetadataRes['Value']['Failed'].has_key(destDir))
    expectedError = "SRM2Storage.getFileMetadata: Supplied path is not a file."
    self.assertEqual(directoryMetadataRes['Value']['Failed'][destDir],expectedError)

  def test_putGetFileSize(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file size test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    putFileRes = self.storage.putFile(fileTuple)
    # Check that we can get the file size
    getSizeRes = self.storage.getFileSize(destFile)
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(destFile)
    # Check non existant files
    failedSizeRes = self.storage.getFileMetadata(destFile)
    # Check directories are handled properly
    destDir = os.path.dirname(destFile)
    directorySizeRes = self.storage.getFileSize(destDir)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(destFile))
    # Check that we got the file size correctly
    self.assert_(getSizeRes['OK'])
    self.assert_(getSizeRes['Value']['Successful'].has_key(destFile))
    self.assertEqual(getSizeRes['Value']['Successful'][destFile],fileSize)
    # check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    # Check the get size with non existant file works properly
    self.assert_(failedSizeRes['OK'])
    self.assert_(failedSizeRes['Value']['Failed'].has_key(destFile))
    expectedError = "SRM2Storage.getFileMetadata: File does not exist."
    self.assertEqual(failedSizeRes['Value']['Failed'][destFile],expectedError)
    # Check that the passing a directory is handled correctly
    self.assert_(directorySizeRes['OK'])
    self.assert_(directorySizeRes['Value']['Failed'].has_key(destDir))
    expectedError = "SRM2Storage.getFileSize: Supplied path is not a file."
    self.assertEqual(directorySizeRes['Value']['Failed'][destDir],expectedError)
  def test_putPrestageFile(self):
    print '\n\n#########################################################################\n\n\t\t\tFile prestage test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    putFileRes = self.storage.putFile(fileTuple)
    # Check that we can issue a stage request
    prestageRes = self.storage.prestageFile(destFile)
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(destFile)

    # Check the put file operation
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(destFile))
    # Check the prestage file operation
    self.assert_(prestageRes['OK'])
    self.assert_(prestageRes['Value']['Successful'].has_key(destFile))
    self.assert_(prestageRes['Value']['Successful'][destFile])
    # Check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))

    # These checks are currently disabled until a bug is fixed
    # Check what happens with deleted files
    #deletedPrestageRes = self.storage.prestageFile(destFile)
    #self.assert_(deletedPrestageRes['OK'])
    #self.assert_(deletedPrestageRes['Value']['Failed'].has_key(destFile))

    # Check what happens with non-existant files #THIS IS A BUG, REPORT IR
    #testFile = "%s-THIS-IS-DEFINATELY-NOT-A-FILE" % destFile
    #nonExistantPrestageRes= self.storage.prestageFile(testFile)
    #self.assert_(nonExistantPrestageRes['OK'])
    #self.assert_(nonExistantPrestageRes['Value']['Failed'].has_key(destFile))

  def test_putFilegetTransportURL(self):
    print '\n\n#########################################################################\n\n\t\t\tGet tURL test\n'
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    putFileRes = self.storage.putFile(fileTuple)
    #Check that we can get a turl
    getTurlRes = self.storage.getTransportURL(destFile,['dcap','gsidcap'])
    # Clean up the remote mess
    removeFileRes = self.storage.removeFile(destFile)
    # Try and get a turl for a non existant file
    failedGetTurlRes = self.storage.getTransportURL(destFile,['dcap','gsidcap'])

    # Check the put file operation
    print putFileRes
    self.assert_(putFileRes['OK'])
    self.assert_(putFileRes['Value']['Successful'].has_key(destFile))
    # check the get turl operation
    print getTurlRes, destFile
    self.assert_(getTurlRes['OK'])
    self.assert_(getTurlRes['Value']['Successful'].has_key(destFile))
    # check the remove file operation
    self.assert_(removeFileRes['OK'])
    self.assert_(removeFileRes['Value']['Successful'].has_key(destFile))
    #Check the get turl with non existant file operation
    self.assert_(failedGetTurlRes['OK'])
    self.assert_(failedGetTurlRes['Value']['Failed'].has_key(destFile))
    expectedError = "SRM2Storage.getTransportURL: File does not exist."
    self.assertEqual(failedGetTurlRes['Value']['Failed'][destFile],expectedError)

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(FileTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

