import unittest,types,time,os,shutil
from DIRAC.Core.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.File import getSize

class StoragePlugInTestCase(unittest.TestCase):
  """ Base class for the StoragePlugin test cases
  """
  def setUp(self):
    factory = StorageFactory()
    res = factory.getStorages('IN2P3-RAW', ['SRM2'])
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
    res = self.storage.isDirectory(destDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destDir))
    self.assert_(res['Value']['Successful'][destDir])

    # Test that we can determine that a directory is not a directory
    destDir = self.storage.getCurrentURL('NonExistantFile')['Value']
    res = self.storage.isDirectory(destDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destDir))
    self.assertFalse(res['Value']['Successful'][destDir])

  def test_putRemoveDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tPut Directory test\n'    
    # First create a local directory to upload
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
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']
    dirTuple = (localDir,remoteDir)
    res = self.storage.putDirectory(dirTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)    
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)

    # Now remove the remove directory
    res = self.storage.removeDirectory(remoteDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)

    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

  def test_putGetDirectoryMetadata(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Directory Metadata test\n'
    # First create a local directory to upload
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
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']
    dirTuple = (localDir,remoteDir)
    res = self.storage.putDirectory(dirTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)

    # Get the directory metadata
    res = self.storage.getDirectoryMetadata(remoteDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assert_(resDict.has_key('Permissions'))
    self.assertEqual(resDict['Permissions'],493)

    # Now remove the remove directory
    res = self.storage.removeDirectory(remoteDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

  def test_putGetDirectorySize(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Directory Size test\n'
    # First create a local directory to upload
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
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']
    dirTuple = (localDir,remoteDir)
    res = self.storage.putDirectory(dirTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)

    # Now get the directory size
    res = self.storage.getDirectorySize(remoteDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    self.assertEqual(resDict['Files'],numberOfFiles)

    # Now remove the remove directory
    res = self.storage.removeDirectory(remoteDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
   
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

  def test_putListDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tList Directory test\n'
    # First create a local directory to upload
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
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']
    dirTuple = (localDir,remoteDir)
    res = self.storage.putDirectory(dirTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
       
    # List the remote directory
    res = self.storage.listDirectory(remoteDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assert_(resDict.has_key('SubDirs'))    
    self.assert_(resDict.has_key('Files'))
    self.assertEqual(len(resDict['Files'].keys()), numberOfFiles)

    # Now remove the remove directory
    res = self.storage.removeDirectory(remoteDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)
    
    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)
  
  def test_putGetDirectory(self):
    print '\n\n#########################################################################\n\n\t\t\tGet Directory test\n'
    # First create a local directory to upload
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
    remoteDir = self.storage.getCurrentURL('putDirTest')['Value']  
    dirTuple = (localDir,remoteDir)
    res = self.storage.putDirectory(dirTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)

    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

    # Check that we can get directories from the storage element
    directoryTuple = (remoteDir,localDir)
    res = self.storage.getDirectory(directoryTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir] 
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile) 

    # Now remove the remove directory
    res = self.storage.removeDirectory(remoteDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(remoteDir))
    resDict = res['Value']['Successful'][remoteDir]
    self.assertEqual(resDict['Files'],numberOfFiles)
    self.assertEqual(resDict['Size'],numberOfFiles*sizeOfLocalFile)

    #Clean up the locally created directory
    print 'Removing local directory: %s' % localDir
    localFiles = os.listdir(localDir)
    for fileName in localFiles:
      fullPath = '%s/%s' % (localDir,fileName)
      os.remove(fullPath)
    os.removedirs(localDir)

class FileTestCase(StoragePlugInTestCase):

  def test_putRemoveFile(self):
    print '\n\n#########################################################################\n\n\t\t\tPut and Remove test\n'
    # First test that we are able to determine whether the file sizes of the transfer don't match
    srcFile = '/etc/group'
    fileSize = 10 #This is a made up value
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    res = self.storage.putFile(fileTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Failed'].has_key(destFile))
    expectedError =  'SRM2Storage.putFile: Source and destination file sizes do not match.'    
    self.assertEqual(res['Value']['Failed'][destFile],expectedError)

    # Now make sure that we can actually upload a file properly
    fileSize = getSize(srcFile)
    fileTuple = (srcFile,destFile,fileSize)
    res = self.storage.putFile(fileTuple)  
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))       
 
    # Make sure we are able to remove the file 
    res = self.storage.removeFile(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

  def test_putGetFile(self):
    print '\n\n#########################################################################\n\n\t\t\tPut and Get test\n'  
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    res = self.storage.putFile(fileTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))  
  
    # Make sure we can detect when the file transferred incorrectly
    destLocalFile = "%s/%s.%s" % (os.getcwd(),'unitTestFile',time.time())
    wrongSize = 10
    fileTuple = (destFile,destLocalFile,wrongSize)
    res = self.storage.getFile(fileTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Failed'].has_key(destFile))
    expectedError = 'SRM2Storage.getFile: Source and destination file sizes do not match.'
    self.assertEqual(res['Value']['Failed'][destFile],expectedError)
 
    # Then make sure we can get a local copy of the file
    destLocalFile = "%s/%s.%s" % (os.getcwd(),'unitTestFile',time.time())
    fileTuple = (destFile,destLocalFile,fileSize)
    res = self.storage.getFile(fileTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))
 
    #Cleanup the mess locally
    os.remove(destLocalFile)
    # Cleanup the remote mess
    res = self.storage.removeFile(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile)) 

  def test_putExistsFile(self):
    print '\n\n#########################################################################\n\n\t\t\tExists test\n'  
    # First upload a file to the storage  
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    res = self.storage.putFile(fileTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    # Then check that the file exists
    res = self.storage.exists(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))
    self.assert_(res['Value']['Successful'][destFile]) 

    # Now remove the file
    res = self.storage.removeFile(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    # Check  again that the file exists
    res = self.storage.exists(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))
    self.assertFalse(res['Value']['Successful'][destFile])  

  def test_putIsFile(self):
    print '\n\n#########################################################################\n\n\t\t\tIs file test\n'  
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    res = self.storage.putFile(fileTuple)  
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    # Check we are able to determine that it is a file  
    res = self.storage.isFile(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))
    self.assert_(res['Value']['Successful'][destFile])

    # Check that everything isn't a file
    destDir = os.path.dirname(destFile)
    res = self.storage.isFile(destDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destDir))
    self.assertFalse(res['Value']['Successful'][destDir])

    # Clean up the remote mess   
    res = self.storage.removeFile(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile)) 

  def test_putGetFileMetaData(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file metadata test\n'  
    # First upload a file to the storage   
    srcFile = '/etc/group' 
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    res = self.storage.putFile(fileTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    # Check that we can get the file metadata
    res = self.storage.getFileMetadata(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))
    fileMetaData = res['Value']['Successful'][destFile]
    self.assert_(fileMetaData['Cached'])
    self.assertFalse(fileMetaData['Migrated'])
    self.assertEqual(fileMetaData['Size'],fileSize)

    # Clean up the remote mess
    res = self.storage.removeFile(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    # Check non existant files
    res = self.storage.getFileMetadata(destFile) 
    self.assert_(res['OK'])
    self.assert_(res['Value']['Failed'].has_key(destFile))
    expectedError = "SRM2Storage.getFileMetadata: File does not exist."
    self.assertEqual(res['Value']['Failed'][destFile],expectedError)

    # Check directories are handled properly
    destDir = os.path.dirname(destFile)
    res = self.storage.getFileMetadata(destDir)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Failed'].has_key(destDir))
    expectedError = "SRM2Storage.getFileMetadata: Supplied path is not a file."
    self.assertEqual(res['Value']['Failed'][destDir],expectedError)

  def test_putGetFileSize(self):
    print '\n\n#########################################################################\n\n\t\t\tGet file size test\n'  
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    res = self.storage.putFile(fileTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))
   
    # Check that we can get the file size
    res = self.storage.getFileSize(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))
    self.assertEqual(res['Value']['Successful'][destFile],fileSize)
   
    # Clean up the remote mess
    res = self.storage.removeFile(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    # Check non existant files
    res = self.storage.getFileMetadata(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Failed'].has_key(destFile))
    expectedError = "SRM2Storage.getFileMetadata: File does not exist."
    self.assertEqual(res['Value']['Failed'][destFile],expectedError)
   
    # Check directories are handled properly
    destDir = os.path.dirname(destFile)
    res = self.storage.getFileSize(destDir)
    self.assert_(res['OK'])   
    self.assert_(res['Value']['Failed'].has_key(destDir))
    expectedError = "SRM2Storage.getFileSize: Supplied path is not a file."
    self.assertEqual(res['Value']['Failed'][destDir],expectedError)

  def test_putPrestageFile(self):
    print '\n\n#########################################################################\n\n\t\t\tFile prestage test\n'  
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    res = self.storage.putFile(fileTuple)  
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    # Check that we can issue a stage request
    res = self.storage.prestageFile(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))
    self.assert_(res['Value']['Successful'][destFile])

    # Clean up the remote mess 
    res = self.storage.removeFile(destFile)   
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))
        
    # Check what happens with deleted files
    #res = self.storage.prestageFile(destFile)
    #self.assert_(res['OK'])
    #self.assert_(res['Value']['Failed'].has_key(destFile))
    
    # Check what happens with non-existant files #THIS IS A BUG, REPORT IR
    testFile = "%s-THIS-IS-DEFINATELY-NOT-A-FILE" % destFile
    res = self.storage.prestageFile(testFile)    
    self.assert_(res['OK'])
    #self.assert_(res['Value']['Failed'].has_key(destFile))

  def test_putFilegetTransportURL(self):
    print '\n\n#########################################################################\n\n\t\t\tGet tURL test\n'  
    # First upload a file to the storage
    srcFile = '/etc/group'
    fileSize = getSize(srcFile)
    testFileName = 'testFile.%s' % time.time()
    destFile = self.storage.getCurrentURL(testFileName)['Value']
    fileTuple = (srcFile,destFile,fileSize)
    res = self.storage.putFile(fileTuple)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    #Check that we can get a turl
    res = self.storage.getTransportURL(destFile,['dcap','gsidcap'])
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    # Clean up the remote mess
    res = self.storage.removeFile(destFile)
    self.assert_(res['OK'])
    self.assert_(res['Value']['Successful'].has_key(destFile))

    #Check that we can get a turl
    res = self.storage.getTransportURL(destFile,['gsidcap'])
    self.assert_(res['OK'])
    self.assert_(res['Value']['Failed'].has_key(destFile))
    expectedError = "SRM2Storage.getTransportURL: File does not exist."
    self.assertEqual(res['Value']['Failed'][destFile],expectedError) 

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(FileTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

