""" test for RFIO plugin
"""

# FIXME: doesn't work ATM


import unittest
import time
import os
import shutil
from unittest import mock

from DIRAC import S_OK

from DIRAC.Resources.Storage.test.Test_FilePlugin import (
    mock_StorageFactory_getConfigStorageOptions,
    mock_StorageFactory_getConfigStorageProtocols,
    mock_StorageFactory_getConfigStorageName,
)

# from DIRAC.Resources.Storage.StorageFactory     import StorageFactory
from DIRAC.Resources.Storage.StorageElement import StorageElementItem
from DIRAC.Core.Utilities.File import getSize


def mock_StorageFactory_getCurrentURL(storageName, derivedStorageName):
    """Get the options associated to the StorageElement as defined in the CS"""
    optionsDict = {"BackendType": "local", "ReadAccess": "Active", "WriteAccess": "Active"}

    return S_OK(optionsDict)


# class StoragePlugInTestCase( unittest.TestCase ):
#   """ Base class for the StoragePlugin test cases
#   """
#   def setUp( self ):
#     factory = StorageFactory()
#     res = factory.getStorages( 'CERN-RAW', ['RFIO'] )
#     self.assertTrue(res['OK'])
#     storageDetails = res['Value']


class StoragePlugInTestCase(unittest.TestCase):
    """Base test class. Defines all the method to test"""

    @mock.patch(
        "DIRAC.Resources.Storage.StorageFactory.StorageFactory._getConfigStorageName",
        side_effect=mock_StorageFactory_getConfigStorageName,
    )
    @mock.patch(
        "DIRAC.Resources.Storage.StorageFactory.StorageFactory._getConfigStorageOptions",
        side_effect=mock_StorageFactory_getConfigStorageOptions,
    )
    @mock.patch(
        "DIRAC.Resources.Storage.StorageFactory.StorageFactory._getConfigStorageProtocols",
        side_effect=mock_StorageFactory_getConfigStorageProtocols,
    )
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    # @mock.patch( 'DIRAC.Resources.Storage.StorageFactory.StorageFactory._getCurrentURL',
    #              side_effect = mock_StorageFactory_getCurrentURL_getCurrentURL )
    def setUp(
        self,
        mk_getConfigStorageName,
        mk_getConfigStorageOptions,
        mk_getConfigStorageProtocols,
        mk_isLocalSE,
        mk_addAccountingOperation,
    ):
        self.storage = StorageElementItem("FAKE")
        self.storage.vo = "test"

        # self.storage = storageDetails['StorageObjects'][0]
        # self.storage.changeDirectory( 'lhcb/test/unit-test/Storage/RFIOStorage' )

    def test_createUnitTestDir(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tCreate Directory test\n"
        )
        # destDir = self.storage.getCurrentURL( '' )['Value']
        destDir = "/bla/"
        res = self.storage.createDirectory(destDir)
        print(res)
        self.assertTrue(res["OK"])
        self.assertTrue(destDir in res["Value"]["Successful"])
        self.assertTrue(res["Value"]["Successful"][destDir])


class DirectoryTestCase(StoragePlugInTestCase):
    def test_isDirectory(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tIs Directory test\n"
        )
        # Test that we can determine what is a directory
        destDir = self.storage.getCurrentURL("")["Value"]
        isDirRes = self.storage.isDirectory(destDir)
        # Test that we can determine that a directory is not a directory
        destDir = self.storage.getCurrentURL("NonExistantFile")["Value"]
        nonExistantDirRes = self.storage.isDirectory(destDir)

        # Check the is directory operation
        self.assertTrue(isDirRes["OK"])
        self.assertTrue(destDir in isDirRes["Value"]["Successful"])
        self.assertTrue(isDirRes["Value"]["Successful"][destDir])
        # Check the non existant directory operation
        self.assertTrue(nonExistantDirRes["OK"])
        self.assertTrue(destDir in nonExistantDirRes["Value"]["Successful"])
        self.assertFalse(nonExistantDirRes["Value"]["Successful"][destDir])

    def test_putRemoveDirectory(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tPut Directory test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("putDirTest")["Value"]
        ignore = self.storage.removeDirectory(remoteDir)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        numberOfFiles = 5
        sizeOfLocalFile = getSize(srcFile)
        print("Creating local directory: %s" % localDir)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)
        # Check that we can successfully upload the directory to the storage element
        dirTuple = (localDir, remoteDir)
        putDirRes = self.storage.putDirectory(dirTuple)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir)
        # Clean up the locally created directory
        print("Removing local directory: %s" % localDir)
        localFiles = os.listdir(localDir)
        for fileName in localFiles:
            fullPath = f"{localDir}/{fileName}"
            os.remove(fullPath)
        os.removedirs(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        resDict = putDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)
        # Perform the checks for the remove dir operation
        self.assertTrue(removeDirRes["OK"])
        self.assertTrue(remoteDir in removeDirRes["Value"]["Successful"])
        resDict = removeDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)

    def test_putGetDirectoryMetadata(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet Directory Metadata test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("putDirTest")["Value"]
        ignore = self.storage.removeDirectory(remoteDir)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        numberOfFiles = 5
        sizeOfLocalFile = getSize(srcFile)
        print("Creating local directory: %s" % localDir)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)
        # Check that we can successfully upload the directory to the storage element
        dirTuple = (localDir, remoteDir)
        putDirRes = self.storage.putDirectory(dirTuple)
        # Get the directory metadata
        getMetadataRes = self.storage.getDirectoryMetadata(remoteDir)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir)
        # Clean up the locally created directory
        print("Removing local directory: %s" % localDir)
        localFiles = os.listdir(localDir)
        for fileName in localFiles:
            fullPath = f"{localDir}/{fileName}"
            os.remove(fullPath)
        os.removedirs(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        resDict = putDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)
        # Perform the checks for the get metadata operation
        self.assertTrue(getMetadataRes["OK"])
        self.assertTrue(remoteDir in getMetadataRes["Value"]["Successful"])
        resDict = getMetadataRes["Value"]["Successful"][remoteDir]
        self.assertTrue("Mode" in resDict)
        self.assertEqual(resDict["Mode"], 493)
        # Perform the checks for the remove directory operation
        self.assertTrue(removeDirRes["OK"])
        self.assertTrue(remoteDir in removeDirRes["Value"]["Successful"])
        resDict = removeDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)

    def test_putGetDirectorySize(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet Directory Size test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("putDirTest")["Value"]
        ignore = self.storage.removeDirectory(remoteDir)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        numberOfFiles = 5
        sizeOfLocalFile = getSize(srcFile)
        print("Creating local directory: %s" % localDir)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)
        # Check that we can successfully upload the directory to the storage element
        dirTuple = (localDir, remoteDir)
        putDirRes = self.storage.putDirectory(dirTuple)
        # Now get the directory size
        getDirSizeRes = self.storage.getDirectorySize(remoteDir)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir)
        # Clean up the locally created directory
        print("Removing local directory: %s" % localDir)
        localFiles = os.listdir(localDir)
        for fileName in localFiles:
            fullPath = f"{localDir}/{fileName}"
            os.remove(fullPath)
        os.removedirs(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        resDict = putDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)
        # Now perform the checks for the get directory size operation
        self.assertTrue(getDirSizeRes["OK"])
        self.assertTrue(remoteDir in getDirSizeRes["Value"]["Successful"])
        resDict = getDirSizeRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)
        self.assertEqual(resDict["Files"], numberOfFiles)
        # Perform the checks for the remove directory operation
        self.assertTrue(removeDirRes["OK"])
        self.assertTrue(remoteDir in removeDirRes["Value"]["Successful"])
        resDict = removeDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)

    def test_putListDirectory(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tList Directory test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("putDirTest")["Value"]
        ignore = self.storage.removeDirectory(remoteDir)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        numberOfFiles = 5
        sizeOfLocalFile = getSize(srcFile)
        print("Creating local directory: %s" % localDir)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)
        # Check that we can successfully upload the directory to the storage element
        dirTuple = (localDir, remoteDir)
        putDirRes = self.storage.putDirectory(dirTuple)
        # List the remote directory
        listDirRes = self.storage.listDirectory(remoteDir)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir)
        # Clean up the locally created directory
        print("Removing local directory: %s" % localDir)
        localFiles = os.listdir(localDir)
        for fileName in localFiles:
            fullPath = f"{localDir}/{fileName}"
            os.remove(fullPath)
        os.removedirs(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        resDict = putDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)
        # Perform the checks for the list dir operation
        self.assertTrue(listDirRes["OK"])
        self.assertTrue(remoteDir in listDirRes["Value"]["Successful"])
        resDict = listDirRes["Value"]["Successful"][remoteDir]
        self.assertTrue("SubDirs" in resDict)
        self.assertTrue("Files" in resDict)
        self.assertEqual(len(resDict["Files"]), numberOfFiles)
        # Perform the checks for the remove directory operation
        self.assertTrue(removeDirRes["OK"])
        self.assertTrue(remoteDir in removeDirRes["Value"]["Successful"])
        resDict = removeDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)

    def test_putGetDirectory(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet Directory test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("putDirTest")["Value"]
        ignore = self.storage.removeDirectory(remoteDir)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        numberOfFiles = 5
        sizeOfLocalFile = getSize(srcFile)
        print("Creating local directory: %s" % localDir)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)
        # Check that we can successfully upload the directory to the storage element
        dirTuple = (localDir, remoteDir)
        putDirRes = self.storage.putDirectory(dirTuple)
        # Clean up the locally created directory
        print("Removing local directory: %s" % localDir)
        localFiles = os.listdir(localDir)
        for fileName in localFiles:
            fullPath = f"{localDir}/{fileName}"
            os.remove(fullPath)
        os.removedirs(localDir)
        # Check that we can get directories from the storage element
        directoryTuple = (remoteDir, localDir)
        getDirRes = self.storage.getDirectory(directoryTuple)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir)
        # Clean up the locally created directory
        print("Removing local directory: %s" % localDir)
        localFiles = os.listdir(localDir)
        for fileName in localFiles:
            fullPath = f"{localDir}/{fileName}"
            os.remove(fullPath)
        os.removedirs(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        resDict = putDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)
        # Perform the checks for the get dir operation
        self.assertTrue(getDirRes["OK"])
        self.assertTrue(remoteDir in getDirRes["Value"]["Successful"])
        resDict = getDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)
        # Perform the checks for the remove directory operation
        self.assertTrue(removeDirRes["OK"])
        self.assertTrue(remoteDir in removeDirRes["Value"]["Successful"])
        resDict = removeDirRes["Value"]["Successful"][remoteDir]
        self.assertEqual(resDict["Files"], numberOfFiles)
        self.assertEqual(resDict["Size"], numberOfFiles * sizeOfLocalFile)


class FileTestCase(StoragePlugInTestCase):
    def test_putRemoveFile(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tPut and Remove test\n"
        )
        # First test that we are able to determine whether the file sizes of the transfer don't match
        srcFile = "/etc/group"
        fileSize = 10  # This is a made up value
        testFileName = "testFile.%s" % time.time()
        destFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileTuple = (srcFile, destFile, fileSize)
        failedPutFileRes = self.storage.putFile(fileTuple)
        # Now make sure that we can actually upload a file properly
        fileSize = getSize(srcFile)
        fileTuple = (srcFile, destFile, fileSize)
        putFileRes = self.storage.putFile(fileTuple)
        # Make sure we are able to remove the file
        removeFileRes = self.storage.removeFile(destFile)

        # Check the failed put file operation
        self.assertTrue(failedPutFileRes["OK"])
        self.assertTrue(destFile in failedPutFileRes["Value"]["Failed"])
        expectedError = "RFIOStorage.putFile: Source and destination file sizes do not match."
        self.assertEqual(failedPutFileRes["Value"]["Failed"][destFile], expectedError)
        # Check the successful put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(destFile in putFileRes["Value"]["Successful"])
        # Check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(destFile in removeFileRes["Value"]["Successful"])

    """
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
    self.assertTrue(putRes['OK'])
    self.assertTrue(putRes['Value']['Successful'].has_key(destFile))
    # Check the failed get operation
    self.assertTrue(failedGetRes['OK'])
    self.assertTrue(failedGetRes['Value']['Failed'].has_key(destFile))
    expectedError = 'RFIOStorage.getFile: Source and destination file sizes do not match.'
    self.assertEqual(failedGetRes['Value']['Failed'][destFile],expectedError)
    # Check the get operation
    self.assertTrue(getFileRes['OK'])
    self.assertTrue(getFileRes['Value']['Successful'].has_key(destFile))
    # Check the remove operation
    self.assertTrue(removeFileRes['OK'])
    self.assertTrue(removeFileRes['Value']['Successful'].has_key(destFile))

  """

    def test_putExistsFile(self):
        print("\n\n#########################################################" "################\n\n\t\t\tExists test\n")
        # First upload a file to the storage
        srcFile = "/etc/group"
        fileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        destFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileTuple = (srcFile, destFile, fileSize)
        putFileRes = self.storage.putFile(fileTuple)
        # Then get the file's existance
        existsFileRes = self.storage.exists(destFile)
        # Now remove the file
        removeFileRes = self.storage.removeFile(destFile)
        # Check  again that the file exists
        failedExistRes = self.storage.exists(destFile)

        # Check the put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(destFile in putFileRes["Value"]["Successful"])
        # Check the exists operation
        self.assertTrue(existsFileRes["OK"])
        self.assertTrue(destFile in existsFileRes["Value"]["Successful"])
        self.assertTrue(existsFileRes["Value"]["Successful"][destFile])
        # Check the removal operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(destFile in removeFileRes["Value"]["Successful"])
        # Check the failed exists operation
        self.assertTrue(failedExistRes["OK"])
        self.assertTrue(destFile in failedExistRes["Value"]["Successful"])
        self.assertFalse(failedExistRes["Value"]["Successful"][destFile])

    def test_putIsFile(self):
        print(
            "\n\n#########################################################" "################\n\n\t\t\tIs file test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        fileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        destFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileTuple = (srcFile, destFile, fileSize)
        putFileRes = self.storage.putFile(fileTuple)
        # Check we are able to determine that it is a file
        isFileRes = self.storage.isFile(destFile)
        # Clean up the remote mess
        removeFileRes = self.storage.removeFile(destFile)
        # Check that everything isn't a file
        destDir = os.path.dirname(destFile)
        failedIsFileRes = self.storage.isFile(destDir)

        # Check the put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(destFile in putFileRes["Value"]["Successful"])
        # Check the is file operation
        self.assertTrue(isFileRes["OK"])
        self.assertTrue(destFile in isFileRes["Value"]["Successful"])
        self.assertTrue(isFileRes["Value"]["Successful"][destFile])
        # check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(destFile in removeFileRes["Value"]["Successful"])
        # Check that the directory is not a file
        self.assertTrue(failedIsFileRes["OK"])
        self.assertTrue(destDir in failedIsFileRes["Value"]["Successful"])
        self.assertFalse(failedIsFileRes["Value"]["Successful"][destDir])

    def test_putGetFileMetaData(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet file metadata test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        fileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        destFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileTuple = (srcFile, destFile, fileSize)
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
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(destFile in putFileRes["Value"]["Successful"])
        # Check the get metadata operation
        self.assertTrue(getMetadataRes["OK"])
        self.assertTrue(destFile in getMetadataRes["Value"]["Successful"])
        fileMetaData = getMetadataRes["Value"]["Successful"][destFile]
        # self.assertTrue(fileMetaData['Cached'])
        # self.assertFalse(fileMetaData['Migrated'])
        self.assertEqual(fileMetaData["Size"], fileSize)
        # check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(destFile in removeFileRes["Value"]["Successful"])
        # Check the get metadata for non existant file
        self.assertTrue(failedMetadataRes["OK"])
        self.assertTrue(destFile in failedMetadataRes["Value"]["Failed"])
        expectedError = "No such file or directory"
        self.assertEqual(failedMetadataRes["Value"]["Failed"][destFile], expectedError)
        # Check that metadata operation with a directory
        self.assertTrue(directoryMetadataRes["OK"])
        self.assertTrue(destDir in directoryMetadataRes["Value"]["Failed"])
        expectedError = "RFIOStorage.getFileMetadata: Supplied path is not a file."
        self.assertEqual(directoryMetadataRes["Value"]["Failed"][destDir], expectedError)

    def test_putGetFileSize(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet file size test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        fileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        destFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileTuple = (srcFile, destFile, fileSize)
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
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(destFile in putFileRes["Value"]["Successful"])
        # Check that we got the file size correctly
        self.assertTrue(getSizeRes["OK"])
        self.assertTrue(destFile in getSizeRes["Value"]["Successful"])
        self.assertEqual(getSizeRes["Value"]["Successful"][destFile], fileSize)
        # check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(destFile in removeFileRes["Value"]["Successful"])
        # Check the get size with non existant file works properly
        self.assertTrue(failedSizeRes["OK"])
        self.assertTrue(destFile in failedSizeRes["Value"]["Failed"])
        expectedError = "No such file or directory"
        self.assertEqual(failedSizeRes["Value"]["Failed"][destFile], expectedError)
        # Check that the passing a directory is handled correctly
        self.assertTrue(directorySizeRes["OK"])
        self.assertTrue(destDir in directorySizeRes["Value"]["Failed"])
        expectedError = "RFIOStorage.getFileSize: Supplied path is not a file."
        self.assertEqual(directorySizeRes["Value"]["Failed"][destDir], expectedError)

    def test_putPrestageFile(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tFile prestage test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        fileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        destFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileTuple = (srcFile, destFile, fileSize)
        putFileRes = self.storage.putFile(fileTuple)
        # Check that we can issue a stage request
        prestageRes = self.storage.prestageFile(destFile)
        # Clean up the remote mess
        removeFileRes = self.storage.removeFile(destFile)

        # Check the put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(destFile in putFileRes["Value"]["Successful"])
        # Check the prestage file operation
        self.assertTrue(prestageRes["OK"])
        self.assertTrue(destFile in prestageRes["Value"]["Successful"])
        self.assertTrue(prestageRes["Value"]["Successful"][destFile])
        # Check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(destFile in removeFileRes["Value"]["Successful"])

        # These checks are currently disabled until a bug is fixed
        # Check what happens with deleted files
        # deletedPrestageRes = self.storage.prestageFile(destFile)
        # self.assertTrue(deletedPrestageRes['OK'])
        # self.assertTrue(deletedPrestageRes['Value']['Failed'].has_key(destFile))

        # Check what happens with non-existant files #THIS IS A BUG, REPORT IR
        # testFile = "%s-THIS-IS-DEFINATELY-NOT-A-FILE" % destFile
        # nonExistantPrestageRes= self.storage.prestageFile(testFile)
        # self.assertTrue(nonExistantPrestageRes['OK'])
        # self.assertTrue(nonExistantPrestageRes['Value']['Failed'].has_key(destFile))

    def test_putFilegetTransportURL(self):
        print(
            "\n\n#########################################################" "################\n\n\t\t\tGet tURL test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        fileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        destFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileTuple = (srcFile, destFile, fileSize)
        putFileRes = self.storage.putFile(fileTuple)
        # Check that we can get a turl
        getTurlRes = self.storage.getTransportURL(destFile, ["dcap", "gsidcap"])
        # Clean up the remote mess
        removeFileRes = self.storage.removeFile(destFile)
        # Try and get a turl for a non existant file
        failedGetTurlRes = self.storage.getTransportURL(destFile, ["dcap", "gsidcap"])

        # Check the put file operation
        print(putFileRes)
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(destFile in putFileRes["Value"]["Successful"])
        # check the get turl operation
        print(getTurlRes, destFile)
        self.assertTrue(getTurlRes["OK"])
        self.assertTrue(destFile in getTurlRes["Value"]["Successful"])
        # check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(destFile in removeFileRes["Value"]["Successful"])
        # Check the get turl with non existant file operation
        self.assertTrue(failedGetTurlRes["OK"])
        self.assertTrue(destFile in failedGetTurlRes["Value"]["Failed"])
        expectedError = "RFIOStorage.getTransportURL: File does not exist."
        self.assertEqual(failedGetTurlRes["Value"]["Failed"][destFile], expectedError)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(StoragePlugInTestCase)
    # suite = unittest.defaultTestLoader.loadTestsFromTestCase( FileTestCase )
    # suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
