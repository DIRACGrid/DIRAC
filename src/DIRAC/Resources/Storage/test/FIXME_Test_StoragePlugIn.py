#! /usr/bin/env python

# FIXME: if it requires a dirac.cfg it is not a unit test and should be moved to tests directory


import unittest
import time
import os
import shutil
import sys

from DIRAC.Core.Base.Script import parseCommandLine, getPositionalArgs

parseCommandLine()

from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.File import getSize

positionalArgs = getPositionalArgs()
if len(positionalArgs) < 2:
    print("Usage: TestStoragePlugIn.py StorageElement plugin")
    sys.exit()
else:
    storageElementToTest = positionalArgs[0]
    plugin = positionalArgs[1]


class StoragePlugInTestCase(unittest.TestCase):
    """Base class for the StoragePlugin test cases"""

    def setUp(self):

        factory = StorageFactory("lhcb")
        res = factory.getStorages(storageElementToTest, [plugin])
        self.assertTrue(res["OK"])
        storageDetails = res["Value"]
        self.storage = storageDetails["StorageObjects"][0]
        self.storage.changeDirectory("lhcb/test/unit-test/TestStoragePlugIn")
        destDir = self.storage.getCurrentURL("")["Value"]
        res = self.storage.createDirectory(destDir)
        self.assertTrue(res["OK"])
        self.assertTrue(destDir in res["Value"]["Successful"])
        self.assertTrue(res["Value"]["Successful"][destDir])
        self.numberOfFiles = 1

    def tearDown(self):
        remoteDir = self.storage.getCurrentURL("")["Value"]
        _ = self.storage.removeDirectory(remoteDir, True)


class DirectoryTestCase(StoragePlugInTestCase):
    def test_putRemoveDirectory(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tPut Directory test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("")["Value"]
        _ = self.storage.removeDirectory(remoteDir, True)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        sizeOfLocalFile = getSize(srcFile)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(self.numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)

        # Check that we can successfully upload the directory to the storage element
        dirDict = {remoteDir: localDir}
        putDirRes = self.storage.putDirectory(dirDict)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir, True)
        # Clean up the locally created directory
        shutil.rmtree(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        if putDirRes["Value"]["Successful"][remoteDir]["Files"]:
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Files"], self.numberOfFiles)
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Size"], self.numberOfFiles * sizeOfLocalFile)
        self.assertTrue(isinstance(putDirRes["Value"]["Successful"][remoteDir]["Files"], int))
        self.assertTrue(type(putDirRes["Value"]["Successful"][remoteDir]["Size"]) in (int,))
        # Perform the checks for the remove dir operation
        self.assertTrue(removeDirRes["OK"])
        self.assertTrue(remoteDir in removeDirRes["Value"]["Successful"])
        if removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"]:
            self.assertEqual(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], self.numberOfFiles)
            self.assertEqual(
                removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"], self.numberOfFiles * sizeOfLocalFile
            )
        self.assertTrue(isinstance(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], int))
        self.assertTrue(type(removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"]) in (int,))

    def test_isDirectory(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tIs Directory test\n"
        )
        # Test that we can determine what is a directory
        destDir = self.storage.getCurrentURL("")["Value"]
        isDirRes = self.storage.isDirectory(destDir)
        # Test that we can determine that a directory is not a directory
        dummyDir = self.storage.getCurrentURL("NonExistantFile")["Value"]
        nonExistantDirRes = self.storage.isDirectory(dummyDir)

        # Check the is directory operation
        self.assertTrue(isDirRes["OK"])
        self.assertTrue(destDir in isDirRes["Value"]["Successful"])
        self.assertTrue(isDirRes["Value"]["Successful"][destDir])
        # Check the non existant directory operation
        self.assertTrue(nonExistantDirRes["OK"])
        self.assertTrue(dummyDir in nonExistantDirRes["Value"]["Failed"])
        expectedError = "Path does not exist"
        self.assertTrue(expectedError in nonExistantDirRes["Value"]["Failed"][dummyDir])

    def test_putGetDirectoryMetadata(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet Directory Metadata test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("")["Value"]
        _ = self.storage.removeDirectory(remoteDir, True)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        sizeOfLocalFile = getSize(srcFile)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(self.numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)

        # Check that we can successfully upload the directory to the storage element
        dirDict = {remoteDir: localDir}
        putDirRes = self.storage.putDirectory(dirDict)

        # Get the directory metadata
        getMetadataRes = self.storage.getDirectoryMetadata(remoteDir)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir, True)
        # Clean up the locally created directory
        shutil.rmtree(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        if putDirRes["Value"]["Successful"][remoteDir]["Files"]:
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Files"], self.numberOfFiles)
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Size"], self.numberOfFiles * sizeOfLocalFile)
        self.assertTrue(isinstance(putDirRes["Value"]["Successful"][remoteDir]["Files"], int))
        self.assertTrue(type(putDirRes["Value"]["Successful"][remoteDir]["Size"]) in (int,))
        # Perform the checks for the get metadata operation
        self.assertTrue(getMetadataRes["OK"])
        self.assertTrue(remoteDir in getMetadataRes["Value"]["Successful"])
        resDict = getMetadataRes["Value"]["Successful"][remoteDir]
        self.assertTrue("Mode" in resDict)
        self.assertTrue(isinstance(resDict["Mode"], int))
        # Perform the checks for the remove directory operation
        if removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"]:
            self.assertEqual(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], self.numberOfFiles)
            self.assertEqual(
                removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"], self.numberOfFiles * sizeOfLocalFile
            )
        self.assertTrue(isinstance(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], int))
        self.assertTrue(type(removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"]) in (int,))

    def test_putGetDirectorySize(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet Directory Size test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("")["Value"]
        _ = self.storage.removeDirectory(remoteDir, True)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        sizeOfLocalFile = getSize(srcFile)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(self.numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)
        # Check that we can successfully upload the directory to the storage element
        dirDict = {remoteDir: localDir}
        putDirRes = self.storage.putDirectory(dirDict)
        # Now get the directory size
        getDirSizeRes = self.storage.getDirectorySize(remoteDir)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir, True)
        # Clean up the locally created directory
        shutil.rmtree(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        if putDirRes["Value"]["Successful"][remoteDir]["Files"]:
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Files"], self.numberOfFiles)
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Size"], self.numberOfFiles * sizeOfLocalFile)
        self.assertTrue(isinstance(putDirRes["Value"]["Successful"][remoteDir]["Files"], int))
        self.assertTrue(type(putDirRes["Value"]["Successful"][remoteDir]["Size"]) in (int,))
        # Now perform the checks for the get directory size operation
        self.assertTrue(getDirSizeRes["OK"])
        self.assertTrue(remoteDir in getDirSizeRes["Value"]["Successful"])
        resDict = getDirSizeRes["Value"]["Successful"][remoteDir]
        self.assertTrue(type(resDict["Size"]) in (int,))
        self.assertTrue(isinstance(resDict["Files"], int))
        # Perform the checks for the remove directory operation
        self.assertTrue(removeDirRes["OK"])
        self.assertTrue(remoteDir in removeDirRes["Value"]["Successful"])
        if removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"]:
            self.assertEqual(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], self.numberOfFiles)
            self.assertEqual(
                removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"], self.numberOfFiles * sizeOfLocalFile
            )
        self.assertTrue(isinstance(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], int))
        self.assertTrue(type(removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"]) in (int,))

    def test_putListDirectory(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tList Directory test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("")["Value"]
        _ = self.storage.removeDirectory(remoteDir, True)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        sizeOfLocalFile = getSize(srcFile)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(self.numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)
        # Check that we can successfully upload the directory to the storage element
        dirDict = {remoteDir: localDir}
        putDirRes = self.storage.putDirectory(dirDict)
        # List the remote directory
        listDirRes = self.storage.listDirectory(remoteDir)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir, True)
        # Clean up the locally created directory
        shutil.rmtree(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        if putDirRes["Value"]["Successful"][remoteDir]["Files"]:
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Files"], self.numberOfFiles)
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Size"], self.numberOfFiles * sizeOfLocalFile)
        self.assertTrue(isinstance(putDirRes["Value"]["Successful"][remoteDir]["Files"], int))
        self.assertTrue(type(putDirRes["Value"]["Successful"][remoteDir]["Size"]) in (int,))
        # Perform the checks for the list dir operation
        self.assertTrue(listDirRes["OK"])
        self.assertTrue(remoteDir in listDirRes["Value"]["Successful"])
        resDict = listDirRes["Value"]["Successful"][remoteDir]
        self.assertTrue("SubDirs" in resDict)
        self.assertTrue("Files" in resDict)
        self.assertEqual(len(resDict["Files"]), self.numberOfFiles)
        # Perform the checks for the remove directory operation
        self.assertTrue(removeDirRes["OK"])
        self.assertTrue(remoteDir in removeDirRes["Value"]["Successful"])
        if removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"]:
            self.assertEqual(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], self.numberOfFiles)
            self.assertEqual(
                removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"], self.numberOfFiles * sizeOfLocalFile
            )
        self.assertTrue(isinstance(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], int))
        self.assertTrue(type(removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"]) in (int,))

    def test_putGetDirectory(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet Directory test\n"
        )
        # First clean the remote directory incase something was left there
        remoteDir = self.storage.getCurrentURL("")["Value"]
        _ = self.storage.removeDirectory(remoteDir, True)

        # Create a local directory to upload
        localDir = "/tmp/unit-test"
        srcFile = "/etc/group"
        sizeOfLocalFile = getSize(srcFile)
        if not os.path.exists(localDir):
            os.mkdir(localDir)
        for i in range(self.numberOfFiles):
            shutil.copy(srcFile, f"{localDir}/testFile.{time.time()}")
            time.sleep(1)
        # Check that we can successfully upload the directory to the storage element
        dirDict = {remoteDir: localDir}
        putDirRes = self.storage.putDirectory(dirDict)
        # Clean up the locally created directory
        shutil.rmtree(localDir)
        # Check that we can get directories from the storage element
        getDirRes = self.storage.getDirectory(remoteDir, localPath=localDir)
        # Now remove the remove directory
        removeDirRes = self.storage.removeDirectory(remoteDir, True)
        # Clean up the locally created directory
        shutil.rmtree(localDir)

        # Perform the checks for the put dir operation
        self.assertTrue(putDirRes["OK"])
        self.assertTrue(remoteDir in putDirRes["Value"]["Successful"])
        if putDirRes["Value"]["Successful"][remoteDir]["Files"]:
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Files"], self.numberOfFiles)
            self.assertEqual(putDirRes["Value"]["Successful"][remoteDir]["Size"], self.numberOfFiles * sizeOfLocalFile)
        self.assertTrue(isinstance(putDirRes["Value"]["Successful"][remoteDir]["Files"], int))
        self.assertTrue(type(putDirRes["Value"]["Successful"][remoteDir]["Size"]) in (int,))
        # Perform the checks for the get dir operation
        self.assertTrue(getDirRes["OK"])
        self.assertTrue(remoteDir in getDirRes["Value"]["Successful"])
        resDict = getDirRes["Value"]["Successful"][remoteDir]
        if resDict["Files"]:
            self.assertEqual(resDict["Files"], self.numberOfFiles)
            self.assertEqual(resDict["Size"], self.numberOfFiles * sizeOfLocalFile)
        self.assertTrue(isinstance(resDict["Files"], int))
        self.assertTrue(type(resDict["Size"]) in (int,))
        # Perform the checks for the remove directory operation
        self.assertTrue(removeDirRes["OK"])
        self.assertTrue(remoteDir in removeDirRes["Value"]["Successful"])
        if removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"]:
            self.assertEqual(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], self.numberOfFiles)
            self.assertEqual(
                removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"], self.numberOfFiles * sizeOfLocalFile
            )
        self.assertTrue(isinstance(removeDirRes["Value"]["Successful"][remoteDir]["FilesRemoved"], int))
        self.assertTrue(type(removeDirRes["Value"]["Successful"][remoteDir]["SizeRemoved"]) in (int,))


class FileTestCase(StoragePlugInTestCase):
    def test_putRemoveFile(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tPut and Remove test\n"
        )

        # Make sure that we can actually upload a file properly
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        destFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {destFile: srcFile}
        putFileRes = self.storage.putFile(fileDict)
        # Make sure we are able to remove the file
        removeFileRes = self.storage.removeFile(destFile)

        # Check the successful put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(destFile in putFileRes["Value"]["Successful"])
        self.assertEqual(putFileRes["Value"]["Successful"][destFile], srcFileSize)
        # Check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(destFile in removeFileRes["Value"]["Successful"])
        self.assertTrue(removeFileRes["Value"]["Successful"][destFile])

    def test_putGetFile(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tPut and Get test\n"
        )

        # First upload a file to the storage
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        remoteFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {remoteFile: srcFile}
        putFileRes = self.storage.putFile(fileDict)

        # Then make sure we can get a local copy of the file
        getFileRes = self.storage.getFile(remoteFile)
        # Cleanup the remote mess
        removeFileRes = self.storage.removeFile(remoteFile)
        # Cleanup the mess locally
        os.remove(testFileName)

        # Check the put operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(remoteFile in putFileRes["Value"]["Successful"])
        self.assertEqual(putFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # Check the get operation
        self.assertTrue(getFileRes["OK"])
        self.assertTrue(remoteFile in getFileRes["Value"]["Successful"])
        self.assertEqual(getFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # Check the remove operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(remoteFile in removeFileRes["Value"]["Successful"])
        self.assertTrue(removeFileRes["Value"]["Successful"][remoteFile])

    def test_putExistsFile(self):
        print("\n\n#########################################################" "################\n\n\t\t\tExists test\n")
        # First upload a file to the storage
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        remoteFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {remoteFile: srcFile}
        putFileRes = self.storage.putFile(fileDict)
        # Then get the file's existance
        existsFileRes = self.storage.exists(remoteFile)
        # Now remove the file
        removeFileRes = self.storage.removeFile(remoteFile)
        # Check  again that the file exists
        failedExistRes = self.storage.exists(remoteFile)

        # Check the put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(remoteFile in putFileRes["Value"]["Successful"])
        self.assertEqual(putFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # Check the exists operation
        self.assertTrue(existsFileRes["OK"])
        self.assertTrue(remoteFile in existsFileRes["Value"]["Successful"])
        self.assertTrue(existsFileRes["Value"]["Successful"][remoteFile])
        # Check the removal operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(remoteFile in removeFileRes["Value"]["Successful"])
        # Check the failed exists operation
        self.assertTrue(failedExistRes["OK"])
        self.assertTrue(remoteFile in failedExistRes["Value"]["Successful"])
        self.assertFalse(failedExistRes["Value"]["Successful"][remoteFile])

    def test_putIsFile(self):
        print(
            "\n\n#########################################################" "################\n\n\t\t\tIs file test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        remoteFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {remoteFile: srcFile}
        putFileRes = self.storage.putFile(fileDict)
        # Check we are able to determine that it is a file
        isFileRes = self.storage.isFile(remoteFile)
        # Clean up the remote mess
        removeFileRes = self.storage.removeFile(remoteFile)
        # Check that everything isn't a file
        remoteDir = os.path.dirname(remoteFile)
        failedIsFileRes = self.storage.isFile(remoteDir)

        # Check the put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(remoteFile in putFileRes["Value"]["Successful"])
        self.assertTrue(putFileRes["Value"]["Successful"][remoteFile])
        self.assertEqual(putFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # Check the is file operation
        self.assertTrue(isFileRes["OK"])
        self.assertTrue(remoteFile in isFileRes["Value"]["Successful"])
        self.assertTrue(isFileRes["Value"]["Successful"][remoteFile])
        # check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(remoteFile in removeFileRes["Value"]["Successful"])
        # Check that the directory is not a file
        self.assertTrue(failedIsFileRes["OK"])
        self.assertTrue(remoteDir in failedIsFileRes["Value"]["Successful"])
        self.assertFalse(failedIsFileRes["Value"]["Successful"][remoteDir])

    def test_putGetFileMetaData(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet file metadata test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        remoteFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {remoteFile: srcFile}
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
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(remoteFile in putFileRes["Value"]["Successful"])
        self.assertTrue(putFileRes["Value"]["Successful"][remoteFile])
        self.assertEqual(putFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # Check the get metadata operation
        self.assertTrue(getMetadataRes["OK"])
        self.assertTrue(remoteFile in getMetadataRes["Value"]["Successful"])
        fileMetaData = getMetadataRes["Value"]["Successful"][remoteFile]
        self.assertTrue(fileMetaData["Cached"])
        self.assertFalse(fileMetaData["Migrated"])
        self.assertEqual(fileMetaData["Size"], srcFileSize)
        # check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(remoteFile in removeFileRes["Value"]["Successful"])
        # Check the get metadata for non existant file
        self.assertTrue(failedMetadataRes["OK"])
        self.assertTrue(remoteFile in failedMetadataRes["Value"]["Failed"])
        expectedError = "File does not exist"
        self.assertTrue(expectedError in failedMetadataRes["Value"]["Failed"][remoteFile])
        # Check that metadata operation with a directory
        self.assertTrue(directoryMetadataRes["OK"])
        self.assertTrue(remoteDir in directoryMetadataRes["Value"]["Failed"])
        expectedError = "Supplied path is not a file"
        self.assertTrue(expectedError in directoryMetadataRes["Value"]["Failed"][remoteDir])

    def test_putGetFileSize(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tGet file size test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        remoteFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {remoteFile: srcFile}
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
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(remoteFile in putFileRes["Value"]["Successful"])
        self.assertTrue(putFileRes["Value"]["Successful"][remoteFile])
        self.assertEqual(putFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # Check that we got the file size correctly
        self.assertTrue(getSizeRes["OK"])
        self.assertTrue(remoteFile in getSizeRes["Value"]["Successful"])
        self.assertEqual(getSizeRes["Value"]["Successful"][remoteFile], srcFileSize)
        # check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(remoteFile in removeFileRes["Value"]["Successful"])
        # Check the get size with non existant file works properly
        self.assertTrue(failedSizeRes["OK"])
        self.assertTrue(remoteFile in failedSizeRes["Value"]["Failed"])
        expectedError = "File does not exist"
        self.assertTrue(expectedError in failedSizeRes["Value"]["Failed"][remoteFile])
        # Check that the passing a directory is handled correctly
        self.assertTrue(directorySizeRes["OK"])
        self.assertTrue(remoteDir in directorySizeRes["Value"]["Failed"])
        expectedError = "Supplied path is not a file"
        self.assertTrue(expectedError in directorySizeRes["Value"]["Failed"][remoteDir])

    def test_putPrestageFile(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tFile prestage test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        remoteFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {remoteFile: srcFile}
        putFileRes = self.storage.putFile(fileDict)
        # Check that we can issue a stage request
        prestageRes = self.storage.prestageFile(remoteFile)
        # Clean up the remote mess
        removeFileRes = self.storage.removeFile(remoteFile)
        # Check what happens with deleted files
        deletedPrestageRes = self.storage.prestageFile(remoteFile)

        # Check the put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(remoteFile in putFileRes["Value"]["Successful"])
        self.assertTrue(putFileRes["Value"]["Successful"][remoteFile])
        self.assertEqual(putFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # Check the prestage file operation
        self.assertTrue(prestageRes["OK"])
        self.assertTrue(remoteFile in prestageRes["Value"]["Successful"])
        self.assertTrue(prestageRes["Value"]["Successful"][remoteFile])
        # Check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(remoteFile in removeFileRes["Value"]["Successful"])
        # Check that pre-staging non-existant file fails
        self.assertTrue(deletedPrestageRes["OK"])
        self.assertTrue(remoteFile in deletedPrestageRes["Value"]["Failed"])
        expectedError = "No such file or directory"
        self.assertTrue(expectedError in deletedPrestageRes["Value"]["Failed"][remoteFile])

    def test_putFilegetTransportURL(self):
        print(
            "\n\n#########################################################" "################\n\n\t\t\tGet tURL test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        remoteFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {remoteFile: srcFile}
        putFileRes = self.storage.putFile(fileDict)
        # Check that we can get a turl
        getTurlRes = self.storage.getTransportURL(remoteFile)
        # Clean up the remote mess
        removeFileRes = self.storage.removeFile(remoteFile)
        # Try and get a turl for a non existant file
        failedGetTurlRes = self.storage.getTransportURL(remoteFile)

        # Check the put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(remoteFile in putFileRes["Value"]["Successful"])
        self.assertTrue(putFileRes["Value"]["Successful"][remoteFile])
        self.assertEqual(putFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # check the get turl operation
        self.assertTrue(getTurlRes["OK"])
        self.assertTrue(remoteFile in getTurlRes["Value"]["Successful"])
        # check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(remoteFile in removeFileRes["Value"]["Successful"])
        # Check the get turl with non existant file operation
        self.assertTrue(failedGetTurlRes["OK"])
        self.assertTrue(remoteFile in failedGetTurlRes["Value"]["Failed"])
        expectedError = "File does not exist"
        self.assertTrue(expectedError in failedGetTurlRes["Value"]["Failed"][remoteFile])

    def test_putPinRelease(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tPin and Release test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        remoteFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {remoteFile: srcFile}
        putFileRes = self.storage.putFile(fileDict)
        # Check that we can pin the file
        pinFileRes = self.storage.pinFile(remoteFile)
        srmID = ""
        if pinFileRes["OK"]:
            if remoteFile in pinFileRes["Value"]["Successful"]:
                srmID = pinFileRes["Value"]["Successful"][remoteFile]
        # Check that we can release the file
        releaseFileRes = self.storage.releaseFile({remoteFile: srmID})
        # Clean up the mess
        removeFileRes = self.storage.removeFile(remoteFile)

        # Check the put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(remoteFile in putFileRes["Value"]["Successful"])
        self.assertTrue(putFileRes["Value"]["Successful"][remoteFile])
        self.assertEqual(putFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # Check the pin file operation
        self.assertTrue(pinFileRes["OK"])
        self.assertTrue(remoteFile in pinFileRes["Value"]["Successful"])
        self.assertTrue(type(pinFileRes["Value"]["Successful"][remoteFile]) in (str,))
        # Check the release file operation
        self.assertTrue(releaseFileRes["OK"])
        self.assertTrue(remoteFile in releaseFileRes["Value"]["Successful"])
        # check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(remoteFile in removeFileRes["Value"]["Successful"])

    def test_putPrestageStatus(self):
        print(
            "\n\n#########################################################"
            "################\n\n\t\t\tPrestage status test\n"
        )
        # First upload a file to the storage
        srcFile = "/etc/group"
        srcFileSize = getSize(srcFile)
        testFileName = "testFile.%s" % time.time()
        remoteFile = self.storage.getCurrentURL(testFileName)["Value"]
        fileDict = {remoteFile: srcFile}
        putFileRes = self.storage.putFile(fileDict)
        # Check that we can issue a stage request
        prestageRes = self.storage.prestageFile(remoteFile)
        srmID = ""
        if prestageRes["OK"]:
            if remoteFile in prestageRes["Value"]["Successful"]:
                srmID = prestageRes["Value"]["Successful"][remoteFile]
        # Take a quick break to allow the SRM to realise the file is available
        sleepTime = 10
        print("Sleeping for %s seconds" % sleepTime)
        time.sleep(sleepTime)
        # Check that we can monitor the stage request
        prestageStatusRes = self.storage.prestageFileStatus({remoteFile: srmID})
        # Clean up the remote mess
        removeFileRes = self.storage.removeFile(remoteFile)

        # Check the put file operation
        self.assertTrue(putFileRes["OK"])
        self.assertTrue(remoteFile in putFileRes["Value"]["Successful"])
        self.assertTrue(putFileRes["Value"]["Successful"][remoteFile])
        self.assertEqual(putFileRes["Value"]["Successful"][remoteFile], srcFileSize)
        # Check the prestage file operation
        self.assertTrue(prestageRes["OK"])
        self.assertTrue(remoteFile in prestageRes["Value"]["Successful"])
        self.assertTrue(prestageRes["Value"]["Successful"][remoteFile])
        self.assertTrue(type(prestageRes["Value"]["Successful"][remoteFile]) in (str,))
        # Check the prestage status operation
        self.assertTrue(prestageStatusRes["OK"])
        self.assertTrue(remoteFile in prestageStatusRes["Value"]["Successful"])
        self.assertTrue(prestageStatusRes["Value"]["Successful"][remoteFile])
        # Check the remove file operation
        self.assertTrue(removeFileRes["OK"])
        self.assertTrue(remoteFile in removeFileRes["Value"]["Successful"])


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(FileTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
