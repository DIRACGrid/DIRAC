""" test File Plugin
"""

from unittest import mock
import unittest
import tempfile
import os
import shutil
import errno

from DIRAC import S_OK
from DIRAC.Resources.Storage.StorageElement import StorageElementItem


def mock_StorageFactory_getConfigStorageName(storageName, referenceType, seConfigPath=None):
    resolvedName = storageName
    return S_OK(resolvedName)


def mock_StorageFactory_getConfigStorageOptions(storageName, derivedStorageName=None, seConfigPath=None):
    """Get the options associated to the StorageElement as defined in the CS"""
    optionsDict = {
        "BackendType": "local",
        "ReadAccess": "Active",
        "WriteAccess": "Active",
        "AccessProtocols": ["file"],
        "WriteProtocols": ["file"],
    }
    return S_OK(optionsDict)


def mock_StorageFactory_getConfigStorageProtocols(storageName, derivedStorageName=None, seConfigPath=None):
    """Protocol specific information is present as sections in the Storage configuration"""
    protocolDetails = {
        "Section": {
            "Host": "",
            "Path": "/tmp/se",
            "PluginName": "File",
            "Port": "",
            "Protocol": "file",
            "SpaceToken": "",
            "WSUrl": "",
        }
    }

    return S_OK(protocolDetails)


class TestBase(unittest.TestCase):
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
    def setUp(
        self,
        mk_getConfigStorageName,
        mk_getConfigStorageOptions,
        mk_getConfigStorageProtocols,
        mk_isLocalSE,
        mk_addAccountingOperation,
    ):
        self.se = StorageElementItem("FAKE")
        self.se.vo = "test"

        self.basePath = tempfile.mkdtemp(dir="/tmp")
        # Update the basePath of the plugin
        list(self.se.storages.values())[0].basePath = self.basePath

        self.srcPath = tempfile.mkdtemp(dir="/tmp")

        self.destPath = tempfile.mkdtemp(dir="/tmp")

        self.existingFile = "/test/file.txt"
        self.existingFileSize = 0

        self.nonExistingFile = "/test/nonExistingFile.txt"
        self.subDir = "/test/subDir"
        self.subFile = os.path.join(self.subDir, "subFile.txt")
        self.subFileSize = 0

        self.FILES = [self.existingFile, self.nonExistingFile, self.subFile]
        self.DIRECTORIES = [self.subDir]
        self.ALL = self.FILES + self.DIRECTORIES

        with open(os.path.join(self.srcPath, self.existingFile.replace("/test/", "")), "w") as f:
            f.write("I put something in the file so that it has a size\n")
        self.existingFileSize = os.path.getsize(os.path.join(self.srcPath, self.existingFile.replace("/test/", "")))

        assert self.existingFileSize

        os.mkdir(os.path.join(self.srcPath, os.path.basename(self.subDir)))

        with open(os.path.join(self.srcPath, self.subFile.replace("/test/", "")), "w") as f:
            f.write("This one should have a size as well\n")
        self.subFileSize = os.path.getsize(os.path.join(self.srcPath, self.subFile.replace("/test/", "")))

        assert self.subFileSize

    def tearDown(self):
        shutil.rmtree(self.basePath)
        shutil.rmtree(self.srcPath)
        shutil.rmtree(self.destPath)
        pass

    def walkAll(self):
        for dirname in [self.basePath, self.destPath]:
            self.walkPath(dirname)

    def walkPath(self, path):
        for root, dirs, files in os.walk(path):
            print(root)
            print("  dirs")
            for d in dirs:
                print("    ", os.path.join(root, d))
            print("  files")
            for f in files:
                print("    ", os.path.join(root, f))

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_01_getURL(self, mk_isLocalSE, mk_addAccounting):
        """Testing getURL"""
        # Testing the getURL
        res = self.se.getURL(self.ALL)
        self.assertTrue(res["OK"], res)
        self.assertTrue(not res["Value"]["Failed"], res["Value"]["Failed"])
        self.assertTrue(len(res["Value"]["Successful"]) == len(self.ALL))
        for lfn, url in res["Value"]["Successful"].items():
            self.assertEqual(url, self.basePath.rstrip("/") + lfn)

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_02_FileTest(self, mk_isLocalSE, mk_addAccounting):
        """Testing createDirectory"""
        # Putting the files

        def localPutFile(fn, size=0):
            """If fn is '/test/fn.txt', it calls
            { '/test/fn.txt' : /tmp/generatedPath/fn.txt}
            """
            transfDic = {fn: os.path.join(self.srcPath, fn.replace("/test/", ""))}
            return self.se.putFile(transfDic, sourceSize=size)

        # wrong size
        res = localPutFile(self.existingFile, size=-1)
        self.assertTrue(res["OK"], res)
        self.assertTrue(self.existingFile in res["Value"]["Failed"], res)
        self.assertTrue("not match" in res["Value"]["Failed"][self.existingFile], res)
        self.assertTrue(not os.path.exists(self.basePath + self.existingFile))

        # Correct size
        res = localPutFile(self.existingFile, size=self.existingFileSize)
        self.assertTrue(res["OK"], res)
        self.assertTrue(self.existingFile in res["Value"]["Successful"], res)
        self.assertTrue(os.path.exists(self.basePath + self.existingFile))

        # No size
        res = localPutFile(self.existingFile)
        self.assertTrue(res["OK"], res)
        self.assertTrue(self.existingFile in res["Value"]["Successful"], res)
        self.assertTrue(os.path.exists(self.basePath + self.existingFile))

        # No existing source file
        res = localPutFile(self.nonExistingFile)
        self.assertTrue(res["OK"], res)
        self.assertTrue(self.nonExistingFile in res["Value"]["Failed"], res)
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][self.nonExistingFile], res)

        # sub file
        res = localPutFile(self.subFile)
        self.assertTrue(res["OK"], res)
        self.assertTrue(self.subFile in res["Value"]["Successful"], res)
        self.assertTrue(os.path.exists(self.basePath + self.subFile))

        # Directory
        res = localPutFile(self.subDir)
        self.assertTrue(res["OK"], res)
        self.assertTrue(self.subDir in res["Value"]["Failed"])
        self.assertTrue(
            os.strerror(errno.EISDIR) in res["Value"]["Failed"][self.subDir] or
            # Python 3.9.7+ improved the Exception that is raised
            "Directory does not exist" in res["Value"]["Failed"][self.subDir],
            res,
        )

        res = self.se.exists(self.FILES)
        self.assertTrue(res["OK"], res)
        self.assertTrue(not res["Value"]["Failed"], res)
        self.assertTrue(res["Value"]["Successful"][self.existingFile], res)
        self.assertTrue(not res["Value"]["Successful"][self.nonExistingFile], res)

        res = self.se.getFileSize(self.ALL)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Successful"][self.existingFile], self.existingFileSize)
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][self.nonExistingFile], res)
        self.assertTrue(os.strerror(errno.EISDIR) in res["Value"]["Failed"][self.subDir], res)

        res = self.se.getFileMetadata(self.ALL)
        self.assertTrue(res["OK"], res)
        self.assertTrue(self.existingFile in res["Value"]["Successful"])
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][self.nonExistingFile], res)
        self.assertTrue(os.strerror(errno.EISDIR) in res["Value"]["Failed"][self.subDir], res)

        res = self.se.isFile(self.ALL)
        self.assertTrue(res["OK"], res)
        self.assertTrue(res["Value"]["Successful"][self.existingFile], res)
        self.assertTrue(not res["Value"]["Successful"][self.subDir], res)
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][self.nonExistingFile], res)

        res = self.se.getFile(self.ALL, localPath=self.destPath)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Successful"][self.existingFile], self.existingFileSize)
        self.assertTrue(os.path.exists(os.path.join(self.destPath, os.path.basename(self.existingFile))))
        self.assertEqual(res["Value"]["Successful"][self.subFile], self.subFileSize)
        self.assertTrue(os.path.exists(os.path.join(self.destPath, os.path.basename(self.subFile))))
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][self.nonExistingFile], res)
        self.assertTrue(
            os.strerror(errno.EISDIR) in res["Value"]["Failed"][self.subDir] or
            # Python 3.9.7+ improved the Exception that is raised
            "Directory does not exist" in res["Value"]["Failed"][self.subDir],
            res,
        )

        res = self.se.removeFile(self.ALL)
        self.assertTrue(res["OK"], res)
        self.assertTrue(res["Value"]["Successful"][self.existingFile])
        self.assertTrue(not os.path.exists(self.basePath + self.existingFile))
        self.assertTrue(res["Value"]["Successful"][self.subFile])
        self.assertTrue(not os.path.exists(self.basePath + self.subFile))
        self.assertTrue(res["Value"]["Successful"][self.nonExistingFile])
        self.assertTrue(
            os.strerror(errno.EISDIR) in res["Value"]["Failed"][self.subDir]
            or
            # macOS raises EPERM instead of EISDIR, see python/cpython#95815
            os.strerror(errno.EPERM) in res["Value"]["Failed"][self.subDir]
        )

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_03_createDirectory(self, mk_isLocalSE, mk_addAccounting):
        """Testing creating directories"""

        res = self.se.createDirectory(self.subDir)
        self.assertTrue(res["OK"], res)
        self.assertTrue(self.subDir in res["Value"]["Successful"])
        self.assertTrue(os.path.exists(self.basePath + self.subDir))

    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE",
        return_value=S_OK(True),
    )  # Pretend it's local
    @mock.patch(
        "DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation", return_value=None
    )  # Don't send accounting
    def test_04_putDirectory(self, mk_isLocalSE, mk_addAccounting):
        """Testing putDirectory"""

        nonExistingDir = "/test/forsuredoesnotexist"
        localdirs = ["/test", nonExistingDir]

        # Correct size
        res = self.se.putDirectory({"/test": self.srcPath})
        self.assertTrue(res["OK"], res)
        self.assertTrue("/test" in res["Value"]["Successful"], res)
        self.assertEqual(
            res["Value"]["Successful"]["/test"], {"Files": 2, "Size": self.existingFileSize + self.subFileSize}
        )
        self.assertTrue(os.path.exists(self.basePath + "/test"))
        self.assertTrue(os.path.exists(self.basePath + self.existingFile))
        self.assertTrue(os.path.exists(self.basePath + self.subFile))

        # No existing source directory
        res = self.se.putDirectory({"/test": nonExistingDir})
        self.assertTrue(res["OK"], res)
        self.assertTrue("/test" in res["Value"]["Failed"], res)
        self.assertEqual(res["Value"]["Failed"]["/test"], {"Files": 0, "Size": 0})

        # sub file
        res = self.se.putDirectory({"/test": self.existingFile})
        self.assertTrue(res["OK"], res)
        self.assertTrue("/test" in res["Value"]["Failed"], res)
        self.assertEqual(res["Value"]["Failed"]["/test"], {"Files": 0, "Size": 0})

        res = self.se.exists(self.DIRECTORIES + localdirs)
        self.assertTrue(res["OK"], res)
        self.assertTrue(not res["Value"]["Failed"], res)
        self.assertTrue(res["Value"]["Successful"][self.subDir], res)
        self.assertTrue(not res["Value"]["Successful"][nonExistingDir], res)

        res = self.se.getDirectorySize(self.ALL + localdirs)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Successful"][self.subDir], {"Files": 1, "Size": self.subFileSize, "SubDirs": 0})
        self.assertEqual(res["Value"]["Successful"]["/test"], {"Files": 1, "Size": self.existingFileSize, "SubDirs": 1})
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][self.nonExistingFile], res)
        self.assertTrue(os.strerror(errno.ENOTDIR) in res["Value"]["Failed"][self.existingFile], res)
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][nonExistingDir], res)

        res = self.se.getDirectoryMetadata(self.ALL + localdirs)
        self.assertTrue(res["OK"], res)
        self.assertTrue(self.subDir in res["Value"]["Successful"])
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][self.nonExistingFile], res)
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][nonExistingDir], res)
        self.assertTrue(os.strerror(errno.ENOTDIR) in res["Value"]["Failed"][self.existingFile], res)

        res = self.se.isDirectory(self.ALL + localdirs)
        self.assertTrue(res["OK"], res)
        self.assertTrue(not res["Value"]["Successful"][self.existingFile])
        self.assertTrue(res["Value"]["Successful"][self.subDir], res)
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][self.nonExistingFile], res)
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][nonExistingDir], res)

        res = self.se.listDirectory(self.ALL + localdirs)
        self.assertTrue(res["OK"], res)

        self.assertEqual(sorted(list(res["Value"]["Successful"][self.subDir])), sorted(["Files", "SubDirs"]))
        self.assertEqual(list(res["Value"]["Successful"][self.subDir]["Files"]), [self.subFile])
        self.assertEqual(list(res["Value"]["Successful"][self.subDir]["SubDirs"]), [])

        self.assertEqual(list(res["Value"]["Successful"]["/test"]["Files"]), [self.existingFile])
        self.assertEqual(list(res["Value"]["Successful"]["/test"]["SubDirs"]), [self.subDir])

        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][self.nonExistingFile], res)
        self.assertTrue(os.strerror(errno.ENOTDIR) in res["Value"]["Failed"][self.existingFile], res)
        self.assertTrue(os.strerror(errno.ENOENT) in res["Value"]["Failed"][nonExistingDir], res)

        res = self.se.getDirectory(self.ALL + localdirs, localPath=self.destPath)
        self.assertTrue(res["OK"], res)
        self.assertEqual(
            res["Value"]["Successful"]["/test"], {"Files": 2, "Size": self.existingFileSize + self.subFileSize}
        )
        self.assertTrue(os.path.exists(self.destPath + self.existingFile))
        self.assertTrue(os.path.exists(self.destPath + self.subFile))
        self.assertEqual(res["Value"]["Successful"][self.subDir], {"Files": 1, "Size": self.subFileSize})
        self.assertTrue(os.path.exists(self.destPath + self.subFile.replace("/test", "")))
        self.assertEqual(res["Value"]["Failed"][self.nonExistingFile], {"Files": 0, "Size": 0})
        self.assertEqual(res["Value"]["Failed"][self.existingFile], {"Files": 0, "Size": 0})
        self.assertEqual(res["Value"]["Failed"][nonExistingDir], {"Files": 0, "Size": 0})

        res = self.se.removeDirectory(nonExistingDir, recursive=False)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Successful"][nonExistingDir], True)

        res = self.se.removeDirectory(nonExistingDir, recursive=True)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Failed"][nonExistingDir], {"FilesRemoved": 0, "SizeRemoved": 0})

        res = self.se.removeDirectory(self.nonExistingFile, recursive=False)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Successful"][self.nonExistingFile], True)

        res = self.se.removeDirectory(self.nonExistingFile, recursive=True)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Failed"][self.nonExistingFile], {"FilesRemoved": 0, "SizeRemoved": 0})

        res = self.se.removeDirectory(self.existingFile, recursive=False)
        self.assertTrue(res["OK"], res)
        self.assertTrue(os.strerror(errno.ENOTDIR) in res["Value"]["Failed"][self.existingFile], res)

        res = self.se.removeDirectory(self.existingFile, recursive=True)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Failed"][self.existingFile], {"FilesRemoved": 0, "SizeRemoved": 0})

        res = self.se.removeDirectory("/test", recursive=False)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Successful"]["/test"], True)
        self.assertTrue(not os.path.exists(self.basePath + self.existingFile))
        self.assertTrue(os.path.exists(self.basePath + self.subFile))

        res = self.se.removeDirectory("/test", recursive=True)
        self.assertTrue(res["OK"], res)
        self.assertEqual(res["Value"]["Successful"]["/test"], {"FilesRemoved": 1, "SizeRemoved": self.subFileSize})
        self.assertTrue(not os.path.exists(self.basePath + "/test"))


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestBase)

    unittest.TextTestRunner(verbosity=2).run(suite)
