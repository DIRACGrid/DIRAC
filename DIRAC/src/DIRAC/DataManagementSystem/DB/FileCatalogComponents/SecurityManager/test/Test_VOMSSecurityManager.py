""" VOMSSecurityManager unit tests
"""
# pylint: disable=protected-access,missing-docstring,invalid-name,too-many-lines,no-value-for-parameter

import unittest
import stat

from unittest import mock
from DIRAC import S_OK, S_ERROR
import DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.VOMSSecurityManager

# This just defines a few groups with their VOMSRole
diracGrps = {
    "grp_admin": None,
    "grp_data": "vomsProd",
    "grp_mc": "vomsProd",
    "grp_user": "vomsUser",
    "grp_nothing": None,
}


# A dictionary of directories. The keys are path,
# the values are another dic with keys 'owner', 'OwnerGroup' and 'mode'
directoryTree = {}

# Same as directoryTree, but for files
fileTree = {}


def setupTree():
    """This method sets up the hierarchy, directories and files, and stores them in
    directoryTree and fileTree
    """

    def makeNode(owner, group, mode):
        """Just returns a dictionary, keys are the parm names, and associated to their values"""
        return {"owner": owner, "OwnerGroup": group, "mode": mode}

    def setupFiles():
        """Internal method adding the files"""

        global fileTree
        fileTree = {}

        fileTree["/atTheRoot.txt"] = makeNode("admin", "grp_admin", 0o775)

        fileTree["/realData/run1/run1_data.txt"] = makeNode("dm", "grp_data", 0o775)
        fileTree["/realData/run2/run2_data.txt"] = makeNode("dm", "grp_data", 0o705)

        # We should be able to replicate but not remove that file
        fileTree["/realData/run3/run3_data.txt"] = makeNode("dm", "grp_data", 0o775)

        fileTree["/users/usr1/usr1_file.txt"] = makeNode("usr1", "grp_user", 0o755)
        fileTree["/users/usr1/sub1/usr1_secret.txt"] = makeNode("usr1", "grp_user", 0o700)

        fileTree["/users/usr2/usr2_file.txt"] = makeNode("usr2", "grp_user", 0o700)

    global directoryTree
    directoryTree = {}

    # Only root and members from grp_admin should be able to create something in the root directory
    # Others cannot read it
    directoryTree["/"] = makeNode("admin", "grp_admin", 0o770)

    # groups with vomsProd should be able to create and remove directories
    # in /realData
    directoryTree["/realData"] = makeNode("dm", "grp_data", 0o775)
    directoryTree["/realData/run1"] = makeNode("dm", "grp_data", 0o775)
    directoryTree["/realData/run2"] = makeNode("dm", "grp_data", 0o775)
    # No one should be able to write in this one
    directoryTree["/realData/run3"] = makeNode("otherdm", "grp_data", 0o555)

    # Only root can create new user directories or remove them
    # Only the user can create or remove its subdirs
    directoryTree["/users"] = makeNode("admin", "grp_admin", 0o755)
    directoryTree["/users/usr1"] = makeNode("usr1", "grp_user", 0o755)
    directoryTree["/users/usr1/sub1"] = makeNode("usr1", "grp_user", 0o700)
    directoryTree["/users/usr2"] = makeNode("usr2", "grp_user", 0o755)
    directoryTree["/users/usr2/sub1"] = makeNode("usr2", "grp_user", 0o755)

    # grp_data and grp_mc atre both prodVoms role so should be able to write
    directoryTree["/mc"] = makeNode("mc1", "grp_mc", 0o775)
    directoryTree["/mc/prod1"] = makeNode("mc1", "grp_data", 0o775)
    directoryTree["/mc/prod2"] = makeNode("mc2", "grp_mc", 0o775)

    setupFiles()


# List of non existing directories.
nonExistingDirectories = ["/realData/futurRun", "/fakeBaseDir", "/users/usr1/subUsr1", "/users/usr2/subUsr2"]

# List of non existing files.
nonExistingFiles = ["/realData/futurRun/futur_data.txt", "/fakeBaseDir/fake_file.txt", "/fake_base.txt"]


class mock_DirectoryManager:
    """This class is a mock of a directory manager.
    It takes the information from directoryTree instead of the DB
    """

    def __init__(self):
        pass

    def exists(self, lfns):
        return S_OK({"Successful": {lfn: lfn in directoryTree for lfn in lfns}, "Failed": {}})

    def getDirectoryParameters(self, path):
        return S_OK(directoryTree[path]) if path in directoryTree else S_ERROR("Directory not found")

    def getDirectoryPermissions(self, path, credDict):
        if path not in directoryTree:
            return S_ERROR("Directory not found")

        owner = credDict["username"] == directoryTree[path]["owner"]
        group = credDict["group"] == directoryTree[path]["OwnerGroup"]
        mode = directoryTree[path]["mode"]
        resultDict = {}

        resultDict["Read"] = (
            (owner and mode & stat.S_IRUSR > 0) or (group and mode & stat.S_IRGRP > 0) or mode & stat.S_IROTH > 0
        )

        resultDict["Write"] = (
            (owner and mode & stat.S_IWUSR > 0) or (group and mode & stat.S_IWGRP > 0) or mode & stat.S_IWOTH > 0
        )

        resultDict["Execute"] = (
            (owner and mode & stat.S_IXUSR > 0) or (group and mode & stat.S_IXGRP > 0) or mode & stat.S_IXOTH > 0
        )

        return S_OK(resultDict)


class mock_FileManager:
    """This class is a mock of a file manager.
    It takes the information from fileTree instead of the DB
    """

    def __init__(self):
        pass

    def exists(self, lfns):
        return S_OK({"Successful": {lfn: lfn in fileTree for lfn in lfns}, "Failed": {}})

    def getFileMetadata(self, lfns):
        if not isinstance(lfns, list):
            lfns = [lfns]

        retParam = [
            "Size",
            "Checksum",
            "ChecksumType",
            "UID",
            "GID",
            "GUID",
            "CreationDate",
            "ModificationDate",
            "Mode",
            "Status",
        ]

        successful = {}
        failed = {}
        for filename in lfns:
            if filename not in fileTree:
                failed[filename] = "No such file or directory"
                continue

            val = dict.fromkeys(retParam, "mockValues")
            val["Owner"] = fileTree[filename]["owner"]
            val["OwnerGroup"] = fileTree[filename]["OwnerGroup"]
            val["Mode"] = fileTree[filename]["mode"]

            successful[filename] = val

        return S_OK({"Successful": successful, "Failed": failed})

    def getPathPermissions(self, lfns, credDict):
        if not isinstance(lfns, list):
            lfns = [lfns]

        successful = {}
        failed = {}
        for filename in lfns:
            if filename not in fileTree:
                failed[filename] = "File not found"
                continue

            owner = credDict["username"] == fileTree[filename]["owner"]
            group = credDict["group"] == fileTree[filename]["OwnerGroup"]
            mode = fileTree[filename]["mode"]

            resultDict = {}

            resultDict["Read"] = (
                (owner and mode & stat.S_IRUSR > 0) or (group and mode & stat.S_IRGRP > 0) or mode & stat.S_IROTH > 0
            )

            resultDict["Write"] = (
                (owner and mode & stat.S_IWUSR > 0) or (group and mode & stat.S_IWGRP > 0) or mode & stat.S_IWOTH > 0
            )

            resultDict["Execute"] = (
                (owner and mode & stat.S_IXUSR > 0) or (group and mode & stat.S_IXGRP > 0) or mode & stat.S_IXOTH > 0
            )

            successful[filename] = resultDict

        return S_OK({"Successful": successful, "Failed": failed})


class mock_db:
    """This class is a mock of a FileCatalogDB.
    It just contains dtree and fileManager references
    and sets the globalReadAccess to False
    """

    def __init__(self):
        self.globalReadAccess = False
        self.dtree = mock_DirectoryManager()
        self.fileManager = mock_FileManager()


class mock_SecurityManagerBase:
    """This class is a mock of a security manager.
    It just mockes the hasAdminAccess method
    """

    def __init__(self, database=False):
        self.db = mock_db()

    def hasAdminAccess(self, credDict):
        """Returns true only if the group is grp_admin"""
        return S_OK(credDict["group"] == "grp_admin")


# The mock module could not trick the inheritance mechanism, so I do it myself
DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.VOMSSecurityManager.VOMSSecurityManager.__bases__ = (
    mock_SecurityManagerBase,
)


def mock_getAllGroups():
    """Mocks the getAllGroups method from the CS"""
    return list(diracGrps)


def mock_getGroupOption(grpName, grpOption):
    """Mocks the getGroupOption method from the CS,
    only for vomsRole option
    """
    return diracGrps[grpName]


class BaseCaseMixin:
    """Base test class. Defines all the method to test"""

    @mock.patch(
        "DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.VOMSSecurityManager.getGroupOption",
        side_effect=mock_getGroupOption,
    )
    @mock.patch(
        "DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.VOMSSecurityManager.getAllGroups",
        side_effect=mock_getAllGroups,
    )
    def setUp(self, _a, _b):
        global directoryTree
        global fileTree
        # A dictionary of directories. The keys are path,
        # the values are another dic with keys 'owner', 'OwnerGroup' and 'mode'
        directoryTree = {}

        # Same as directoryTree, but for files
        fileTree = {}

        setupTree()

        # These two dict have to be defined by the children class and method,
        # and are used to compare against the output of the test
        self.expectedExistingRet = None
        self.expectedNonExistingRet = None

        # Manager object
        self.securityManager = (
            DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.VOMSSecurityManager.VOMSSecurityManager()
        )
        self.credDict = {"username": "anon", "group": "grp_admin"}

    def callForDirectories(self, methodName):
        """Test the permissions of a given file catalog method
        against all the existing and non existing directories
        """

        self.existingRet = self.securityManager.hasAccess(methodName, list(directoryTree), self.credDict)
        self.nonExistingRet = self.securityManager.hasAccess(methodName, nonExistingDirectories, self.credDict)

    def callForFiles(self, methodName):
        """Test the permissions of a given file catalog method
        against all the existing and non existing files
        """

        self.existingRet = self.securityManager.hasAccess(methodName, list(fileTree), self.credDict)
        self.nonExistingRet = self.securityManager.hasAccess(methodName, nonExistingFiles, self.credDict)

    def compareResult(self):
        """Compares the result between the test output and the expected values"""

        for testSet, real, expected in [
            ("Existing", self.existingRet, self.expectedExistingRet),
            ("NonExisting", self.nonExistingRet, self.expectedNonExistingRet),
        ]:
            self.assertTrue(real, "The method was not run")
            self.assertTrue(expected, "No expected results given")

            self.assertTrue(real["OK"] == expected["OK"], real)

            for dic in ["Successful", "Failed"]:
                dicReal = real["Value"][dic]
                dicExpected = expected["Value"][dic]

                notExpected = set(dicReal) - set(dicExpected)
                self.assertTrue(
                    notExpected == set(), f"({testSet}) Returned more keys in {dic} than expected {notExpected}"
                )

                notReturned = set(dicExpected) - set(dicReal)
                self.assertTrue(notReturned == set(), f"Some keys in {dic} are missing {notReturned}")

                for k in dicReal:
                    self.assertTrue(
                        dicReal[k] == dicExpected[k],
                        "(%s) Incompatible result for %s:\n\treal : %s\n\texpected %s"
                        % (testSet, k, dicReal[k], dicExpected[k]),
                    )

    def test_removeDirectory(self):
        """This is to test the special policy for removing directories"""

        self.callForDirectories("removeDirectory")
        self.compareResult()

    def test_createDirectory(self):
        """This is to test the default write policy for directories"""

        self.callForDirectories("createDirectory")
        self.compareResult()

    def test_listDirectory(self):
        """This is to test the special policy for listing directories"""

        self.callForDirectories("listDirectory")
        self.compareResult()

    def test_getDirectorySize(self):
        """This is to test the default read policy for directories"""

        self.callForDirectories("getDirectorySize")
        self.compareResult()

    def test_addFile(self):
        """This is to test the default write policy for files"""

        self.callForFiles("addFile")
        self.compareResult()

    def test_removeFile(self):
        """This is to test the special policy for removing files"""

        self.callForFiles("removeFile")
        self.compareResult()

    def test_getFileSize(self):
        """This is to test the default read policy for files"""

        self.callForFiles("getFileSize")
        self.compareResult()

    def test_changePathOwner(self):
        """This is to test policy for which we need to be admin"""

        self.callForFiles("changePathOwner")
        self.compareResult()

    def test_getReplicas(self):
        """This is to test the default read policy for replicas"""

        self.callForFiles("getReplicas")
        self.compareResult()

    def test_addReplica(self):
        """This is to test the default write policy for replicas"""

        self.callForFiles("addReplica")
        self.compareResult()


class TestNonExistingUser(BaseCaseMixin, unittest.TestCase):
    """As anonymous user and no group"""

    def setUp(self):
        super().setUp()
        self.credDict = {"username": "anon", "group": "grp_nothing"}

    def test_removeDirectory(self):
        """Removing directory with (anon, grp_nothing)
        Anonymous should not be able to remove any existing directory, but has
        the permission to remove non existing one
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(directoryTree, False), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_removeDirectory()

    def test_createDirectory(self):
        """Creating directory with (anon, grp_nothing)
        An anonymous user should not be able to create anything
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(directoryTree, False), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, False), "Failed": {}})

        super().test_createDirectory()

    def test_listDirectory(self):
        """Listing directory with (anon, grp_nothing)
        Anonymous should not be granted the other bit
        """
        existingDic = {
            "/": False,
            "/realData": True,
            # owner of /realData is dm
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": True,
            "/users/usr1": True,
            "/users/usr1/sub1": False,  # sub1 is 700
            "/users/usr2": True,
            "/users/usr2/sub1": True,
            # Owner group of /mc has vomsProd as well
            "/mc": True,
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,
            "/users/usr2/subUsr2": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_listDirectory()

    def test_getDirectorySize(self):
        """Getting directory size with (anon, grp_nothing)
        Anonymous should not be granted the other bit
        """
        existingDic = {
            "/": False,
            "/realData": False,  # / is not readable by other
            # owner of /realData is dm
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": False,  # / is not readable by other
            "/users/usr1": True,
            "/users/usr1/sub1": True,  # usr1 is readable by all
            "/users/usr2": True,
            "/users/usr2/sub1": True,
            # Owner group of /mc has vomsProd as well
            "/mc": False,
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,
            "/users/usr2/subUsr2": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getDirectorySize()

    def test_addFile(self):
        """Adding files with (anon, grp_nothing)
        An anonymous user should not be able to create anything
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, False), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, False), "Failed": {}})

        super().test_addFile()

    def test_removeFile(self):
        """Removing files with (anon, grp_nothing)
        An anonymous user should not be able to remove anything,
        except non existing files
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, False), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_removeFile()

    def test_getFileSize(self):
        """Checking file size with (anon, grp_nothing)"""
        existingDic = {
            "/atTheRoot.txt": False,  # No read permission on /
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": False,  # sub1 is 700
            "/users/usr2/usr2_file.txt": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,  # Read permission on /realdata
            "/fakeBaseDir/fake_file.txt": False,  # No read permission on /
            "/fake_base.txt": False,  # No read permission on /
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getFileSize()

    def test_changePathOwner(self):
        """Setting fiel owner with (anon, grp_nothing)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, False), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, False), "Failed": {}})

        super().test_changePathOwner()

    def test_getReplicas(self):
        """Checking get replicas with (anon, grp_nothing)"""
        existingDic = {
            "/atTheRoot.txt": True,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": False,  # usr1_secret.txt is 700
            "/users/usr2/usr2_file.txt": False,  # usr2_file.txt is 700
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": True,
            "/fake_base.txt": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getReplicas()

    def test_addReplica(self):
        """Adding replicas with (anon, grp_nothing)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": False,
            "/realData/run2/run2_data.txt": False,
            "/realData/run3/run3_data.txt": False,
            "/users/usr1/usr1_file.txt": False,
            "/users/usr1/sub1/usr1_secret.txt": False,  # usr1_secret.txt is 700
            "/users/usr2/usr2_file.txt": False,  # usr2_file.txt is 700
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": True,
            "/fake_base.txt": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_addReplica()


class TestAdminGrpAnonUser(BaseCaseMixin, unittest.TestCase):
    """The grp_admin has adminAccess so should be able to do everything"""

    def setUp(self):
        super().setUp()
        self.credDict = {"username": "anon", "group": "grp_admin"}

    def test_removeDirectory(self):
        """Removing directory with (anon, grp_admin)
        The grp_admin has adminAccess so should be able to do everything
        """

        existingDic = dict.fromkeys(directoryTree, True)

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_removeDirectory()

    def test_createDirectory(self):
        """Creating directory with (anon, grp_admin)
        Everything should be granted
        """

        existingDic = dict.fromkeys(directoryTree, True)

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_createDirectory()

    def test_listDirectory(self):
        """Listing directory with (anon, grp_admin)
        Everything should be granted
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(directoryTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_listDirectory()

    def test_getDirectorySize(self):
        """Getting directory size with (anon, grp_admin)
        Everything should be granted
        """
        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(directoryTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_getDirectorySize()

    def test_addFile(self):
        """Adding files with (anon, grp_admin)
        Everything should be granted
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_addFile()

    def test_removeFile(self):
        """Removing files with (anon, grp_admin)
        Everything should be granted
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_removeFile()

    def test_getFileSize(self):
        """Checking file size with (anon, grp_admin)"""
        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_getFileSize()

    def test_changePathOwner(self):
        """Setting fiel owner with (anon, grp_admin)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_changePathOwner()

    def test_getReplicas(self):
        """Checking get replicas with (anon, grp_admin)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_getReplicas()

    def test_addReplica(self):
        """Adding  replicas with (anon, grp_admin)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_addReplica()


class TestAdminGrpAdminUser(BaseCaseMixin, unittest.TestCase):
    """The grp_admin has adminAccess so should be able to do everything"""

    def setUp(self):
        super().setUp()
        self.credDict = {"username": "admin", "group": "grp_admin"}

    def test_removeDirectory(self):
        """Removing directory with (admin, grp_admin)
        Everything should be granted
        """

        existingDic = dict.fromkeys(directoryTree, True)

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_removeDirectory()

    def test_createDirectory(self):
        """Creating directory with (admin, grp_admin)
        Everything should be granted
        """

        existingDic = dict.fromkeys(directoryTree, True)

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_createDirectory()

    def test_listDirectory(self):
        """Listing directory with (admin, grp_admin)
        Everything should be granted
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(directoryTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_listDirectory()

    def test_getDirectorySize(self):
        """Getting directory size with (admin, grp_admin)
        Everything should be granted
        """
        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(directoryTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_getDirectorySize()

    def test_addFile(self):
        """Adding files with (admin, grp_admin)
        Everything should be granted
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_addFile()

    def test_removeFile(self):
        """Removing files with (admin, grp_admin)
        Everything should be granted
        """

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_removeFile()

    def test_getFileSize(self):
        """Adding files with (admin, grp_admin)"""
        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_getFileSize()

    def test_changePathOwner(self):
        """Setting fiel owner with (admin, grp_admin)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_changePathOwner()

    def test_getReplicas(self):
        """Checking get replicas with (admin, grp_admin)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_getReplicas()

    def test_addReplica(self):
        """Adding replicas with (admin, grp_admin)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, True), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_addReplica()


class TestDataGrpDmUser(BaseCaseMixin, unittest.TestCase):
    """Should have the permission of the 'dm' user and the group
    permission of all the vomsRole 'prod' (grp_data, grp_mc)
    """

    def setUp(self):
        super().setUp()
        self.credDict = {"username": "dm", "group": "grp_data"}

    def test_removeDirectory(self):
        """Removing directory with (dm, grp_data)"""

        existingDic = {
            "/": False,
            "/realData": False,
            # owner of /realData is dm
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": False,
            "/users/usr1": False,
            "/users/usr1/sub1": False,
            "/users/usr2": False,
            "/users/usr2/sub1": False,
            "/mc": False,
            # Owner group of /mc has vomsProd as well
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_removeDirectory()

    def test_createDirectory(self):
        """Creating directory with (dm, grp_data)"""

        existingDic = {
            "/": False,
            "/realData": False,
            # owner of /realData is dm
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": False,
            "/users/usr1": False,
            "/users/usr1/sub1": False,
            "/users/usr2": False,
            "/users/usr2/sub1": False,
            "/mc": False,
            # Owner group of /mc has vomsProd as well
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": False,
            "/users/usr2/subUsr2": False,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_createDirectory()

    def test_listDirectory(self):
        """Listing directory with (dm, grp_data)"""

        existingDic = {
            "/": False,
            "/realData": True,
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": True,
            "/users/usr1": True,
            "/users/usr1/sub1": False,  # sub1 is 700
            "/users/usr2": True,
            "/users/usr2/sub1": True,
            "/mc": True,
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,
            "/users/usr2/subUsr2": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_listDirectory()

    def test_getDirectorySize(self):
        """Getting directory size with (dm, grp_data)"""

        existingDic = {
            "/": False,
            "/realData": False,
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": False,
            "/users/usr1": True,
            "/users/usr1/sub1": True,
            "/users/usr2": True,
            "/users/usr2/sub1": True,
            "/mc": False,
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,
            "/users/usr2/subUsr2": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getDirectorySize()

    def test_addFile(self):
        """Adding files with (dm, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": False,
            "/users/usr1/usr1_file.txt": False,
            "/users/usr1/sub1/usr1_secret.txt": False,  # sub1 is 700
            "/users/usr2/usr2_file.txt": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": False,
            "/fake_base.txt": False,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_addFile()

    def test_removeFile(self):
        """Removing files with (dm, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": False,
            "/users/usr1/usr1_file.txt": False,
            "/users/usr1/sub1/usr1_secret.txt": False,  # sub1 is 700
            "/users/usr2/usr2_file.txt": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_removeFile()

    def test_getFileSize(self):
        """Checking file size with (dm, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": False,  # sub1 is 700
            "/users/usr2/usr2_file.txt": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": False,
            "/fake_base.txt": False,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getFileSize()

    def test_changePathOwner(self):
        """Setting fiel owner with (dm, grp_data)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, False), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, False), "Failed": {}})

        super().test_changePathOwner()

    def test_getReplicas(self):
        """Checking get replicas with (dm, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": True,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": False,  # usr1_secret.txt is 700
            "/users/usr2/usr2_file.txt": False,  # usr2_file.txt is 700
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": True,
            "/fake_base.txt": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getReplicas()

    def test_addReplica(self):
        """Adding replicas with (dm, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": False,
            "/users/usr1/sub1/usr1_secret.txt": False,
            "/users/usr2/usr2_file.txt": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": True,
            "/fake_base.txt": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_addReplica()


class TestDataGrpUsr1User(BaseCaseMixin, unittest.TestCase):
    """Should have the permission of the 'usr1' user and the group
    permission of all the vomsRole 'prod' (grp_data, grp_mc)
    """

    def setUp(self):
        super().setUp()
        self.credDict = {"username": "usr1", "group": "grp_data"}

    def test_removeDirectory(self):
        """Removing directory with (usr1, grp_data)"""

        existingDic = {
            "/": False,
            "/realData": False,
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": False,
            "/users/usr1": False,
            # usr1 is the owner of /users/usr1
            "/users/usr1/sub1": True,
            "/users/usr2": False,
            "/users/usr2/sub1": False,
            "/mc": False,
            # Owner group of /mc has vomsProd as well
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_removeDirectory()

    def test_createDirectory(self):
        """Creating directory with (usr1, grp_data)"""

        existingDic = {
            "/": False,
            "/realData": False,
            # owner of /realData is dm
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": False,
            "/users/usr1": False,
            "/users/usr1/sub1": True,
            "/users/usr2": False,
            "/users/usr2/sub1": False,
            "/mc": False,
            # Owner group of /mc has vomsProd as well
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,  # Owner group of /realData is grp_data
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,  # Owner of /users/usr1 is usr1
            "/users/usr2/subUsr2": False,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_createDirectory()

    def test_listDirectory(self):
        """Listing directory with (usr1, grp_data)"""

        existingDic = {
            "/": False,
            "/realData": True,
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": True,
            "/users/usr1": True,
            "/users/usr1/sub1": True,
            "/users/usr2": True,
            "/users/usr2/sub1": True,
            "/mc": True,
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,
            "/users/usr2/subUsr2": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_listDirectory()

    def test_getDirectorySize(self):
        """Getting directory size with (usr1, grp_data)"""

        existingDic = {
            "/": False,
            "/realData": False,
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": False,
            "/users/usr1": True,
            "/users/usr1/sub1": True,
            "/users/usr2": True,
            "/users/usr2/sub1": True,
            "/mc": False,
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,
            "/users/usr2/subUsr2": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getDirectorySize()

    def test_addFile(self):
        """Adding files with (usr1, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": False,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,
            "/users/usr2/usr2_file.txt": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": False,
            "/fake_base.txt": False,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_addFile()

    def test_removeFile(self):
        """Removing files with (usr1, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": False,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,
            "/users/usr2/usr2_file.txt": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_removeFile()

    def test_getFileSize(self):
        """Checking file size with (usr1, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,  # Because other has it...
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,
            "/users/usr2/usr2_file.txt": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": False,
            "/fake_base.txt": False,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getFileSize()

    def test_changePathOwner(self):
        """Setting fiel owner with (usr1, grp_data)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, False), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, False), "Failed": {}})

        super().test_changePathOwner()

    def test_getReplicas(self):
        """Checking get replicas with (usr1, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": True,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,  # usr1_secret.txt is 700
            "/users/usr2/usr2_file.txt": False,  # usr2_file.txt is 700
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": True,
            "/fake_base.txt": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getReplicas()

    def test_addReplica(self):
        """Adding replicas with (usr1, grp_data)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": False,  # Group has 0
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,
            "/users/usr2/usr2_file.txt": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": True,
            "/fake_base.txt": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_addReplica()


class TestUserGrpUsr1User(BaseCaseMixin, unittest.TestCase):
    """Just a normal user, should be able to write only in its own directory"""

    def setUp(self):
        super().setUp()
        self.credDict = {"username": "usr1", "group": "grp_user"}

    def test_removeDirectory(self):
        """Removing directory with (usr1, grp_user)"""

        existingDic = {
            "/": False,
            "/realData": False,
            "/realData/run1": False,
            "/realData/run2": False,
            "/realData/run3": False,
            "/users": False,
            "/users/usr1": False,
            "/users/usr1/sub1": True,
            "/users/usr2": False,
            "/users/usr2/sub1": False,
            "/mc": False,
            "/mc/prod1": False,
            "/mc/prod2": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingDirectories, True), "Failed": {}})

        super().test_removeDirectory()

    def test_createDirectory(self):
        """Creating directory with (usr1, grp_user)"""

        existingDic = {
            "/": False,
            "/realData": False,
            "/realData/run1": False,
            "/realData/run2": False,
            "/realData/run3": False,
            "/users": False,
            "/users/usr1": False,
            "/users/usr1/sub1": True,
            "/users/usr2": False,
            "/users/usr2/sub1": False,
            "/mc": False,
            "/mc/prod1": False,
            "/mc/prod2": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": False,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,
            "/users/usr2/subUsr2": False,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_createDirectory()

    def test_listDirectory(self):
        """Listing directory with (usr1, grp_user)"""

        existingDic = {
            "/": False,
            "/realData": True,
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": True,
            "/users/usr1": True,
            "/users/usr1/sub1": True,
            "/users/usr2": True,
            "/users/usr2/sub1": True,
            "/mc": True,
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,
            "/users/usr2/subUsr2": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_listDirectory()

    def test_getDirectorySize(self):
        """Getting directory size with (usr1, grp_data)"""

        existingDic = {
            "/": False,
            "/realData": False,
            "/realData/run1": True,
            "/realData/run2": True,
            "/realData/run3": True,
            "/users": False,
            "/users/usr1": True,
            "/users/usr1/sub1": True,
            "/users/usr2": True,
            "/users/usr2/sub1": True,
            "/mc": False,
            "/mc/prod1": True,
            "/mc/prod2": True,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun": True,
            "/fakeBaseDir": False,
            "/users/usr1/subUsr1": True,
            "/users/usr2/subUsr2": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getDirectorySize()

    def test_addFile(self):
        """Adding files with (usr1, grp_user)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": False,
            "/realData/run2/run2_data.txt": False,
            "/realData/run3/run3_data.txt": False,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,
            "/users/usr2/usr2_file.txt": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": False,
            "/fakeBaseDir/fake_file.txt": False,
            "/fake_base.txt": False,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_addFile()

    def test_removeFile(self):
        """Removing files with (usr1, grp_user)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": False,
            "/realData/run2/run2_data.txt": False,
            "/realData/run3/run3_data.txt": False,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,
            "/users/usr2/usr2_file.txt": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, True), "Failed": {}})

        super().test_removeFile()

    def test_getFileSize(self):
        """Checking file size with (usr1, grp_user)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,
            "/users/usr2/usr2_file.txt": True,  # usr2 is readable by the group
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": False,
            "/fake_base.txt": False,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getFileSize()

    def test_changePathOwner(self):
        """Setting fiel owner with (usr1, grp_user)"""

        self.expectedExistingRet = S_OK({"Successful": dict.fromkeys(fileTree, False), "Failed": {}})

        self.expectedNonExistingRet = S_OK({"Successful": dict.fromkeys(nonExistingFiles, False), "Failed": {}})

        super().test_changePathOwner()

    def test_getReplicas(self):
        """Checking get replicas with (usr1, grp_user)"""
        existingDic = {
            "/atTheRoot.txt": True,
            "/realData/run1/run1_data.txt": True,
            "/realData/run2/run2_data.txt": True,
            "/realData/run3/run3_data.txt": True,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,  # usr1_secret.txt is 700
            "/users/usr2/usr2_file.txt": False,  # usr2_file.txt is 700
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": True,
            "/fake_base.txt": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_getReplicas()

    def test_addReplica(self):
        """Adding replicas with (usr1, grp_user)"""
        existingDic = {
            "/atTheRoot.txt": False,
            "/realData/run1/run1_data.txt": False,
            "/realData/run2/run2_data.txt": False,
            "/realData/run3/run3_data.txt": False,
            "/users/usr1/usr1_file.txt": True,
            "/users/usr1/sub1/usr1_secret.txt": True,
            "/users/usr2/usr2_file.txt": False,
        }

        self.expectedExistingRet = S_OK({"Successful": existingDic, "Failed": {}})

        nonExistingDic = {
            "/realData/futurRun/futur_data.txt": True,
            "/fakeBaseDir/fake_file.txt": True,
            "/fake_base.txt": True,
        }

        self.expectedNonExistingRet = S_OK({"Successful": nonExistingDic, "Failed": {}})

        super().test_addReplica()


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestNonExistingUser)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestAdminGrpAnonUser))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestAdminGrpAdminUser))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestDataGrpDmUser))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestDataGrpUsr1User))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestUserGrpUsr1User))

    unittest.TextTestRunner(verbosity=2).run(suite)
