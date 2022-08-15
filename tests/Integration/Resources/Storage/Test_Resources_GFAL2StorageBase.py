"""
This integration tests will perform basic operations on a storage element, depending on which protocols are available.
It creates a local hierarchy, and then tries to upload, download, remove, get metadata etc

Potential problems:
* it might seem a good idea to simply add tests for the old srm in it. It is not :-)
  There is a deadlock between gfal and gfal2 libraries, you can't load both of them together
* if running in debug mode, you will hit a deadlock with gsiftp :-)  https://its.cern.ch/jira/browse/DMC-922
* On some storage (like EOS), there is a caching of metadata. So a file just created, even if present,
  might return no metadata information. Sleep times might be needed when this happens.

Examples:
<python Test_Resources_GFAL2StorageBase.py CERN-GFAL2>: will test all the gfal2 plugins defined for CERN-GFAL2
<python Test_Resources_GFAL2StorageBase.py CERN-GFAL2 GFAL2_XROOT>: will test the GFAL2_XROOT plugins defined for CERN-GFAL2


"""

# pylint: disable=invalid-name,wrong-import-position
import unittest
import sys
import os
import tempfile
import shutil


from DIRAC.Core.Base.Script import Script


Script.setUsageMessage(
    """
Test a full DMS workflow against a StorageElement
\t%s <SE name> <PluginLists>
\t<SE name>: mandatory
\t<plugins>: comma separated list of plugin to test (defautl all)
"""
    % Script.scriptName
)


Script.parseCommandLine()

# [SEName, <plugins>]
posArgs = Script.getPositionalArgs()

if not posArgs:
    Script.showHelp(exitCode=1)


from DIRAC import gLogger
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.Core.Utilities.File import getSize
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup

#### GLOBAL VARIABLES: ################

# Name of the storage element that has to be tested
gLogger.setLevel("DEBUG")

STORAGE_NAME = posArgs[0]
# Size in bytes of the file we want to produce
FILE_SIZE = 5 * 1024  # 5kB
# base path on the storage where the test files/folders will be created
DESTINATION_PATH = ""
# plugins that will be used
AVAILABLE_PLUGINS = []

if len(posArgs) > 1:
    AVAILABLE_PLUGINS = posArgs[1].split(",")
else:
    res = StorageElement(STORAGE_NAME).getProtocolSections()
    if not res["OK"]:
        gLogger.error("Failed fetching available plugins", res["Message"])
        sys.exit(2)
    AVAILABLE_PLUGINS = res["Value"]


try:
    res = getProxyInfo()
    if not res["OK"]:
        gLogger.error("Failed to get client proxy information.", res["Message"])
        sys.exit(2)
    proxyInfo = res["Value"]
    username = proxyInfo["username"]
    vo = ""
    if "group" in proxyInfo:
        vo = getVOForGroup(proxyInfo["group"])

    DESTINATION_PATH = "/%s/user/%s/%s/gfaltests" % (vo, username[0], username)

except Exception as e:  # pylint: disable=broad-except
    print(repr(e))
    sys.exit(2)


# local path containing test files. There should be a folder called Workflow containing (the files can be simple textfiles)
# FolderA
# -FolderAA
# --FileAA
# -FileA
# FolderB
# -FileB
# File1
# File2
# File3


def _mul(txt):
    """Multiply the input text enough time so that we
    reach the expected file size
    """
    return txt * (max(1, int(FILE_SIZE / len(txt))))


class basicTest(unittest.TestCase):
    """This performs all the test, and is just called for a specific plugin"""

    def setUp(self, pluginToTest):
        """Put in place the local directory structure"""
        # gLogger.setLevel( 'DEBUG' )
        self.LOCAL_PATH = tempfile.mkdtemp()

        self.storageName = STORAGE_NAME

        # create the local structure
        workPath = os.path.join(self.LOCAL_PATH, "Workflow")
        os.mkdir(workPath)

        os.mkdir(os.path.join(workPath, "FolderA"))
        with open(os.path.join(workPath, "FolderA", "FileA"), "w") as f:
            f.write(_mul("FileA"))

        os.mkdir(os.path.join(workPath, "FolderA", "FolderAA"))
        with open(os.path.join(workPath, "FolderA", "FolderAA", "FileAA"), "w") as f:
            f.write(_mul("FileAA"))

        os.mkdir(os.path.join(workPath, "FolderB"))
        with open(os.path.join(workPath, "FolderB", "FileB"), "w") as f:
            f.write(_mul("FileB"))

        for fn in ["File1", "File2", "File3"]:
            with open(os.path.join(workPath, fn), "w") as f:
                f.write(_mul(fn))

        # When testing for a given plugin, this plugin might not be able to
        # write or read. In this case, we use this specific plugins
        # ONLY for the operations it is allowed to
        specSE = StorageElement(self.storageName, protocolSections=pluginToTest)
        genericSE = StorageElement(self.storageName)

        pluginProtocol = specSE.protocolOptions[0]["Protocol"]

        if pluginProtocol in specSE.localAccessProtocolList:
            print("Using specific SE with %s only for reading" % pluginToTest)
            self.readSE = specSE
        else:
            print("Plugin %s is not available for read. Use a generic SE" % pluginToTest)
            self.readSE = genericSE

        if pluginProtocol in specSE.localWriteProtocolList:
            print("Using specific SE with %s only for writing" % pluginToTest)
            self.writeSE = specSE
        else:
            print("Plugin %s is not available for write. Use a generic SE" % pluginToTest)
            self.writeSE = genericSE

        # Make sure we are testing the specific plugin at least for one
        self.assertTrue(self.readSE == specSE or self.writeSE == specSE, "Using only generic SE does not make sense!!")

        basicTest.clearDirectory(self)

    def tearDown(self):
        """Remove the local tree and the remote files"""
        shutil.rmtree(self.LOCAL_PATH)
        self.clearDirectory()

    def clearDirectory(self):
        """Removing target directory"""
        print("==================================================")
        print("==== Removing the older Directory ================")
        workflow_folder = DESTINATION_PATH + "/Workflow"
        res = self.writeSE.removeDirectory(workflow_folder)
        if not res["OK"]:
            print("basicTest.clearDirectory: Workflow folder maybe not empty")
        print("==================================================")

    def testWorkflow(self):
        """This perform a complete workflow puting, removing, stating files and directories"""

        putDir = {
            os.path.join(DESTINATION_PATH, "Workflow/FolderA"): os.path.join(self.LOCAL_PATH, "Workflow/FolderA"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderB"): os.path.join(self.LOCAL_PATH, "Workflow/FolderB"),
        }

        createDir = [
            os.path.join(DESTINATION_PATH, "Workflow/FolderA/FolderAA"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderA/FolderABA"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderA/FolderAAB"),
        ]

        putFile = {
            os.path.join(DESTINATION_PATH, "Workflow/FolderA/File1"): os.path.join(self.LOCAL_PATH, "Workflow/File1"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderAA/File1"): os.path.join(self.LOCAL_PATH, "Workflow/File1"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderBB/File2"): os.path.join(self.LOCAL_PATH, "Workflow/File2"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderB/File2"): os.path.join(self.LOCAL_PATH, "Workflow/File2"),
            os.path.join(DESTINATION_PATH, "Workflow/File3"): os.path.join(self.LOCAL_PATH, "Workflow/File3"),
        }

        isFile = {
            os.path.join(DESTINATION_PATH, "Workflow/FolderA/File1"): os.path.join(self.LOCAL_PATH, "Workflow/File1"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderB/FileB"): os.path.join(
                self.LOCAL_PATH, "Workflow/FolderB/FileB"
            ),
        }

        listDir = [
            os.path.join(DESTINATION_PATH, "Workflow"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderA"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderB"),
        ]

        getDir = [
            os.path.join(DESTINATION_PATH, "Workflow/FolderA"),
            os.path.join(DESTINATION_PATH, "Workflow/FolderB"),
        ]

        removeFile = [os.path.join(DESTINATION_PATH, "Workflow/FolderA/File1")]
        rmdir = [os.path.join(DESTINATION_PATH, "Workflow")]

        ##### Computing local adler and size #####

        fileAdlers = {}
        fileSizes = {}

        for lfn, localFn in isFile.items():
            fileAdlers[lfn] = fileAdler(localFn)
            fileSizes[lfn] = getSize(localFn)

        ########## uploading directory #############
        res = self.writeSE.putDirectory(putDir)
        self.assertEqual(res["OK"], True)
        # time.sleep(5)
        res = self.readSE.listDirectory(listDir)
        self.assertEqual(
            any(
                os.path.join(DESTINATION_PATH, "Workflow/FolderA/FileA") in dictKey
                for dictKey in res["Value"]["Successful"][os.path.join(DESTINATION_PATH, "Workflow/FolderA")][
                    "Files"
                ].keys()
            ),
            True,
        )
        self.assertEqual(
            any(
                os.path.join(DESTINATION_PATH, "Workflow/FolderB/FileB") in dictKey
                for dictKey in res["Value"]["Successful"][os.path.join(DESTINATION_PATH, "Workflow/FolderB")][
                    "Files"
                ].keys()
            ),
            True,
        )

        ########## createDir #############
        res = self.writeSE.createDirectory(createDir)
        self.assertEqual(res["OK"], True)
        res = res["Value"]
        self.assertEqual(res["Successful"][createDir[0]], True)
        self.assertEqual(res["Successful"][createDir[1]], True)
        self.assertEqual(res["Successful"][createDir[2]], True)

        ######## putFile ########
        res = self.writeSE.putFile(putFile)
        self.assertEqual(res["OK"], True)
        # time.sleep(5)
        res = self.readSE.isFile(isFile)
        self.assertEqual(res["OK"], True)
        self.assertTrue(all([x for x in res["Value"]["Successful"].values()]))
        # self.assertEqual( res['Value']['Successful'][isFile[0]], True )
        # self.assertEqual( res['Value']['Successful'][isFile[1]], True )

        ######## getMetadata ###########
        res = self.readSE.getFileMetadata(isFile)
        self.assertEqual(res["OK"], True)
        res = res["Value"]["Successful"]
        self.assertEqual(any(path in resKey for path in isFile for resKey in res.keys()), True)

        # Checking that the checksums and sizes are correct
        for lfn in isFile:
            self.assertEqual(res[lfn]["Checksum"], fileAdlers[lfn])
            self.assertEqual(res[lfn]["Size"], fileSizes[lfn])

        ####### getDirectory ######
        res = self.readSE.getDirectory(getDir, os.path.join(self.LOCAL_PATH, "getDir"))
        self.assertEqual(res["OK"], True)
        res = res["Value"]
        self.assertEqual(any(getDir[0] in dictKey for dictKey in res["Successful"]), True)
        self.assertEqual(any(getDir[1] in dictKey for dictKey in res["Successful"]), True)

        ###### removeFile ##########
        res = self.writeSE.removeFile(removeFile)
        self.assertEqual(res["OK"], True)
        res = self.readSE.exists(removeFile)
        self.assertEqual(res["OK"], True)
        self.assertEqual(res["Value"]["Successful"][removeFile[0]], False)

        ###### remove non existing file #####
        res = self.writeSE.removeFile(removeFile)
        self.assertEqual(res["OK"], True)
        res = self.readSE.exists(removeFile)
        self.assertEqual(res["OK"], True)
        self.assertEqual(res["Value"]["Successful"][removeFile[0]], False)

        ########### removing directory  ###########
        res = self.writeSE.removeDirectory(rmdir, True)

        res = self.readSE.exists(rmdir)
        self.assertEqual(res["OK"], True)
        self.assertEqual(res["Value"]["Successful"][rmdir[0]], False)


@unittest.skipIf(
    "GFAL2_SRM2" not in AVAILABLE_PLUGINS, "StorageElement %s does not have plugin GFAL2_SRM2 defined" % STORAGE_NAME
)
class GFAL2_SRM2_Test(basicTest):
    """Test using the GFAL2_SRM2 plugin"""

    def setUp(self):
        basicTest.setUp(self, "GFAL2_SRM2")


@unittest.skipIf(
    "GFAL2_HTTPS" not in AVAILABLE_PLUGINS, "StorageElement %s does not have plugin GFAL2_HTTPS defined" % STORAGE_NAME
)
class GFAL2_HTTPS_Test(basicTest):
    """Test using the GFAL2_HTTPS plugin"""

    def setUp(self):
        basicTest.setUp(self, "GFAL2_HTTPS")


@unittest.skipIf(
    "GFAL2_XROOT" not in AVAILABLE_PLUGINS, "StorageElement %s does not have plugin GFAL2_XROOT defined" % STORAGE_NAME
)
class GFAL2_XROOT_Test(basicTest):
    """Test using the GFAL2_XROOT plugin"""

    def setUp(self):
        basicTest.setUp(self, "GFAL2_XROOT")


@unittest.skipIf(
    "XROOT" not in AVAILABLE_PLUGINS, "StorageElement %s does not have plugin XROOT defined" % STORAGE_NAME
)
class XROOT_Test(basicTest):
    """Test using the XROOT plugin"""

    def setUp(self):
        basicTest.setUp(self, "XROOT")


@unittest.skipIf(
    "GFAL2_GSIFTP" not in AVAILABLE_PLUGINS,
    "StorageElement %s does not have plugin GFAL2_GSIFTP defined" % STORAGE_NAME,
)
class GFAL2_GSIFTP_Test(basicTest):
    """Test using the GFAL2_GSIFTP plugin"""

    def setUp(self):
        basicTest.setUp(self, "GFAL2_GSIFTP")


@unittest.skipIf("SRM2" not in AVAILABLE_PLUGINS, "StorageElement %s does not have plugin SRM2 defined" % STORAGE_NAME)
class SRM2_Test(basicTest):
    def setUp(self):
        basicTest.setUp(self, "SRM2")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(GFAL2_SRM2_Test)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GFAL2_XROOT_Test))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GFAL2_HTTPS_Test))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GFAL2_GSIFTP_Test))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(XROOT_Test))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not testResult.wasSuccessful())
