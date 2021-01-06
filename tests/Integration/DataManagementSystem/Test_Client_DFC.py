""" This is a test of the chain
    FileCatalogClient -> FileCatalogHandler -> FileCatalogDB

    It supposes that the DB is present, and that the service is running
"""

# pylint: disable=invalid-name,wrong-import-position
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import csv
import filecmp

import os
import unittest
import tempfile
import sys


from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Security.Properties import FC_MANAGEMENT
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

seName = "mySE"
testUser = 'atsareg'
testGroup = 'dirac_user'
testDir = '/vo.formation.idgrilles.fr/user/a/atsareg/testdir'
parentDir = '/vo.formation.idgrilles.fr/user/a/atsareg'
parentparentDir = '/vo.formation.idgrilles.fr/user/a'
nonExistingDir = "/I/Dont/exist/dir"
testFile = '/vo.formation.idgrilles.fr/user/a/atsareg/testdir/testfile'
nonExistingFile = "/I/Dont/exist"

isAdmin = False
proxyUser = 'anon'
proxyGroup = 'anon'


class DFCTestCase(unittest.TestCase):

  def setUp(self):
    #     gLogger.setLevel( "DEBUG" )

    self.dfc = FileCatalogClient("DataManagement/FileCatalog")


class UserGroupCase(DFCTestCase):

  def test_userOperations(self):
    """Testing the user related operations
       If you are an admin, you should be allowed to, if not, it should fail

       CAUTION : THEY ARE DESIGNED FOR THE SecurityManager VOMSPolicy

    """

    expectedRes = None
    if isAdmin:
      print("Running UserTest in admin mode")
      expectedRes = True
    else:
      print("Running UserTest in non admin mode")
      expectedRes = False

    # Add the user
    result = self.dfc.addUser(testUser)
    self.assertEqual(result['OK'], expectedRes, "AddUser failed when adding new user: %s" % result)
    # Add an existing user
    result = self.dfc.addUser(testUser)
    self.assertEqual(
        result['OK'],
        expectedRes,
        "AddUser failed when adding existing user: %s" %
        result)
    # Fetch the list of user
    result = self.dfc.getUsers()
    self.assertEqual(result['OK'], expectedRes, "getUsers failed: %s" % result)

    if isAdmin:
      # Check if our user is present
      self.assertTrue(testUser in result['Value'], "getUsers failed: %s" % result)

    # remove the user we created
    result = self.dfc.deleteUser(testUser)
    self.assertEqual(result['OK'], expectedRes, "deleteUser failed: %s" % result)

  def test_groupOperations(self):
    """Testing the group related operations
           If you are an admin, you should be allowed to, if not, it should fail

        CAUTION : THEY ARE DESIGNED FOR THE SecurityManager DirectorySecurityManagerWithDelete or VOMSPolicy
    """

    expectedRes = None
    if isAdmin:
      print("Running UserTest in admin mode")
      expectedRes = True
    else:
      print("Running UserTest in non admin mode")
      expectedRes = False

    # Create new group
    result = self.dfc.addGroup(testGroup)
    self.assertEqual(result['OK'], expectedRes, "AddGroup failed when adding new user: %s" % result)

    result = self.dfc.addGroup(testGroup)
    self.assertEqual(
        result['OK'],
        expectedRes,
        "AddGroup failed when adding existing user: %s" %
        result)

    result = self.dfc.getGroups()
    self.assertEqual(result['OK'], expectedRes, "getGroups failed: %s" % result)

    if isAdmin:
      self.assertTrue(testGroup in result['Value'])

    result = self.dfc.deleteGroup(testGroup)
    self.assertEqual(result['OK'], expectedRes, "deleteGroup failed: %s" % result)


class FileCase(DFCTestCase):

  def test_fileOperations(self):
    """
      Tests the File related Operations
      CAUTION : THEY ARE DESIGNED FOR THE SecurityManager DirectorySecurityManagerWithDelete or VOMSPolicy

    """
    if isAdmin:
      print("Running UserTest in admin mode")
    else:
      print("Running UserTest in non admin mode")

    # Adding a new file
    result = self.dfc.addFile({testFile: {'PFN': 'testfilePFN',
                                          'SE': 'testSE',
                                          'Size': 123,
                                          'GUID': '1000',
                                          'Checksum': '0'}})
    self.assertTrue(result['OK'], "addFile failed when adding new file %s" % result)
    self.assertTrue(testFile in result['Value'].get('Successful', {}), result)

    result = self.dfc.exists(testFile)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'].get('Successful', {}).get(testFile),
                     testFile, "exists( testFile) should be the same lfn %s" % result)

    result = self.dfc.exists({testFile: '1000'})
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'].get('Successful', {}).get(testFile),
                     testFile, "exists( testFile : 1000) should be the same lfn %s" % result)

    result = self.dfc.exists({testFile: {'GUID': '1000', 'PFN': 'blabla'}})
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'].get('Successful', {}).get(testFile),
                     testFile, "exists( testFile : 1000) should be the same lfn %s" % result)

    # In fact, we don't check if the GUID is correct...
    result = self.dfc.exists({testFile: '1001'})
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'].get('Successful', {}).get(testFile),
                     testFile, "exists( testFile : 1001) should be the same lfn %s" % result)

    result = self.dfc.exists({testFile + '2': '1000'})
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'].get('Successful', {}).get(testFile + '2'),
                     testFile, "exists( testFile2 : 1000) should return testFile %s" % result)

    # Re-adding the same file
    result = self.dfc.addFile({testFile: {'PFN': 'testfilePFN',
                                          'SE': 'testSE',
                                          'Size': 123,
                                          'GUID': '1000',
                                          'Checksum': '0'}})
    self.assertTrue(result["OK"], "addFile failed when adding existing file %s" % result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "addFile failed: it should  be possible to add an existing lfn with the same attributes %s" %
        result)

  # Re-adding the same file
    result = self.dfc.addFile({testFile: {'PFN': 'testfilePFN',
                                          'SE': 'testSE',
                                          'Size': 123,
                                          'GUID': '1000',
                                          'Checksum': '1'}})
    self.assertTrue(result["OK"], "addFile failed when adding existing file %s" % result)
    self.assertTrue(
        testFile in result["Value"]["Failed"],
        "addFile failed: it should not be possible to add an existing lfn with the different attributes %s" %
        result)

    # Re-adding the different LFN but same GUID
    result = self.dfc.addFile({testFile + '2': {'PFN': 'testfilePFN',
                                                'SE': 'testSE',
                                                'Size': 123,
                                                'GUID': '1000',
                                                'Checksum': '0'}})
    self.assertTrue(
        result["OK"],
        "addFile failed when adding non existing file with existing GUID %s" %
        result)
    self.assertTrue(
        testFile +
        '2' in result["Value"]["Failed"],
        "addFile failed: it should not be possible to add an existing GUID %s" %
        result)

    ##################################################################################
    # Setting existing status of existing file
    result = self.dfc.setFileStatus({testFile: "AprioriGood"})
    self.assertTrue(
        result["OK"],
        "setFileStatus failed when setting existing status of existing file %s" %
        result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "setFileStatus failed: %s should be in successful (%s)" %
        (testFile,
         result))

    # Setting unexisting status of existing file
    result = self.dfc.setFileStatus({testFile: "Happy"})
    self.assertTrue(
        result["OK"],
        "setFileStatus failed when setting un-existing status of existing file %s" %
        result)
    self.assertTrue(
        testFile in result["Value"]["Failed"],
        "setFileStatus should have failed %s" %
        result)

    # Setting existing status of unexisting file
    result = self.dfc.setFileStatus({nonExistingFile: "Trash"})
    self.assertTrue(
        result["OK"],
        "setFileStatus failed when setting existing status of non-existing file %s" %
        result)
    self.assertTrue(
        nonExistingFile in result["Value"]["Failed"],
        "setFileStatus failed: %s should be in failed (%s)" %
        (nonExistingFile,
         result))

    ##################################################################################

    result = self.dfc.isFile([testFile, nonExistingFile])
    self.assertTrue(result["OK"], "isFile failed: %s" % result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "isFile : %s should be in Successful %s" %
        (testFile,
         result))
    self.assertTrue(
        result["Value"]["Successful"][testFile],
        "isFile : %s should be seen as a file %s" %
        (testFile,
         result))
    self.assertTrue(
        nonExistingFile in result["Value"]["Successful"],
        "isFile : %s should be in Successful %s" %
        (nonExistingFile,
         result))
    self.assertTrue(result["Value"]["Successful"][nonExistingFile] is False,
                    "isFile : %s should be seen as a file %s" % (nonExistingFile, result))

    result = self.dfc.changePathOwner({testFile: "toto", nonExistingFile: "tata"})
    self.assertTrue(result["OK"], "changePathOwner failed: %s" % result)

    # Only admin can change path owner
    if isAdmin:
      self.assertTrue(
          testFile in result["Value"]["Successful"],
          "changePathOwner : %s should be in Successful %s" %
          (testFile,
           result))
    else:
      self.assertTrue(
          testFile in result["Value"]["Failed"],
          "changePathOwner : %s should be in Failed %s" %
          (testFile,
           result))

    self.assertTrue(
        nonExistingFile in result["Value"]["Failed"],
        "changePathOwner : %s should be in Failed %s" %
        (nonExistingFile,
         result))

    # Only admin can change path group
    result = self.dfc.changePathGroup({testFile: "toto", nonExistingFile: "tata"})
    self.assertTrue(result["OK"], "changePathGroup failed: %s" % result)
    if isAdmin:
      self.assertTrue(
          testFile in result["Value"]["Successful"],
          "changePathGroup : %s should be in Successful %s" %
          (testFile,
           result))
    else:
      self.assertTrue(
          testFile in result["Value"]["Failed"],
          "changePathGroup : %s should be in Failed %s" %
          (testFile,
           result))
    self.assertTrue(
        nonExistingFile in result["Value"]["Failed"],
        "changePathGroup : %s should be in Failed %s" %
        (nonExistingFile,
         result))

    result = self.dfc.changePathMode({testFile: 0o44, nonExistingFile: 0o44})
    self.assertTrue(result["OK"], "changePathMode failed: %s" % result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "changePathMode : %s should be in Successful %s" %
        (testFile,
         result))
    self.assertTrue(
        nonExistingFile in result["Value"]["Failed"],
        "changePathMode : %s should be in Failed %s" %
        (nonExistingFile,
         result))

    result = self.dfc.getFileSize([testFile, nonExistingFile])
    self.assertTrue(result["OK"], "getFileSize failed: %s" % result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "getFileSize : %s should be in Successful %s" %
        (testFile,
         result))
    self.assertEqual(
        result["Value"]["Successful"][testFile],
        123,
        "getFileSize got incorrect file size %s" %
        result)
    self.assertTrue(
        nonExistingFile in result["Value"]["Failed"],
        "getFileSize : %s should be in Failed %s" %
        (nonExistingFile,
         result))

    result = self.dfc.getFileMetadata([testFile, nonExistingFile])
    self.assertTrue(result["OK"], "getFileMetadata failed: %s" % result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "getFileMetadata : %s should be in Successful %s" %
        (testFile,
         result))

    # The owner changed only if we are admin
    if isAdmin:
      self.assertEqual(
          result["Value"]["Successful"][testFile]["Owner"],
          "toto",
          "getFileMetadata got incorrect Owner %s" %
          result)

    self.assertEqual(
        result["Value"]["Successful"][testFile]["Status"],
        "AprioriGood",
        "getFileMetadata got incorrect status %s" %
        result)
    self.assertTrue(
        nonExistingFile in result["Value"]["Failed"],
        "getFileMetadata : %s should be in Failed %s" %
        (nonExistingFile,
         result))

    # Here we write the output to a file
    # So we dump the expected content in a file
    _, expectedDumpFn = tempfile.mkstemp()

    with open(expectedDumpFn, 'w') as expectedDumpFd:
      csvWriter = csv.writer(expectedDumpFd, delimiter='|')
      csvWriter.writerow([testFile, '0', 123])

    actualDumpFn = expectedDumpFn + 'real'
    result = self.dfc.getSEDump('testSE', actualDumpFn)
    self.assertTrue(result['OK'], "Error when getting SE dump %s" % result)
    self.assertTrue(filecmp.cmp(expectedDumpFn, actualDumpFn), "Did not get the expected SE Dump")
    os.remove(expectedDumpFn)
    os.remove(actualDumpFn)

    result = self.dfc.removeFile([testFile, nonExistingFile])
    self.assertTrue(result["OK"], "removeFile failed: %s" % result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "removeFile : %s should be in Successful %s" %
        (testFile,
         result))
    self.assertTrue(
        result["Value"]["Successful"][testFile],
        "removeFile : %s should be in True %s" %
        (testFile,
         result))
    self.assertTrue(
        result["Value"]["Successful"][nonExistingFile],
        "removeFile : %s should be in True %s" %
        (nonExistingFile,
         result))


class ReplicaCase(DFCTestCase):

  def test_replicaOperations(self):
    """
      this test requires the SE to be properly defined in the CS -> NO IT DOES NOT!!
    """
    # Adding a new file
    result = self.dfc.addFile({testFile: {'PFN': 'testfile',
                                          'SE': 'testSE',
                                          'Size': 123,
                                          'GUID': '1000',
                                          'Checksum': '0'}})
    self.assertTrue(result['OK'], "addFile failed when adding new file %s" % result)

    # Adding new replica
    result = self.dfc.addReplica({testFile: {"PFN": "testFilePFN", "SE": "otherSE"}})
    self.assertTrue(result['OK'], "addReplica failed when adding new Replica %s" % result)
    self.assertTrue(
        testFile in result['Value']["Successful"],
        "addReplica failed when adding new Replica %s" %
        result)

    # Adding the same replica
    result = self.dfc.addReplica({testFile: {"PFN": "testFilePFN", "SE": "otherSE"}})
    self.assertTrue(result['OK'], "addReplica failed when adding new Replica %s" % result)
    self.assertTrue(
        testFile in result['Value']["Successful"],
        "addReplica failed when adding new Replica %s" %
        result)

    # Adding replica of a non existing file
    result = self.dfc.addReplica({nonExistingFile: {"PFN": "IdontexistPFN", "SE": "otherSE"}})
    self.assertTrue(
        result['OK'],
        "addReplica failed when adding Replica to non existing Replica %s" %
        result)
    self.assertTrue(
        nonExistingFile in result['Value']["Failed"],
        "addReplica for non existing file should go in Failed  %s" %
        result)

    # Setting existing status of existing Replica
    result = self.dfc.setReplicaStatus({testFile: {"Status": "Trash", "SE": "otherSE"}})
    self.assertTrue(
        result["OK"],
        "setReplicaStatus failed when setting existing status of existing Replica %s" %
        result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "setReplicaStatus failed: %s should be in successful (%s)" %
        (testFile,
         result))

    # Setting non existing status of existing Replica
    result = self.dfc.setReplicaStatus({testFile: {"Status": "randomStatus", "SE": "otherSE"}})
    self.assertTrue(
        result["OK"],
        "setReplicaStatus failed when setting non-existing status of existing Replica %s" %
        result)
    self.assertTrue(
        testFile in result["Value"]["Failed"],
        "setReplicaStatus failed: %s should be in Failed (%s)" %
        (testFile,
         result))

    # Setting existing status of non-existing Replica
    result = self.dfc.setReplicaStatus({testFile: {"Status": "Trash", "SE": "nonExistingSe"}})
    self.assertTrue(
        result["OK"],
        "setReplicaStatus failed when setting existing status of non-existing Replica %s" %
        result)
    self.assertTrue(
        testFile in result["Value"]["Failed"],
        "setReplicaStatus failed: %s should be in Failed (%s)" %
        (testFile,
         result))

    # Setting existing status of non-existing File
    result = self.dfc.setReplicaStatus(
        {nonExistingFile: {"Status": "Trash", "SE": "nonExistingSe"}})
    self.assertTrue(
        result["OK"],
        "setReplicaStatus failed when setting existing status of non-existing File %s" %
        result)
    self.assertTrue(
        nonExistingFile in result["Value"]["Failed"],
        "setReplicaStatus failed: %s should be in Failed (%s)" %
        (nonExistingFile,
         result))

    # Getting existing status of existing Replica but not visible
    result = self.dfc.getReplicaStatus({testFile: "testSE"})
    self.assertTrue(
        result["OK"],
        "getReplicaStatus failed when getting existing status of existing Replica %s" %
        result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "getReplicaStatus failed: %s should be in Successful (%s)" %
        (testFile,
         result))

    # Getting existing status of existing Replica but not visible
    result = self.dfc.getReplicaStatus({testFile: "otherSE"})
    self.assertTrue(
        result["OK"],
        "getReplicaStatus failed when getting existing status of existing Replica but not visible %s" %
        result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "getReplicaStatus failed: %s should be in Successful (%s)" %
        (testFile,
         result))

    # Getting status of non-existing File but not visible
    result = self.dfc.getReplicaStatus({nonExistingFile: "testSE"})
    self.assertTrue(
        result["OK"],
        "getReplicaStatus failed when getting status of non existing File %s" %
        result)
    self.assertTrue(
        nonExistingFile in result["Value"]["Failed"],
        "getReplicaStatus failed: %s should be in failed (%s)" %
        (nonExistingFile,
         result))

    # Getting replicas of existing File and non existing file, seeing all replicas
    result = self.dfc.getReplicas([testFile, nonExistingFile], allStatus=True)
    self.assertTrue(result["OK"], "getReplicas failed %s" % result)
    self.assertTrue(
        testFile in result["Value"]["Successful"],
        "getReplicas failed, %s should be in Successful %s" %
        (testFile,
         result))
    self.assertEqual(
        result["Value"]["Successful"][testFile], {
            "otherSE": testFile, "testSE": testFile}, "getReplicas failed, %s should be in Successful %s" %
        (testFile, result))
    self.assertTrue(
        nonExistingFile in result["Value"]["Failed"],
        "getReplicas failed, %s should be in Failed %s" %
        (nonExistingFile,
         result))

    expectedSize = {'PhysicalSize': {'TotalSize': 246,
                                     'otherSE': {'Files': 1, 'Size': 123},
                                     'TotalFiles': 2,
                                     'testSE': {'Files': 1, 'Size': 123}},
                    'LogicalFiles': 1, 'LogicalDirectories': 0, 'LogicalSize': 123}

    result = self.dfc.getDirectorySize([testDir], True, False)
    self.assertTrue(result["OK"], "getDirectorySize failed: %s" % result)
    self.assertTrue(
        testDir in result["Value"]["Successful"],
        "getDirectorySize : %s should be in Successful %s" %
        (testDir,
         result))
    self.assertEqual(
        result["Value"]["Successful"][testDir],
        expectedSize,
        "getDirectorySize got incorrect directory size %s" %
        result)

    result = self.dfc.getDirectorySize([testDir], True, True)

    self.assertTrue(result["OK"], "getDirectorySize (calc) failed: %s" % result)
    self.assertTrue(
        testDir in result["Value"]["Successful"],
        "getDirectorySize (calc): %s should be in Successful %s" %
        (testDir,
         result))
    self.assertEqual(
        result["Value"]["Successful"][testDir],
        expectedSize,
        "getDirectorySize got incorrect directory size %s" %
        result)

    # removing master replica
    result = self.dfc.removeReplica({testFile: {"SE": "testSE"}})
    self.assertTrue(result['OK'], "removeReplica failed when removing master Replica %s" % result)
    self.assertTrue(
        testFile in result['Value']["Successful"],
        "removeReplica failed when removing master Replica %s" %
        result)

    # removing non existing replica of existing File
    result = self.dfc.removeReplica({testFile: {"SE": "nonExistingSe2"}})
    self.assertTrue(
        result['OK'],
        "removeReplica failed when removing non existing Replica %s" %
        result)
    self.assertTrue(
        testFile in result['Value']["Successful"],
        "removeReplica failed when removing new Replica %s" %
        result)

    # removing non existing replica of non existing file
    result = self.dfc.removeReplica({nonExistingFile: {"SE": "nonExistingSe3"}})
    self.assertTrue(
        result['OK'],
        "removeReplica failed when removing replica of non existing File %s" %
        result)
    self.assertTrue(
        nonExistingFile in result['Value']["Successful"],
        "removeReplica of non existing file, %s should be in Successful %s" %
        (nonExistingFile,
         result))

    # removing last replica
    result = self.dfc.removeReplica({testFile: {"SE": "otherSE"}})
    self.assertTrue(result['OK'], "removeReplica failed when removing last Replica %s" % result)
    self.assertTrue(
        testFile in result['Value']["Successful"],
        "removeReplica failed when removing last Replica %s" %
        result)

    # Cleaning after us
    result = self.dfc.removeFile(testFile)
    self.assertTrue(result["OK"], "removeFile failed: %s" % result)


class DirectoryCase(DFCTestCase):

  def test_directoryOperations(self):
    """
      Tests the Directory related Operations
      this test requires the SE to be properly defined in the CS -> NO IT DOES NOT!!
    """
    # Adding a new directory
    result = self.dfc.createDirectory(testDir)
    self.assertTrue(result['OK'], "addDirectory failed when adding new directory %s" % result)

    result = self.dfc.addFile({testFile: {'PFN': 'testfile',
                                          'SE': 'testSE',
                                          'Size': 123,
                                          'GUID': '1000',
                                          'Checksum': '0'}})
    self.assertTrue(result['OK'], "addFile failed when adding new file %s" % result)

    # Re-adding the same directory (CAUTION, different from addFile)
    result = self.dfc.createDirectory(testDir)
    self.assertTrue(result["OK"], "addDirectory failed when adding existing directory %s" % result)
    self.assertTrue(
        testDir in result["Value"]["Successful"],
        "addDirectory failed: it should be possible to add an existing lfn %s" %
        result)

    result = self.dfc.isDirectory([testDir, nonExistingDir])
    self.assertTrue(result["OK"], "isDirectory failed: %s" % result)
    self.assertTrue(
        testDir in result["Value"]["Successful"],
        "isDirectory : %s should be in Successful %s" %
        (testDir,
         result))
    self.assertTrue(
        result["Value"]["Successful"][testDir],
        "isDirectory : %s should be seen as a directory %s" %
        (testDir,
         result))
    self.assertTrue(
        nonExistingDir in result["Value"]["Successful"],
        "isDirectory : %s should be in Successful %s" %
        (nonExistingDir,
         result))
    self.assertTrue(
        result["Value"]["Successful"][nonExistingDir] is False,
        "isDirectory : %s should be seen as a directory %s" %
        (nonExistingDir,
         result))

    result = self.dfc.getDirectorySize([testDir, nonExistingDir], False, False)
    self.assertTrue(result["OK"], "getDirectorySize failed: %s" % result)
    self.assertTrue(
        testDir in result["Value"]["Successful"],
        "getDirectorySize : %s should be in Successful %s" %
        (testDir,
         result))
    self.assertEqual(result["Value"]["Successful"][testDir],
                     {'LogicalFiles': 1,
                      'LogicalDirectories': 0,
                      'LogicalSize': 123},
                     "getDirectorySize got incorrect directory size %s" % result)
    self.assertTrue(
        nonExistingDir in result["Value"]["Failed"],
        "getDirectorySize : %s should be in Failed %s" %
        (nonExistingDir,
         result))

    result = self.dfc.getDirectorySize([testDir, nonExistingDir], False, True)
    self.assertTrue(result["OK"], "getDirectorySize (calc) failed: %s" % result)
    self.assertTrue(
        testDir in result["Value"]["Successful"],
        "getDirectorySize (calc): %s should be in Successful %s" %
        (testDir,
         result))
    self.assertEqual(result["Value"]["Successful"][testDir],
                     {'LogicalFiles': 1,
                      'LogicalDirectories': 0,
                      'LogicalSize': 123},
                     "getDirectorySize got incorrect directory size %s" % result)
    self.assertTrue(
        nonExistingDir in result["Value"]["Failed"],
        "getDirectorySize (calc) : %s should be in Failed %s" %
        (nonExistingDir,
         result))

    result = self.dfc.listDirectory([parentDir, testDir, nonExistingDir])
    self.assertTrue(result["OK"], "listDirectory failed: %s" % result)
    self.assertTrue(
        parentDir in result["Value"]["Successful"],
        "listDirectory : %s should be in Successful %s" %
        (parentDir,
         result))
    self.assertEqual(list(result["Value"]["Successful"][parentDir]["SubDirs"]), [testDir],
                     "listDir : incorrect content for %s (%s)" % (parentDir, result))
    self.assertTrue(
        testDir in result["Value"]["Successful"],
        "listDirectory : %s should be in Successful %s" %
        (testDir,
         result))
    self.assertEqual(list(result["Value"]["Successful"][testDir]["Files"]), [testFile],
                     "listDir : incorrect content for %s (%s)" % (testDir, result))
    self.assertTrue(
        nonExistingDir in result["Value"]["Failed"],
        "listDirectory : %s should be in Failed %s" %
        (nonExistingDir,
         result))

    # We do it two times to make sure that
    # when updating something to the same value
    # returns a success if it is allowed
    for attempt in range(2):
      print("Attempt %s" % (attempt + 1))

      # Only admin can change path group
      resultG = self.dfc.changePathGroup({parentDir: "toto"})
      resultM = self.dfc.changePathMode({parentDir: 0o777})
      result = self.dfc.changePathOwner({parentDir: "toto"})

      result2 = self.dfc.getDirectoryMetadata([parentDir, testDir])

      self.assertTrue(result["OK"], "changePathOwner failed: %s" % result)
      self.assertTrue(resultG["OK"], "changePathOwner failed: %s" % result)
      self.assertTrue(resultM["OK"], "changePathMode failed: %s" % result)

      self.assertTrue(result2["OK"], "getDirectoryMetadata failed: %s" % result)

      # Since we were the owner we should have been able to do it in any case, admin or not

      self.assertTrue(
          parentDir in resultM["Value"]["Successful"],
          "changePathMode : %s should be in Successful %s" %
          (parentDir,
           resultM))
      self.assertEqual(
          result2['Value'].get(
              'Successful',
              {}).get(
              parentDir,
              {}).get('Mode'),
          0o777,
          "parentDir should have mode  %s %s" %
          (0o777,
           result2))
      self.assertEqual(
          result2['Value'].get(
              'Successful',
              {}).get(
              testDir,
              {}).get('Mode'),
          0o775,
          "testDir should not have changed %s" %
          result2)

      if isAdmin:
        self.assertTrue(
            parentDir in result["Value"]["Successful"],
            "changePathOwner : %s should be in Successful %s" %
            (parentDir,
             result))
        self.assertEqual(
            result2['Value'].get(
                'Successful',
                {}).get(
                parentDir,
                {}).get('Owner'),
            'toto',
            "parentDir should belong to  %s %s" %
            (proxyUser,
             result2))
        self.assertEqual(
            result2['Value'].get(
                'Successful',
                {}).get(
                testDir,
                {}).get('Owner'),
            proxyUser,
            "testDir should not have changed %s" %
            result2)

        self.assertTrue(
            parentDir in resultG["Value"]["Successful"],
            "changePathGroup : %s should be in Successful %s" %
            (parentDir,
             resultG))
        self.assertEqual(
            result2['Value'].get(
                'Successful',
                {}).get(
                parentDir,
                {}).get('OwnerGroup'),
            'toto',
            "parentDir should belong to  %s %s" %
            (proxyUser,
             result2))
        self.assertEqual(
            result2['Value'].get(
                'Successful',
                {}).get(
                testDir,
                {}).get('OwnerGroup'),
            proxyGroup,
            "testDir should not have changed %s" %
            result2)

      else:
        self.assertTrue(
            parentDir in result["Value"]["Failed"],
            "changePathOwner : %s should be in Failed %s" %
            (parentDir,
             result))
        self.assertEqual(
            result2['Value'].get(
                'Successful',
                {}).get(
                parentDir,
                {}).get('Owner'),
            proxyUser,
            "parentDir should not have changed Owner from %s ==> %s)" %
            (proxyUser,
             result2))
        self.assertEqual(
            result2['Value'].get(
                'Successful',
                {}).get(
                testDir,
                {}).get('Owner'),
            proxyUser,
            "testDir should not have changed Owner from %s ==> %s" %
            (proxyUser,
             result2))

        self.assertTrue(
            parentDir in resultG["Value"]["Failed"],
            "changePathGroup : %s should be in Failed %s" %
            (parentDir,
             resultG))
        self.assertEqual(
            result2['Value'].get(
                'Successful',
                {}).get(
                parentDir,
                {}).get('OwnerGroup'),
            proxyGroup,
            "parentDir should not have changed OwnerGroup from %s ==> %s" %
            (proxyGroup,
             result2))
        self.assertEqual(
            result2['Value'].get(
                'Successful',
                {}).get(
                testDir,
                {}).get('OwnerGroup'),
            proxyGroup,
            "testDir should not have changed Owner from %s ==> %s" %
            (proxyGroup,
             result2))

    # Do it recursively now

    # Only admin can change path group
    resultM = self.dfc.changePathMode({parentDir: 0o777}, True)
    resultG = self.dfc.changePathGroup({parentDir: "toto"}, True)
    result = self.dfc.changePathOwner({parentDir: "toto"}, True)

    result2 = self.dfc.getDirectoryMetadata([parentDir, testDir])
    result3 = self.dfc.getFileMetadata(testFile)

    self.assertTrue(result["OK"], "changePathOwner failed: %s" % result)
    self.assertTrue(resultG["OK"], "changePathOwner failed: %s" % result)
    self.assertTrue(resultM["OK"], "changePathMode failed: %s" % result)

    self.assertTrue(result2["OK"], "getDirectoryMetadata failed: %s" % result)
    self.assertTrue(result3["OK"], "getFileMetadata failed: %s" % result)

    # Since we were the owner we should have been able to do it in any case, admin or not
    self.assertTrue(
        parentDir in resultM["Value"]["Successful"],
        "changePathGroup : %s should be in Successful %s" %
        (parentDir,
         resultM))
    self.assertEqual(
        result2['Value'].get(
            'Successful',
            {}).get(
            parentDir,
            {}).get('Mode'),
        0o777,
        "parentDir should have mode %s %s" %
        (0o777,
            result2))
    self.assertEqual(
        result2['Value'].get(
            'Successful', {}).get(
            testDir, {}).get('Mode'), 0o777, "testDir should have mode %s %s" %
        (0o777, result2))
    self.assertEqual(
        result3['Value'].get(
            'Successful', {}).get(
            testFile, {}).get('Mode'), 0o777, "testFile should have mode %s %s" %
        (0o777, result3))

    if isAdmin:
      self.assertTrue(
          parentDir in result["Value"]["Successful"],
          "changePathOwner : %s should be in Successful %s" %
          (parentDir,
           result))
      self.assertEqual(
          result2['Value'].get(
              'Successful',
              {}).get(
              parentDir,
              {}).get('Owner'),
          'toto',
          "parentDir should belong to %s %s" %
          (proxyUser,
           result2))
      self.assertEqual(
          result2['Value'].get(
              'Successful', {}).get(
              testDir, {}).get('Owner'), 'toto', "testDir should belong to %s %s" %
          (proxyUser, result2))
      self.assertEqual(
          result3['Value'].get(
              'Successful',
              {}).get(
              testFile,
              {}).get('Owner'),
          'toto',
          "testFile should belong to %s %s" %
          (proxyUser,
           result3))

      self.assertTrue(
          parentDir in resultG["Value"]["Successful"],
          "changePathGroup : %s should be in Successful %s" %
          (parentDir,
           resultG))
      self.assertEqual(
          result2['Value'].get(
              'Successful',
              {}).get(
              parentDir,
              {}).get('OwnerGroup'),
          'toto',
          "parentDir should belong to %s %s" %
          (proxyGroup,
           result2))
      self.assertEqual(
          result2['Value'].get(
              'Successful',
              {}).get(
              testDir,
              {}).get('OwnerGroup'),
          'toto',
          "testDir should belong to %s %s" %
          (proxyGroup,
           result2))
      self.assertEqual(
          result3['Value'].get(
              'Successful',
              {}).get(
              testFile,
              {}).get('OwnerGroup'),
          'toto',
          "testFile should belong to %s %s" %
          (proxyGroup,
           result3))

    else:
      self.assertTrue(
          parentDir in result["Value"]["Failed"],
          "changePathOwner : %s should be in Failed %s" %
          (parentDir,
           result))
      self.assertEqual(
          result2['Value'].get(
              'Successful',
              {}).get(
              parentDir,
              {}).get('Owner'),
          proxyUser,
          "parentDir should not have changed from %s ==> %s" %
          (proxyUser,
           result2))
      self.assertEqual(
          result2['Value'].get(
              'Successful',
              {}).get(
              testDir,
              {}).get('Owner'),
          proxyUser,
          "testDir should not have changed from %s ==> %s" %
          (proxyUser,
           result2))
      self.assertEqual(
          result3['Value'].get(
              'Successful',
              {}).get(
              testFile,
              {}).get('Owner'),
          proxyUser,
          "testFile should not have changed from %s ==> %s" %
          (proxyUser,
           result3))

      self.assertTrue(
          parentDir in resultG["Value"]["Failed"],
          "changePathGroup : %s should be in Failed %s" %
          (parentDir,
           resultG))
      self.assertEqual(
          result2['Value'].get(
              'Successful',
              {}).get(
              parentDir,
              {}).get('OwnerGroup'),
          proxyGroup,
          "parentDir should not have changed from %s ==> %s" %
          (proxyGroup,
           result2))
      self.assertEqual(
          result2['Value'].get(
              'Successful',
              {}).get(
              testDir,
              {}).get('OwnerGroup'),
          proxyGroup,
          "testDir should not have changed from %s ==> %s" %
          (proxyGroup,
           result2))
      self.assertEqual(
          result3['Value'].get(
              'Successful',
              {}).get(
              testFile,
              {}).get('OwnerGroup'),
          proxyGroup,
          "testFile should not have changed from %s ==> %s" %
          (proxyGroup,
           result3))

    # Cleaning after us
    result = self.dfc.removeFile(testFile)
    self.assertTrue(result["OK"], "removeFile failed: %s" % result)

    result = self.dfc.removeDirectory([testDir, nonExistingDir])
    self.assertTrue(result["OK"], "removeDirectory failed: %s" % result)
    self.assertTrue(
        testDir in result["Value"]["Successful"],
        "removeDirectory : %s should be in Successful %s" %
        (testDir,
         result))
    self.assertTrue(
        result["Value"]["Successful"][testDir],
        "removeDirectory : %s should be in True %s" %
        (testDir,
         result))
    self.assertTrue(
        nonExistingDir in result["Value"]["Successful"],
        "removeDirectory : %s should be in Successful %s" %
        (nonExistingDir,
         result))
    self.assertTrue(
        result["Value"]["Successful"][nonExistingDir],
        "removeDirectory : %s should be in True %s" %
        (nonExistingDir,
         result))


if __name__ == '__main__':

  res = getProxyInfo()
  if not res['OK']:
    sys.exit(1)

  res = res['Value']
  proxyUser = res.get('username', 'anon')
  proxyGroup = res.get('group', 'anon')
  properties = res.get('properties', [])
  properties.extend(res.get('groupProperties', []))

  isAdmin = FC_MANAGEMENT in properties
  print("Running test with admin privileges : ", isAdmin)

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UserGroupCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FileCase))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ReplicaCase))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryCase))

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
