from DIRAC.Core.Base import Script
Script.parseCommandLine()

import unittest
import itertools
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB

from DIRAC.Core.Security.Properties import FC_MANAGEMENT

seName = "mySE"
testUser  = 'atsareg'
testGroup = 'dirac_user'
testDir = '/vo.formation.idgrilles.fr/user/a/atsareg/testdir'
parentDir = '/vo.formation.idgrilles.fr/user/a/atsareg'
nonExistingDir = "/I/Dont/exist/dir"
testFile  = '/vo.formation.idgrilles.fr/user/a/atsareg/testdir/testfile'
nonExistingFile = "/I/Dont/exist"
credDict = {'DN': '/DC=ch/DC=cern/OU=computers/CN=volhcb12.cern.ch',
            'extraCredentials': 'hosts',
            'group': 'visitor',
            'CN': 'volhcb12.cern.ch',
            'x509Chain': "<X509Chain 3 certs [/DC=ch/DC=cern/OU=computers/CN=volhcb12.cern.ch][/DC=ch/DC=cern/CN=CERN Trusted Certification Authority][/DC=ch/DC=cern/CN=CERN Root CA]>",
            'username': 'anonymous',
            'isLimitedProxy': False,
            'properties': [FC_MANAGEMENT],
            'isProxy': False}

isAdmin = False


# TESTS WERE DESIGNED WITH THIS CONFIGURATION
# DATABASE_CONFIG = {  'UserGroupManager'  : 'UserAndGroupManagerDB',
#                        'SEManager'         : 'SEManagerDB',
#                        'SecurityManager'   : 'NoSecurityManager',
#                        'DirectoryManager'  : 'DirectoryLevelTree',
#                        'FileManager'       : 'FileManager',
#                        'DirectoryMetadata' : 'DirectoryMetadata',
#                        'FileMetadata'      : 'FileMetadata',
#                        'DatasetManager'    : 'DatasetManager',
#                        'UniqueGUID'          : False,
#                        'GlobalReadAccess'    : True,
#                        'LFNPFNConvention'    : 'Strong',
#                        'ResolvePFN'          : True,
#                        'DefaultUmask'        : 0775,
#                        'ValidFileStatus'     : ['AprioriGood', 'Trash', 'Removing', 'Probing'],
#                        'ValidReplicaStatus'  : ['AprioriGood', 'Trash', 'Removing', 'Probing'],
#                        'VisibleFileStatus'   : ['AprioriGood'],
#                        'VisibleReplicaStatus': ['AprioriGood'] }



DATABASE_CONFIG = {  'UserGroupManager'  : 'UserAndGroupManagerDB',  # UserAndGroupManagerDB, UserAndGroupManagerCS
                       'SEManager'         : 'SEManagerDB',  # SEManagerDB, SEManagerCS
                       'SecurityManager'   : 'NoSecurityManager',  # NoSecurityManager, DirectorySecurityManager, FullSecurityManager
                       'DirectoryManager'  : 'DirectoryLevelTree',  # DirectorySimpleTree, DirectoryFlatTree, DirectoryNodeTree, DirectoryLevelTree
                       'FileManager'       : 'FileManager',  # FileManagerFlat, FileManager
                       'DirectoryMetadata' : 'DirectoryMetadata',
                       'FileMetadata'      : 'FileMetadata',
                       'DatasetManager'    : 'DatasetManager',
                       'UniqueGUID'          : True,
                       'GlobalReadAccess'    : True,
                       'LFNPFNConvention'    : 'Strong',
                       'ResolvePFN'          : True,
                       'DefaultUmask'        : 0775,
                       'ValidFileStatus'     : ['AprioriGood', 'Trash', 'Removing', 'Probing'],
                       'ValidReplicaStatus'  : ['AprioriGood', 'Trash', 'Removing', 'Probing'],
                       'VisibleFileStatus'   : ['AprioriGood'],
                       'VisibleReplicaStatus': ['AprioriGood'] }

ALL_MANAGERS = { "UserGroupManager"  : ["UserAndGroupManagerDB", "UserAndGroupManagerCS"],
                    "SEManager" : ["SEManagerDB", "SEManagerCS"],
                    "SecurityManager" : ["NoSecurityManager", "DirectorySecurityManager", "FullSecurityManager"],
                    "DirectoryManager" : ["DirectorySimpleTree", "DirectoryFlatTree", "DirectoryNodeTree", "DirectoryLevelTree"],
                    "FileManager" : ["FileManagerFlat", "FileManager"],
                    }

ALL_MANAGERS_NO_CS = { "UserGroupManager"  : ["UserAndGroupManagerDB"],
                    "SEManager" : ["SEManagerDB"],
                    "SecurityManager" : ["NoSecurityManager", "DirectorySecurityManager", "FullSecurityManager"],
                    "DirectoryManager" : ["DirectorySimpleTree", "DirectoryFlatTree", "DirectoryNodeTree", "DirectoryLevelTree"],
                    "FileManager" : ["FileManagerFlat", "FileManager"],
                    }

DEFAULT_MANAGER = { "UserGroupManager"  : ["UserAndGroupManagerDB"],
                    "SEManager" : ["SEManagerDB"],
                    "SecurityManager" : ["DirectorySecurityManagerWithDelete"],
                    "DirectoryManager" : ["DirectoryClosure"],
                    "FileManager" : ["FileManagerPs"],
                    }

DEFAULT_MANAGER_2 = { "UserGroupManager"  : ["UserAndGroupManagerDB"],
                    "SEManager" : ["SEManagerDB"],
                    "SecurityManager" : ["NoSecurityManager"],
                    "DirectoryManager" : ["DirectoryLevelTree"],
                    "FileManager" : ["FileManager"],
                    }

MANAGER_TO_TEST = DEFAULT_MANAGER


class FileCatalogDBTestCase( unittest.TestCase ):
  """ Base class for the FileCatalogDB test cases
  """

  def setUp( self ):
    self.db = FileCatalogDB()
#     for table in self.db._query( "Show tables;" )["Value"]:
#       self.db.deleteEntries( table[0] )
    self.db.setConfig( DATABASE_CONFIG )

  def tearDown(self):
    pass
#     for table in self.db._query( "Show tables;" )["Value"]:
#       self.db.deleteEntries( table[0] )

class SECase ( FileCatalogDBTestCase ):

  def test_seOperations( self ):
    """Testing SE related operation"""

    # create SE
    ret = self.db.addSE( seName, credDict )
    if isAdmin:

      self.assert_( ret["OK"], "addSE failed when adding new SE: %s" % ret )

      seId = ret["Value"]
      # create it again
      ret = self.db.addSE( seName, credDict )
      self.assertEqual( ret["Value"], seId, "addSE failed when adding existing SE: %s" % ret )

    else:
      self.assertEqual( ret["OK"], False, "addSE should fail when adding new SE as non admin: %s" % ret )

    # remove it
    ret = self.db.deleteSE( seName, credDict )
    self.assertEqual( ret["OK"], True if isAdmin else False, "deleteE failed %s" % ret )


class UserGroupCase( FileCatalogDBTestCase ):

  def test_userOperations( self ):
    """Testing the user related operations"""


    expectedRes = None
    if isAdmin:
      print "Running UserTest in admin mode"
      expectedRes = True
    else:
      print "Running UserTest in non admin mode"
      expectedRes = False

    # Add the user
    result = self.db.addUser( testUser, credDict )
    self.assertEqual( result['OK'], expectedRes, "AddUser failed when adding new user: %s" % result )
    # Add an existing user
    result = self.db.addUser( testUser, credDict )
    self.assertEqual( result['OK'], expectedRes, "AddUser failed when adding existing user: %s" % result )
    # Fetch the list of user
    result = self.db.getUsers( credDict )
    self.assertEqual( result['OK'], expectedRes, "getUsers failed: %s" % result )
    if isAdmin:
      # Check if our user is present
      self.assertEqual( testUser in result['Value'], expectedRes, "getUsers failed: %s" % result )
    # remove the user we created
    result = self.db.deleteUser( testUser, credDict )
    self.assertEqual( result['OK'], expectedRes, "deleteUser failed: %s" % result )


  def test_groupOperations( self ):
    """Testing the group related operations"""

    expectedRes = None
    if isAdmin:
      print "Running UserTest in admin mode"
      expectedRes = True
    else:
      print "Running UserTest in non admin mode"
      expectedRes = False

    # Create new group
    result = self.db.addGroup( testGroup, credDict )
    self.assertEqual( result['OK'], expectedRes, "AddGroup failed when adding new user: %s" % result )
    result = self.db.addGroup( testGroup, credDict )
    self.assertEqual( result['OK'], expectedRes, "AddGroup failed when adding existing user: %s" % result )
    result = self.db.getGroups( credDict )
    self.assertEqual( result['OK'], expectedRes, "getGroups failed: %s" % result )
    if isAdmin:
      self.assertEqual( testGroup in result['Value'], expectedRes )
    result = self.db.deleteGroup( testGroup, credDict )
    self.assertEqual( result['OK'], expectedRes, "deleteGroup failed: %s" % result )




class FileCase( FileCatalogDBTestCase ):

  def test_fileOperations( self ):
    """
      Tests the File related Operations
      this test requires the SE to be properly defined in the CS -> NO IT DOES NOT!!
    """
    # Adding a new file
    result = self.db.addFile( { testFile: { 'PFN': 'testfile',
                                         'SE': 'testSE' ,
                                         'Size':123,
                                         'GUID':'1000',
                                         'Checksum':'0' } }, credDict )
    self.assert_( result['OK'], "addFile failed when adding new file %s" % result )

    result = self.db.exists( testFile , credDict )
    self.assert_( result['OK'] )
    self.assertEqual( result['Value'].get( 'Successful', {} ).get( testFile ),
                       testFile, "exists( testFile) should be the same lfn %s" % result )


    result = self.db.exists( {testFile:'1000'} , credDict )
    self.assert_( result['OK'] )
    self.assertEqual( result['Value'].get( 'Successful', {} ).get( testFile ),
                       testFile, "exists( testFile : 1000) should be the same lfn %s" % result )
    
    result = self.db.exists( {testFile:{'GUID' : '1000', 'PFN' : 'blabla'}} , credDict )
    self.assert_( result['OK'] )
    self.assertEqual( result['Value'].get( 'Successful', {} ).get( testFile ),
                       testFile, "exists( testFile : 1000) should be the same lfn %s" % result )

    # In fact, we don't check if the GUID is correct...
    result = self.db.exists( {testFile:'1001'}, credDict )
    self.assert_( result['OK'] )
    self.assertEqual( result['Value'].get( 'Successful', {} ).get( testFile ),
                       testFile, "exists( testFile : 1001) should be the same lfn %s" % result )

    result = self.db.exists( {testFile + '2' : '1000'}, credDict )
    self.assert_( result['OK'] )
    self.assertEqual( result['Value'].get( 'Successful', {} ).get( testFile + '2' ),
                       testFile, "exists( testFile2 : 1000) should return testFile %s" % result )

    # Re-adding the same file
    result = self.db.addFile( { testFile: { 'PFN': 'testfile',
                                         'SE': 'testSE' ,
                                         'Size':123,
                                         'GUID':'1000',
                                         'Checksum':'0' } }, credDict )
    self.assert_( result["OK"], "addFile failed when adding existing file with same param %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "addFile failed: it should be possible to add an existing lfn with same param %s" % result )

    # Adding same file with different param
    result = self.db.addFile( { testFile: { 'PFN': 'testfile',
                                         'SE': 'testSE' ,
                                         'Size':123,
                                         'GUID':'1000',
                                         'Checksum':'1' } }, credDict )
    self.assert_( result["OK"], "addFile failed when adding existing file with different parem %s" % result )
    self.assert_( testFile in result["Value"]["Failed"], "addFile failed: it should not be possible to add an existing lfn with different param %s" % result )


    result = self.db.addFile( { testFile + '2':  { 'PFN': 'testfile',
                                         'SE': 'testSE' ,
                                         'Size':123,
                                         'GUID':'1000',
                                         'Checksum':'0' } }, credDict )
    self.assert_( result["OK"], "addFile failed when adding existing file %s" % result )
    self.assert_( testFile + '2' in result["Value"]["Failed"], "addFile failed: it should not be possible to add a new lfn with existing GUID %s" % result )

    ##################################################################################
    # Setting existing status of existing file
    result = self.db.setFileStatus( {testFile:"AprioriGood"}, credDict )
    self.assert_( result["OK"], "setFileStatus failed when setting existing status of existing file %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "setFileStatus failed: %s should be in successful (%s)" % ( testFile, result ) )

    # Setting unexisting status of existing file
    result = self.db.setFileStatus( {testFile:"Happy"}, credDict )
    self.assert_( result["OK"], "setFileStatus failed when setting un-existing status of existing file %s" % result )
    self.assert_( testFile in result["Value"]["Failed"], "setFileStatus should have failed %s" % result )

    # Setting existing status of unexisting file
    result = self.db.setFileStatus( {nonExistingFile:"Trash"}, credDict )
    self.assert_( result["OK"], "setFileStatus failed when setting existing status of non-existing file %s" % result )
    self.assert_( nonExistingFile in result["Value"]["Failed"], "setFileStatus failed: %s should be in failed (%s)" % ( nonExistingFile, result ) )

    ##################################################################################

    result = self.db.isFile( [testFile, nonExistingFile], credDict )
    self.assert_( result["OK"], "isFile failed: %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "isFile : %s should be in Successful %s" % ( testFile, result ) )
    self.assert_( result["Value"]["Successful"][testFile], "isFile : %s should be seen as a file %s" % ( testFile, result ) )
    self.assert_( nonExistingFile in result["Value"]["Successful"], "isFile : %s should be in Successful %s" % ( nonExistingFile, result ) )
    self.assert_( result["Value"]["Successful"][nonExistingFile] == False, "isFile : %s should be seen as a file %s" % ( nonExistingFile, result ) )

    result = self.db.setFileOwner( {testFile :  "toto", nonExistingFile : "tata"}, credDict )
    self.assert_( result["OK"], "setFileOwner failed: %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "setFileOwner : %s should be in Successful %s" % ( testFile, result ) )
    self.assert_( nonExistingFile in result["Value"]["Failed"], "setFileOwner : %s should be in Failed %s" % ( nonExistingFile, result ) )

    result = self.db.setFileGroup( {testFile : "toto", nonExistingFile :"tata"}, credDict )
    self.assert_( result["OK"], "setFileGroup failed: %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "setFileGroup : %s should be in Successful %s" % ( testFile, result ) )
    self.assert_( nonExistingFile in result["Value"]["Failed"], "setFileGroup : %s should be in Failed %s" % ( nonExistingFile, result ) )

    result = self.db.setFileMode( {testFile : 044, nonExistingFile : 044}, credDict )
    self.assert_( result["OK"], "setFileMode failed: %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "setFileMode : %s should be in Successful %s" % ( testFile, result ) )
    self.assert_( nonExistingFile in result["Value"]["Failed"], "setFileMode : %s should be in Failed %s" % ( nonExistingFile, result ) )

    result = self.db.getFileSize( [testFile, nonExistingFile], credDict )
    self.assert_( result["OK"], "getFileSize failed: %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "getFileSize : %s should be in Successful %s" % ( testFile, result ) )
    self.assertEqual( result["Value"]["Successful"][testFile], 123, "getFileSize got incorrect file size %s" % result )
    self.assert_( nonExistingFile in result["Value"]["Failed"], "getFileSize : %s should be in Failed %s" % ( nonExistingFile, result ) )

    result = self.db.getFileMetadata( [testFile, nonExistingFile], credDict )
    self.assert_( result["OK"], "getFileMetadata failed: %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "getFileMetadata : %s should be in Successful %s" % ( testFile, result ) )
    self.assertEqual( result["Value"]["Successful"][testFile]["Owner"], "toto", "getFileMetadata got incorrect Owner %s" % result )
    self.assertEqual( result["Value"]["Successful"][testFile]["Status"], "AprioriGood", "getFileMetadata got incorrect status %s" % result )
    self.assert_( nonExistingFile in result["Value"]["Failed"], "getFileMetadata : %s should be in Failed %s" % ( nonExistingFile, result ) )

#      DOES NOT FOLLOW THE SUCCESSFUL/FAILED CONVENTION
#     result = self.db.getFileDetails( [testFile, nonExistingFile], credDict )
#     self.assert_( result["OK"], "getFileDetails failed: %s" % result )
#     self.assert_( testFile in result["Value"]["Successful"], "getFileDetails : %s should be in Successful %s" % ( testFile, result ) )
#     self.assertEqual( result["Value"]["Successful"][testFile]["Owner"], "toto", "getFileDetails got incorrect Owner %s" % result )
#     self.assert_( nonExistingFile in result["Value"]["Failed"], "getFileDetails : %s should be in Failed %s" % ( nonExistingFile, result ) )

#    ADD SOMETHING ABOUT FILE ANCESTORS AND DESCENDENTS

    result = self.db.removeFile( [testFile, nonExistingFile], credDict )
    self.assert_( result["OK"], "removeFile failed: %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "removeFile : %s should be in Successful %s" % ( testFile, result ) )
    self.assert_( result["Value"]["Successful"][testFile], "removeFile : %s should be in True %s" % ( testFile, result ) )
    self.assert_( result["Value"]["Successful"][nonExistingFile], "removeFile : %s should be in True %s" % ( nonExistingFile, result ) )



class ReplicaCase( FileCatalogDBTestCase ):

  def test_replicaOperations( self ):
    """
      this test requires the SE to be properly defined in the CS -> NO IT DOES NOT!!
    """
    # Adding a new file
    result = self.db.addFile( { testFile: { 'PFN': 'testfile',
                                         'SE': 'testSE' ,
                                         'Size':123,
                                         'GUID':'1000',
                                         'Checksum':'0' } }, credDict )
    self.assert_( result['OK'], "addFile failed when adding new file %s" % result )

    # Adding new replica
    result = self.db.addReplica( {testFile : {"PFN" : "testFile", "SE" : "otherSE"}}, credDict )
    self.assert_( result['OK'], "addReplica failed when adding new Replica %s" % result )
    self.assert_( testFile in result['Value']["Successful"], "addReplica failed when adding new Replica %s" % result )

    # Adding the same replica
    result = self.db.addReplica( {testFile : {"PFN" : "testFile", "SE" : "otherSE"}}, credDict )
    self.assert_( result['OK'], "addReplica failed when adding new Replica %s" % result )
    self.assert_( testFile in result['Value']["Successful"], "addReplica failed when adding new Replica %s" % result )

    # Adding replica of a non existing file
    result = self.db.addReplica( {nonExistingFile : {"PFN" : "Idontexist", "SE" : "otherSE"}}, credDict )
    self.assert_( result['OK'], "addReplica failed when adding Replica to non existing Replica %s" % result )
    self.assert_( nonExistingFile in result['Value']["Failed"], "addReplica for non existing file should go in Failed  %s" % result )


    # Setting existing status of existing Replica
    result = self.db.setReplicaStatus( {testFile: {"Status" : "Trash", "SE" : "otherSE"}}, credDict )
    self.assert_( result["OK"], "setReplicaStatus failed when setting existing status of existing Replica %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "setReplicaStatus failed: %s should be in successful (%s)" % ( testFile, result ) )

    # Setting non existing status of existing Replica
    result = self.db.setReplicaStatus( {testFile: {"Status" : "randomStatus", "SE" : "otherSE"}}, credDict )
    self.assert_( result["OK"], "setReplicaStatus failed when setting non-existing status of existing Replica %s" % result )
    self.assert_( testFile in result["Value"]["Failed"], "setReplicaStatus failed: %s should be in Failed (%s)" % ( testFile, result ) )

    # Setting existing status of non-existing Replica
    result = self.db.setReplicaStatus( {testFile: {"Status" : "Trash", "SE" : "nonExistingSe"}}, credDict )
    self.assert_( result["OK"], "setReplicaStatus failed when setting existing status of non-existing Replica %s" % result )
    self.assert_( testFile in result["Value"]["Failed"], "setReplicaStatus failed: %s should be in Failed (%s)" % ( testFile, result ) )

    # Setting existing status of non-existing File
    result = self.db.setReplicaStatus( {nonExistingFile: {"Status" : "Trash", "SE" : "nonExistingSe"}}, credDict )
    self.assert_( result["OK"], "setReplicaStatus failed when setting existing status of non-existing File %s" % result )
    self.assert_( nonExistingFile in result["Value"]["Failed"], "setReplicaStatus failed: %s should be in Failed (%s)" % ( nonExistingFile, result ) )


    # Getting existing status of existing Replica but not visible
    result = self.db.getReplicaStatus( {testFile: "testSE"}, credDict )
    self.assert_( result["OK"], "getReplicaStatus failed when getting existing status of existing Replica %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "getReplicaStatus failed: %s should be in Successful (%s)" % ( testFile, result ) )

    # Getting existing status of existing Replica but not visible
    result = self.db.getReplicaStatus( {testFile : "otherSE"}, credDict )
    self.assert_( result["OK"], "getReplicaStatus failed when getting existing status of existing Replica but not visible %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "getReplicaStatus failed: %s should be in Successful (%s)" % ( testFile, result ) )

    # Getting status of non-existing File but not visible
    result = self.db.getReplicaStatus( {nonExistingFile: "testSE"}, credDict )
    self.assert_( result["OK"], "getReplicaStatus failed when getting status of non existing File %s" % result )
    self.assert_( nonExistingFile in result["Value"]["Failed"], "getReplicaStatus failed: %s should be in failed (%s)" % ( nonExistingFile, result ) )

    # Getting replicas of existing File and non existing file, seeing all replicas
    result = self.db.getReplicas( [testFile, nonExistingFile], allStatus = True, credDict = credDict )
    self.assert_( result["OK"], "getReplicas failed %s" % result )
    self.assert_( testFile in result["Value"]["Successful"], "getReplicas failed, %s should be in Successful %s" % ( testFile, result ) )
    self.assertEqual( result["Value"]["Successful"][testFile], {"otherSE" : "", "testSE" : ""}, "getReplicas failed, %s should be in Successful %s" % ( testFile, result ) )
    self.assert_( nonExistingFile in result["Value"]["Failed"], "getReplicas failed, %s should be in Failed %s" % ( nonExistingFile, result ) )

    # removing master replica
    result = self.db.removeReplica( {testFile : { "SE" : "testSE"}}, credDict )
    self.assert_( result['OK'], "removeReplica failed when removing master Replica %s" % result )
    self.assert_( testFile in result['Value']["Successful"], "removeReplica failed when removing master Replica %s" % result )

    # removing non existing replica of existing File
    result = self.db.removeReplica( {testFile : { "SE" : "nonExistingSe2"}}, credDict )
    self.assert_( result['OK'], "removeReplica failed when removing non existing Replica %s" % result )
    self.assert_( testFile in result['Value']["Successful"], "removeReplica failed when removing new Replica %s" % result )

    # removing non existing replica of non existing file
    result = self.db.removeReplica( {nonExistingFile : { "SE" : "nonExistingSe3"}}, credDict )
    self.assert_( result['OK'], "removeReplica failed when removing replica of non existing File %s" % result )
    self.assert_( nonExistingFile in result['Value']["Successful"], "removeReplica of non existing file, %s should be in Successful %s" % ( nonExistingFile, result ) )

    # removing last replica
    result = self.db.removeReplica( {testFile : { "SE" : "otherSE"}}, credDict )
    self.assert_( result['OK'], "removeReplica failed when removing last Replica %s" % result )
    self.assert_( testFile in result['Value']["Successful"], "removeReplica failed when removing last Replica %s" % result )

    # Cleaning after us
    result = self.db.removeFile( testFile, credDict )
    self.assert_( result["OK"], "removeFile failed: %s" % result )



class DirectoryCase( FileCatalogDBTestCase ):

  def test_directoryOperations( self ):
    """
      Tests the Directory related Operations
      this test requires the SE to be properly defined in the CS -> NO IT DOES NOT!!
    """
    # Adding a new directory
    result = self.db.createDirectory( testDir, credDict )
    self.assert_( result['OK'], "addDirectory failed when adding new directory %s" % result )

    result = self.db.addFile( { testFile: { 'PFN': 'testfile',
                                         'SE': 'testSE' ,
                                         'Size':123,
                                         'GUID':'1000',
                                         'Checksum':'0' } }, credDict )
    self.assert_( result['OK'], "addFile failed when adding new file %s" % result )



    # Re-adding the same directory (CAUTION, different from addFile)
    result = self.db.createDirectory( testDir, credDict )
    self.assert_( result["OK"], "addDirectory failed when adding existing directory %s" % result )
    self.assert_( testDir in result["Value"]["Successful"], "addDirectory failed: it should be possible to add an existing lfn %s" % result )


    result = self.db.isDirectory( [testDir, nonExistingDir], credDict )
    self.assert_( result["OK"], "isDirectory failed: %s" % result )
    self.assert_( testDir in result["Value"]["Successful"], "isDirectory : %s should be in Successful %s" % ( testDir, result ) )
    self.assert_( result["Value"]["Successful"][testDir], "isDirectory : %s should be seen as a directory %s" % ( testDir, result ) )
    self.assert_( nonExistingDir in result["Value"]["Successful"], "isDirectory : %s should be in Successful %s" % ( nonExistingDir, result ) )
    self.assert_( result["Value"]["Successful"][nonExistingDir] == False, "isDirectory : %s should be seen as a directory %s" % ( nonExistingDir, result ) )

    result = self.db.getDirectorySize( [testDir, nonExistingDir], False, False, credDict )
    self.assert_( result["OK"], "getDirectorySize failed: %s" % result )
    self.assert_( testDir in result["Value"]["Successful"], "getDirectorySize : %s should be in Successful %s" % ( testDir, result ) )
    self.assertEqual( result["Value"]["Successful"][testDir], {'LogicalFiles': 1, 'LogicalDirectories': 0, 'LogicalSize': 123}, "getDirectorySize got incorrect directory size %s" % result )
    self.assert_( nonExistingDir in result["Value"]["Failed"], "getDirectorySize : %s should be in Failed %s" % ( nonExistingDir, result ) )


    result = self.db.getDirectorySize( [testDir, nonExistingDir], False, True, credDict )
    self.assert_( result["OK"], "getDirectorySize (calc) failed: %s" % result )
    self.assert_( testDir in result["Value"]["Successful"], "getDirectorySize (calc): %s should be in Successful %s" % ( testDir, result ) )
    self.assertEqual( result["Value"]["Successful"][testDir], {'LogicalFiles': 1, 'LogicalDirectories': 0, 'LogicalSize': 123}, "getDirectorySize got incorrect directory size %s" % result )
    self.assert_( nonExistingDir in result["Value"]["Failed"], "getDirectorySize (calc) : %s should be in Failed %s" % ( nonExistingDir, result ) )



    result = self.db.listDirectory( [parentDir, testDir, nonExistingDir], credDict )
    self.assert_( result["OK"], "listDirectory failed: %s" % result )
    self.assert_( parentDir in result["Value"]["Successful"], "listDirectory : %s should be in Successful %s" % ( parentDir, result ) )
    self.assertEqual( result["Value"]["Successful"][parentDir]["SubDirs"].keys(), [testDir], \
                     "listDir : incorrect content for %s (%s)" % ( parentDir, result ) )
    self.assert_( testDir in result["Value"]["Successful"], "listDirectory : %s should be in Successful %s" % ( testDir, result ) )
    self.assertEqual( result["Value"]["Successful"][testDir]["Files"].keys(), [testFile.split( "/" )[-1]], \
                     "listDir : incorrect content for %s (%s)" % ( testDir, result ) )
    self.assert_( nonExistingDir in result["Value"]["Failed"], "listDirectory : %s should be in Failed %s" % ( nonExistingDir, result ) )



    # Cleaning after us
    result = self.db.removeFile( testFile, credDict )
    self.assert_( result["OK"], "removeFile failed: %s" % result )

#     result = self.db.removeDirectory( [testDir, nonExistingDir], credDict )
#     self.assert_( result["OK"], "removeDirectory failed: %s" % result )
#     self.assert_( testDir in result["Value"]["Successful"], "removeDirectory : %s should be in Successful %s" % ( testDir, result ) )
#     self.assert_( result["Value"]["Successful"][testDir], "removeDirectory : %s should be in True %s" % ( testDir, result ) )
#     self.assert_( nonExistingDir in result["Value"]["Successful"], "removeDirectory : %s should be in Successful %s" % ( nonExistingDir, result ) )
#     self.assert_( result["Value"]["Successful"][nonExistingDir], "removeDirectory : %s should be in True %s" % ( nonExistingDir, result ) )





class DirectoryUsageCase ( FileCatalogDBTestCase ):

  def getPhysicalSize(self, sizeDict, dirName, seName):
    """ Extract the information from a ret dictionary
        and return the tuple (files, size) for a given
        directory and a se
    """
    try:
      val = sizeDict[dirName]['PhysicalSize'][seName]
      files = val['Files']
      size = val['Size']
      return ( files, size )
    
    except Exception as e:
      raise

  def getLogicalSize( self, sizeDict, dirName ):
    """ Extract the information from a ret dictionary
        and return the tuple (files, size) for a given
        directory and a se
    """
    try:
      files = sizeDict[dirName]['LogicalFiles']
      size = sizeDict[dirName]['LogicalSize']
      return ( files, size )

    except Exception as e:
#       print e
#       print "sizeDict %s" % sizeDict
#       print "dirName %s" % dirName
      raise

  def test_directoryUsage( self ):
    """Testing DirectoryUsage related operation"""
    # create SE

    # Only admin can run that
    if not isAdmin:
      return

    d1 = '/sizeTest/d1'
    d2 = '/sizeTest/d2'
    f1 = d1 + '/f1'
    f2 = d1 + '/f2'
    f3 = d2 + '/f3'

    f1Size = 3000000000
    f2Size = 3000000001
    f3Size = 3000000002

#     f1Size = 1
#     f2Size = 2
#     f3Size = 5

    for sen in ['se1', 'se2', 'se3']:
      ret = self.db.addSE( sen, credDict )
      self.assert_( ret["OK"] )

    for din in [d1, d2]:
      ret = self.db.createDirectory( din, credDict )
      self.assert_( ret["OK"] )


    ret = self.db.addFile( { f1: { 'PFN': 'f1se1',
                                         'SE': 'se1' ,
                                         'Size':f1Size,
                                         'GUID':'1000',
                                         'Checksum':'1' },
                             f2: { 'PFN': 'f2se2',
                                         'SE': 'se2' ,
                                         'Size':f2Size,
                                         'GUID':'1001',
                                         'Checksum':'2' } }, credDict )


    self.assert_( ret["OK"] )

    ret = self.db.getDirectorySize( [d1, d2], True, False, credDict )
    self.assert_( ret["OK"] )
    val = ret['Value']['Successful']

    d1s1 = self.getPhysicalSize( val, d1, 'se1' )
    d1s2 = self.getPhysicalSize( val, d1, 'se2' )
    d1l = self.getLogicalSize( val, d1 )

    self.assertEqual( d1s1 , ( 1, f1Size ), "Unexpected size %s, expected %s" % ( d1s1, ( 1, f1Size ) ) )
    self.assertEqual( d1s2 , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1s2, ( 1, f2Size ) ) )
    self.assertEqual( d1l , ( 2, f1Size + f2Size ), "Unexpected size %s, expected %s" % ( d1l, ( 2, f1Size + f2Size ) ) )

    ret = self.db.addReplica( {f1 : {"PFN" : "f1se2", "SE" : "se2"},
                               f2 : {"PFN" : "f1se3", "SE" : "se3"}},
                              credDict )

    self.assert_( ret['OK'] )

    ret = self.db.getDirectorySize( [d1, d2], True, False, credDict )
    self.assert_( ret["OK"] )
    val = ret['Value']['Successful']

    d1s1 = self.getPhysicalSize( val, d1, 'se1' )
    d1s2 = self.getPhysicalSize( val, d1, 'se2' )
    d1s3 = self.getPhysicalSize( val, d1, 'se3' )
    d1l = self.getLogicalSize( val, d1 )

    self.assertEqual( d1s1 , ( 1, f1Size ), "Unexpected size %s, expected %s" % ( d1s1, ( 1, f1Size ) ) )
    self.assertEqual( d1s2 , ( 2, f1Size + f2Size ), "Unexpected size %s, expected %s" % ( d1s2, ( 2, f1Size + f2Size ) ) )
    self.assertEqual( d1s3 , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1s3, ( 1, f2Size ) ) )
    self.assertEqual( d1l , ( 2, f1Size + f2Size ), "Unexpected size %s, expected %s" % ( d1l, ( 2, f1Size + f2Size ) ) )
    
    
    ret = self.db.removeFile( [f1], credDict )
    self.assert_( ret['OK'] )

    ret = self.db.getDirectorySize( [d1, d2], True, False, credDict )
    self.assert_( ret["OK"] )
    val = ret['Value']['Successful']

    # Here we should have the KeyError, since there are no files left on s1 in principle
    try:
      d1s1 = self.getPhysicalSize( val, d1, 'se1' )
    except KeyError:
      d1s1 = ( 0, 0 )
    d1s2 = self.getPhysicalSize( val, d1, 'se2' )
    d1s3 = self.getPhysicalSize( val, d1, 'se3' )
    d1l = self.getLogicalSize( val, d1 )

    self.assertEqual( d1s1 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d1s1, ( 0, 0 ) ) )
    self.assertEqual( d1s2 , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1s2, ( 1, f2Size ) ) )
    self.assertEqual( d1s3 , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1s3, ( 1, f2Size ) ) )
    self.assertEqual( d1l , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1l, ( 1, f2Size ) ) )


    ret = self.db.removeReplica( {f2 : { "SE" : "se2"}}, credDict )
    self.assert_( ret['OK'] )

    ret = self.db.getDirectorySize( [d1, d2], True, False, credDict )
    self.assert_( ret["OK"] )
    val = ret['Value']['Successful']

    # Here we should have the KeyError, since there are no files left on s1 in principle
    try:
      d1s2 = self.getPhysicalSize( val, d1, 'se1' )
    except KeyError:
      d1s2 = ( 0, 0 )
    d1s3 = self.getPhysicalSize( val, d1, 'se3' )
    d1l = self.getLogicalSize( val, d1 )

    self.assertEqual( d1s2 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d1s2, ( 0, 0 ) ) )
    self.assertEqual( d1s3 , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1s3, ( 1, f2Size ) ) )
    self.assertEqual( d1l , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1l, ( 1, f2Size ) ) )
    

    ret = self.db.addFile( { f1: { 'PFN': 'f1se1',
                                         'SE': 'se1' ,
                                         'Size':f1Size,
                                         'GUID':'1000',
                                         'Checksum':'1' },
                             f3: { 'PFN': 'f3se3',
                                         'SE': 'se3' ,
                                         'Size':f3Size,
                                         'GUID':'1002',
                                         'Checksum':'3' } }, credDict )


    self.assert_( ret["OK"] )

    ret = self.db.getDirectorySize( [d1, d2], True, False, credDict )
    self.assert_( ret["OK"] )
    val = ret['Value']['Successful']

    d1s1 = self.getPhysicalSize( val, d1, 'se1' )
    d1s3 = self.getPhysicalSize( val, d1, 'se3' )
    d2s3 = self.getPhysicalSize( val, d2, 'se3' )
    d1l = self.getLogicalSize( val, d1 )
    d2l = self.getLogicalSize( val, d2 )

    self.assertEqual( d1s1 , ( 1, f1Size ), "Unexpected size %s, expected %s" % ( d1s1, ( 1, f1Size ) ) )
    self.assertEqual( d1s3 , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1s3, ( 1, f2Size ) ) )
    self.assertEqual( d2s3 , ( 1, f3Size ), "Unexpected size %s, expected %s" % ( d2s3, ( 1, f3Size ) ) )
    self.assertEqual( d1l , ( 2, f1Size + f2Size ), "Unexpected size %s, expected %s" % ( d1l, ( 2, f1Size + f2Size ) ) )
    self.assertEqual( d2l , ( 1, f3Size ), "Unexpected size %s, expected %s" % ( d2l, ( 1, f3Size ) ) )


    ret = self.db.removeReplica( {f1 : { "SE" : "se1"}}, credDict )
    self.assert_( ret['OK'] )

    ret = self.db.getDirectorySize( [d1, d2], True, False, credDict )
    self.assert_( ret["OK"] )
    val = ret['Value']['Successful']


    try:
      d1s1 = self.getPhysicalSize( val, d1, 'se1' )
    except KeyError:
      d1s1 = ( 0, 0 )
    d1s3 = self.getPhysicalSize( val, d1, 'se3' )
    d2s3 = self.getPhysicalSize( val, d2, 'se3' )
    d1l = self.getLogicalSize( val, d1 )
    d2l = self.getLogicalSize( val, d2 )

    self.assertEqual( d1s1 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d1s1, ( 0, 0 ) ) )
    self.assertEqual( d1s3 , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1s3, ( 1, f2Size ) ) )
    self.assertEqual( d2s3 , ( 1, f3Size ), "Unexpected size %s, expected %s" % ( d2s3, ( 1, f3Size ) ) )
    # This one is silly... there are no replicas of f1, but since the file is still there,
    # the logical size does not change
    self.assertEqual( d1l , ( 2, f1Size + f2Size ), "Unexpected size %s, expected %s" % ( d1l, ( 2, f1Size + f2Size ) ) )
    self.assertEqual( d2l , ( 1, f3Size ), "Unexpected size %s, expected %s" % ( d2l, ( 1, f3Size ) ) )


    ret = self.db.removeFile( [f1], credDict )
    self.assert_( ret['OK'] )

    ret = self.db.getDirectorySize( [d1, d2], True, False, credDict )
    self.assert_( ret["OK"] )
    val = ret['Value']['Successful']

    try:
      d1s1 = self.getPhysicalSize( val, d1, 'se1' )
    except KeyError:
      d1s1 = ( 0, 0 )

    d1s3 = self.getPhysicalSize( val, d1, 'se3' )
    d2s3 = self.getPhysicalSize( val, d2, 'se3' )
    d1l = self.getLogicalSize( val, d1 )
    d2l = self.getLogicalSize( val, d2 )

    self.assertEqual( d1s1 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d1s1, ( 0, 0 ) ) )
    self.assertEqual( d1s3 , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1s3, ( 1, f2Size ) ) )
    self.assertEqual( d2s3 , ( 1, f3Size ), "Unexpected size %s, expected %s" % ( d2s3, ( 1, f3Size ) ) )
    self.assertEqual( d1l , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1l, ( 1, f2Size ) ) )
    self.assertEqual( d2l , ( 1, f3Size ), "Unexpected size %s, expected %s" % ( d2l, ( 1, f3Size ) ) )



    ret = self.db.removeReplica( {f2 : { "SE" : "se3"},
                                  f3 : { "SE" : "se3"}}, credDict )
    self.assert_( ret['OK'] )

    ret = self.db.getDirectorySize( [d1, d2], True, False, credDict )
    self.assert_( ret["OK"] )
    val = ret['Value']['Successful']


    try:
      d1s1 = self.getPhysicalSize( val, d1, 'se1' )
    except KeyError:
      d1s1 = ( 0, 0 )
    try:
      d1s3 = self.getPhysicalSize( val, d1, 'se3' )
    except KeyError:
      d1s3 = ( 0, 0 )
    try:
      d2s3 = self.getPhysicalSize( val, d2, 'se3' )
    except KeyError:
      d2s3 = ( 0, 0 )
    d1l = self.getLogicalSize( val, d1 )
    d2l = self.getLogicalSize( val, d2 )

    self.assertEqual( d1s1 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d1s1, ( 0, 0 ) ) )
    self.assertEqual( d1s3 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d1s3, ( 0, 0 ) ) )
    self.assertEqual( d2s3 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d2s3, ( 0, 0 ) ) )
    # This one is silly... there are no replicas of f1, but since the file is still there,
    # the logical size does not change
    self.assertEqual( d1l , ( 1, f2Size ), "Unexpected size %s, expected %s" % ( d1l, ( 1, f2Size ) ) )
    self.assertEqual( d2l , ( 1, f3Size ), "Unexpected size %s, expected %s" % ( d2l, ( 1, f3Size ) ) )



    ret = self.db.removeFile( [f2, f3], credDict )
    self.assert_( ret['OK'] )

    ret = self.db.getDirectorySize( [d1, d2], True, False, credDict )
    self.assert_( ret["OK"] )
    val = ret['Value']['Successful']


    try:
      d1s1 = self.getPhysicalSize( val, d1, 'se1' )
    except KeyError:
      d1s1 = ( 0, 0 )
    try:
      d1s3 = self.getPhysicalSize( val, d1, 'se3' )
    except KeyError:
      d1s3 = ( 0, 0 )
    try:
      d2s3 = self.getPhysicalSize( val, d2, 'se3' )
    except KeyError:
      d2s3 = ( 0, 0 )
    d1l = self.getLogicalSize( val, d1 )
    d2l = self.getLogicalSize( val, d2 )

    self.assertEqual( d1s1 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d1s1, ( 0, 0 ) ) )
    self.assertEqual( d1s3 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d1s3, ( 0, 0 ) ) )
    self.assertEqual( d2s3 , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d2s3, ( 0, 0 ) ) )
    # This one is silly... there are no replicas of f1, but since the file is still there,
    # the logical size does not change
    self.assertEqual( d1l , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d1l, ( 0, 0 ) ) )
    self.assertEqual( d2l , ( 0, 0 ), "Unexpected size %s, expected %s" % ( d2l, ( 0, 0 ) ) )


#     self.assert_( testDir in result["Value"]["Successful"], "getDirectorySize : %s should be in Successful %s" % ( testDir, result ) )
#     self.assertEqual( result["Value"]["Successful"][testDir], {'LogicalFiles': 1, 'LogicalDirectories': 0, 'LogicalSize': 123}, "getDirectorySize got incorrect directory size %s" % result )
#     self.assert_( nonExistingDir in result["Value"]["Failed"], "getDirectorySize : %s should be in Failed %s" % ( nonExistingDir, result ) )
#
#
#     result = self.db.getDirectorySize( [testDir, nonExistingDir], False, True, credDict )
#     self.assert_( result["OK"], "getDirectorySize (calc) failed: %s" % result )
#     self.assert_( testDir in result["Value"]["Successful"], "getDirectorySize (calc): %s should be in Successful %s" % ( testDir, result ) )
#     self.assertEqual( result["Value"]["Successful"][testDir], {'LogicalFiles': 1, 'LogicalDirectories': 0, 'LogicalSize': 123}, "getDirectorySize got incorrect directory size %s" % result )
#     self.assert_( nonExistingDir in result["Value"]["Failed"], "getDirectorySize (calc) : %s should be in Failed %s" % ( nonExistingDir, result ) )






if __name__ == '__main__':

  managerTypes = MANAGER_TO_TEST.keys()
  all_combinations = list( itertools.product( *MANAGER_TO_TEST.values() ) )
  numberOfManager = len( managerTypes )
  


  for setup in all_combinations:
    print "Running with:"
    print ( "".join( ["\t %s : %s\n" % ( managerTypes[i], setup[i] ) for i in range( numberOfManager )] ) )
    for i in range( numberOfManager ):
      DATABASE_CONFIG[managerTypes[i]] = setup[i]



    suite = unittest.defaultTestLoader.loadTestsFromTestCase( SECase )
    suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UserGroupCase ) )
    suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FileCase ) )
    suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ReplicaCase ) )
    suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( DirectoryCase ) )
    suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( DirectoryUsageCase ) )


    # First run with admin privilege:
    isAdmin = True
    if FC_MANAGEMENT not in credDict['properties']:
      credDict['properties'].append( FC_MANAGEMENT )
    print "Running test with admin privileges"



    testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

    # Then run without admin privilege:
    isAdmin = False
    if FC_MANAGEMENT in credDict['properties']:
      credDict['properties'].remove( FC_MANAGEMENT )
    print "Running test without admin privileges"


    testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )



