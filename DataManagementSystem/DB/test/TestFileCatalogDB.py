from DIRAC.Core.Base import Script
Script.parseCommandLine()

import unittest
import itertools
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB

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
            'properties': [],
            'isProxy': False}


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
                       'UniqueGUID'          : False,
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
    self.assert_( ret["OK"], "addSE failed when adding new SE: %s" % ret )

    seId = ret["Value"]
    # create it again
    ret = self.db.addSE( seName, credDict )
    self.assertEqual( ret["Value"], seId, "addSE failed when adding existing SE: %s" % ret )
    # remove it
    ret = self.db.deleteSE( seName, credDict )
    self.assert_( ret["OK"], "deleteE failed %s" % ret )


class UserGroupCase( FileCatalogDBTestCase ):

  def test_userOperations( self ):
    """Testing the user related operations"""

    # Add the user
    result = self.db.addUser( testUser, credDict )
    self.assert_( result['OK'], "AddUser failed when adding new user: %s" % result )
    # Add an existing user
    result = self.db.addUser( testUser, credDict )
    self.assert_( result['OK'], "AddUser failed when adding existing user: %s" % result )
    # Fetch the list of user
    result = self.db.getUsers( credDict )
    self.assert_( result['OK'], "getUsers failed: %s" % result )
    # Check if our user is present
    self.assert_( testUser in result['Value'], "getUsers failed: %s" % result )
    # remove the user we created
    result = self.db.deleteUser( testUser, credDict )
    self.assert_( result['OK'], "deleteUser failed: %s" % result )


  def test_groupOperations( self ):
    """Testing the group related operations"""

    # Create new group
    result = self.db.addGroup( testGroup, credDict )
    self.assert_( result['OK'], "AddGroup failed when adding new user: %s" % result )
    result = self.db.addGroup( testGroup, credDict )
    self.assert_( result['OK'], "AddGroup failed when adding existing user: %s" % result )
    result = self.db.getGroups( credDict )
    self.assert_( result['OK'], "getGroups failed: %s" % result )
    self.assert_( testGroup in result['Value'] )
    result = self.db.deleteGroup( testGroup, credDict )
    self.assert_( result['OK'], "deleteGroup failed: %s" % result )




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
                                         'GUID':1000,
                                         'Checksum':'0' } }, credDict )
    self.assert_( result['OK'], "addFile failed when adding new file %s" % result )



    # Re-adding the same file
    result = self.db.addFile( { testFile: { 'PFN': 'testfile',
                                         'SE': 'testSE' ,
                                         'Size':123,
                                         'GUID':1000,
                                         'Checksum':'0' } }, credDict )
    self.assert_( result["OK"], "addFile failed when adding existing file %s" % result )
    self.assert_( testFile in result["Value"]["Failed"], "addFile failed: it should not be possible to add an existing lfn %s" % result )


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
                                         'GUID':1000,
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
                                         'GUID':1000,
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

    testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )




