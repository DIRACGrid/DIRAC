#! /usr/bin/env python

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Script                         import parseCommandLine, getPositionalArgs
parseCommandLine()
from DIRAC.Resources.Storage.StorageElement         import StorageElement
from DIRAC.Core.Utilities.ReturnValues              import returnSingleResult
from DIRAC.Core.Utilities.File                      import getSize
from DIRAC                                          import gLogger

import unittest, time, os, shutil, sys, types

positionalArgs = getPositionalArgs()

if len( positionalArgs ) < 3:
  print 'Usage: TestStoragePlugIn.py StorageElement <lfnDir> <localFile>'
  sys.exit()
else:
  storageElementToTest = positionalArgs[0]
  lfnDirToTest = positionalArgs[1]
  fileToTest = positionalArgs[2]

class StorageElementTestCase( unittest.TestCase ):
  """ Base class for the StorageElement test cases
  """
  def setUp( self ):
    self.numberOfFiles = 1
    self.storageElement = StorageElement( storageElementToTest )
    self.localSourceFile = fileToTest
    self.localFileSize = getSize( self.localSourceFile )
    self.destDirectory = lfnDirToTest
    # destinationDir = returnSingleResult( self.storageElement.getURL( self.destDirectory ) )['Value']
    destinationDir = self.destDirectory
    res = self.storageElement.createDirectory( destinationDir )
    self.assert_( res['OK'] )

  def tearDown( self ):
    # destinationDir = returnSingleResult( self.storageElement.getURL( self.destDirectory ) )['Value']
    res = self.storageElement.removeDirectory( self.destDirectory, recursive = True )
    self.assert_( res['OK'] )

class GetInfoTestCase( StorageElementTestCase ):

  def test_dump( self ):
    print '\n\n#########################################################################\n\n\t\t\tDump test\n'
    self.storageElement.dump()

  def test_isValid( self ):
    print '\n\n#########################################################################\n\n\t\t\tIs valid test\n'
    res = self.storageElement.isValid()
    self.assert_( res['OK'] )

  def test_getRemotePlugins( self ):
    print '\n\n#########################################################################\n\n\t\t\tGet remote protocols test\n'
    res = self.storageElement.getRemotePlugins()
    self.assert_( res['OK'] )
    self.assertEqual( type( res['Value'] ), types.ListType )

  def test_getLocalPlugins( self ):
    print '\n\n#########################################################################\n\n\t\t\tGet local protocols test\n'
    res = self.storageElement.getLocalPlugins()
    self.assert_( res['OK'] )
    self.assertEqual( type( res['Value'] ), types.ListType )

  def test_getPlugins( self ):
    print '\n\n#########################################################################\n\n\t\t\tGet protocols test\n'
    res = self.storageElement.getPlugins()
    self.assert_( res['OK'] )
    self.assertEqual( type( res['Value'] ), types.ListType )

  #def test_isLocalSE( self ):
  #  print '\n\n#########################################################################\n\n\t\t\tIs local SE test\n'
  #  res = self.storageElement.isLocalSE()
  #  self.assert_( res['OK'] )
  #  self.assertFalse( res['Value'] )

  #def test_getStorageElementOption( self ):
  #  print '\n\n#########################################################################\n\n\t\t\tGet storage element option test\n'
  #  res = self.storageElement.getStorageElementOption( 'BackendType' )
  #  self.assert_( res['OK'] )
  #  self.assertEqual( res['Value'], 'DISET' )

  def test_getStorageParameters( self ):
    print '\n\n#########################################################################\n\n\t\t\tGet storage parameters test\n'
    result = self.storageElement.getStorageParameters( 'DIP' )
    self.assert_( result['OK'] )
    resDict = result['Value']
    self.assertEqual( resDict['Protocol'], 'dips' )
    #self.assertEqual( resDict['SpaceToken'], 'LHCb_RAW' )
    #self.assertEqual( resDict['WSUrl'], '/srm/managerv2?SFN=' )
    #self.assertEqual( resDict['Host'], 'srm-lhcb.cern.ch' )
    #self.assertEqual( resDict['Path'], '/castor/cern.ch/grid' )
    #self.assertEqual( resDict['ProtocolName'], 'SRM2' )
    #self.assertEqual( resDict['Port'], '8443' )

class FileTestCases( StorageElementTestCase ):

  def test_exists( self ):
    print '\n\n#########################################################################\n\n\t\t\tExists test\n'
    destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
    # pfnForLfnRes = self.storageElement.getURL( destinationFilePath )
    #destinationPfn = pfnForLfnRes['Value']['Successful'].values()[0]
    fileDict = {destinationFilePath:self.localSourceFile}
    putFileRes = returnSingleResult( self.storageElement.putFile( fileDict ) )
    # File exists
    existsRes = returnSingleResult( self.storageElement.exists( destinationFilePath ) )
    # Now remove the destination file
    removeFileRes = returnSingleResult( self.storageElement.removeFile( destinationFilePath ) )
    # Check removed file
    missingExistsRes = returnSingleResult( self.storageElement.exists( destinationFilePath ) )
    # Check directories are handled properly
    destinationDir = os.path.dirname( destinationFilePath )
    directoryExistsRes = returnSingleResult( self.storageElement.exists( destinationDir ) )

    # Check that the put was done correctly
    self.assert_( putFileRes['OK'] )
    self.assert_( putFileRes['Value'] )
    self.assertEqual( putFileRes['Value'], self.localFileSize )
    # Check that we checked the file correctly
    self.assert_( existsRes['OK'] )
    self.assert_( existsRes['Value'] )
    # Check that the removal was done correctly
    self.assert_( removeFileRes['OK'] )
    self.assert_( removeFileRes['Value'] )
    # Check the exists for non existant file
    self.assert_( missingExistsRes['OK'] )
    self.assertFalse( missingExistsRes['Value'] )
    # Check that directories exist
    self.assert_( directoryExistsRes['OK'] )
    self.assert_( directoryExistsRes['Value'] )

  def test_isFile( self ):
    print '\n\n#########################################################################\n\n\t\t\tIs file size test\n'
    destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
    # pfnForLfnRes = returnSingleResult( self.storageElement.getURL( destinationFilePath ) )
    #destinationPfn = pfnForLfnRes['Value']
    fileDict = {destinationFilePath:self.localSourceFile}
    putFileRes = returnSingleResult( self.storageElement.putFile( fileDict ) )
    # Is a file
    isFileRes = returnSingleResult( self.storageElement.isFile( destinationFilePath ) )
    # Now remove the destination file
    removeFileRes = returnSingleResult( self.storageElement.removeFile( destinationFilePath ) )
    # Get metadata for a removed file
    missingIsFileRes = returnSingleResult( self.storageElement.isFile( destinationFilePath ) )
    # Check directories are handled properly
    destinationDir = os.path.dirname( destinationFilePath )
    directoryIsFileRes = returnSingleResult( self.storageElement.isFile( destinationDir ) )

    # Check that the put was done correctly
    self.assert_( putFileRes['OK'] )
    self.assert_( putFileRes['Value'] )
    self.assertEqual( putFileRes['Value'], self.localFileSize )
    # Check that we checked the file correctly
    self.assert_( isFileRes['OK'] )
    self.assert_( isFileRes['Value'] )
    # Check that the removal was done correctly
    self.assert_( removeFileRes['OK'] )
    self.assert_( removeFileRes['Value'] )
    # Check the is file for non existant file
    self.assertFalse( missingIsFileRes['OK'] )
    expectedError = "File does not exist"
    self.assert_( expectedError in missingIsFileRes['Message'] )
    # Check that is file operation with a directory
    self.assert_( directoryIsFileRes['OK'] )
    self.assertFalse( directoryIsFileRes['Value'] )

  def test_putFile( self ):
    print '\n\n#########################################################################\n\n\t\t\tPut file test\n'
    destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
    # pfnForLfnRes = returnSingleResult( self.storageElement.getURL( destinationFilePath ) )
    #destinationPfn = pfnForLfnRes['Value']
    fileDict = {destinationFilePath:self.localSourceFile}
    putFileRes = returnSingleResult( self.storageElement.putFile( fileDict ) )
    # Now remove the destination file
    removeFileRes = returnSingleResult( self.storageElement.removeFile( destinationFilePath ) )

    # Check that the put was done correctly
    self.assert_( putFileRes['OK'] )
    self.assert_( putFileRes['Value'] )
    self.assertEqual( putFileRes['Value'], self.localFileSize )
    # Check that the removal was done correctly
    self.assert_( removeFileRes['OK'] )
    self.assert_( removeFileRes['Value'] )

  def test_getFile( self ):
    print '\n\n#########################################################################\n\n\t\t\tGet file test\n'
    destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
    # pfnForLfnRes = returnSingleResult( self.storageElement.getURL( destinationFilePath ) )
    #destinationPfn = pfnForLfnRes['Value']
    fileDict = {destinationFilePath:self.localSourceFile}
    putFileRes = returnSingleResult( self.storageElement.putFile( fileDict ) )
    # Now get a local copy of the file
    getFileRes = returnSingleResult( self.storageElement.getFile( destinationFilePath ) )
    # Now remove the destination file
    removeFileRes = returnSingleResult( self.storageElement.removeFile( destinationFilePath ) )
    # Clean up the local mess
    os.remove( os.path.basename( destinationFilePath ) )

    # Check that the put was done correctly
    self.assert_( putFileRes['OK'] )
    self.assert_( putFileRes['Value'] )
    self.assertEqual( putFileRes['Value'], self.localFileSize )
    # Check that we got the file correctly
    self.assert_( getFileRes['OK'] )
    self.assertEqual( getFileRes['Value'], self.localFileSize )
    # Check that the removal was done correctly
    self.assert_( removeFileRes['OK'] )
    self.assert_( removeFileRes['Value'] )

  def test_getFileMetadata( self ):
    print '\n\n#########################################################################\n\n\t\t\tGet file metadata test\n'
    destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
    # pfnForLfnRes = returnSingleResult( self.storageElement.getURL( destinationFilePath ) )
    #destinationPfn = pfnForLfnRes['Value']
    fileDict = {destinationFilePath:self.localSourceFile}
    putFileRes = returnSingleResult( self.storageElement.putFile( fileDict ) )
    # Get the file metadata
    getFileMetadataRes = returnSingleResult( self.storageElement.getFileMetadata( destinationFilePath ) )
    # Now remove the destination file
    removeFileRes = returnSingleResult( self.storageElement.removeFile( destinationFilePath ) )
    # Get metadata for a removed file
    getMissingFileMetadataRes = returnSingleResult( self.storageElement.getFileMetadata( destinationFilePath ) )
    # Check directories are handled properly
    destinationDir = os.path.dirname( destinationFilePath )
    directoryMetadataRes = returnSingleResult( self.storageElement.getFileMetadata( destinationDir ) )

    # Check that the put was done correctly
    self.assert_( putFileRes['OK'] )
    self.assert_( putFileRes['Value'] )
    self.assertEqual( putFileRes['Value'], self.localFileSize )
    # Check that the metadata was done correctly
    self.assert_( getFileMetadataRes['OK'] )
    metadataDict = getFileMetadataRes['Value']

    # Works only for SRM2 plugin
    # self.assert_( metadataDict['Cached'] )
    # self.assertFalse( metadataDict['Migrated'] )
    self.assertEqual( metadataDict['Size'], self.localFileSize )
    # Check that the removal was done correctly
    self.assert_( removeFileRes['OK'] )
    self.assert_( removeFileRes['Value'] )
    # Check the get metadata for non existant file
    self.assertFalse( getMissingFileMetadataRes['OK'] )
    expectedError = "File does not exist"
    self.assert_( expectedError in getMissingFileMetadataRes['Message'] )
    # Check that metadata operation with a directory
    self.assertFalse( directoryMetadataRes['OK'] )
    expectedError = "Supplied path is not a file"
    self.assert_( expectedError in directoryMetadataRes['Message'] )

  def test_getFileSize( self ):
    print '\n\n#########################################################################\n\n\t\t\tGet file size test\n'
    destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
    # pfnForLfnRes = returnSingleResult( self.storageElement.getURL( destinationFilePath ) )
    #destinationPfn = pfnForLfnRes['Value']
    fileDict = {destinationFilePath:self.localSourceFile}
    putFileRes = returnSingleResult( self.storageElement.putFile( fileDict ) )
    # Get the file metadata
    getFileSizeRes = returnSingleResult( self.storageElement.getFileSize( destinationFilePath ) )
    # Now remove the destination file
    removeFileRes = returnSingleResult( self.storageElement.removeFile( destinationFilePath ) )
    # Get metadata for a removed file
    getMissingFileSizeRes = returnSingleResult( self.storageElement.getFileSize( destinationFilePath ) )
    # Check directories are handled properly
    destinationDir = os.path.dirname( destinationFilePath )
    directorySizeRes = returnSingleResult( self.storageElement.getFileSize( destinationDir ) )

    # Check that the put was done correctly
    self.assert_( putFileRes['OK'] )
    self.assert_( putFileRes['Value'] )
    self.assertEqual( putFileRes['Value'], self.localFileSize )
    # Check that the metadata was done correctly
    self.assert_( getFileSizeRes['OK'] )
    self.assertEqual( getFileSizeRes['Value'], self.localFileSize )
    # Check that the removal was done correctly
    self.assert_( removeFileRes['OK'] )
    self.assert_( removeFileRes['Value'] )
    # Check the get metadata for non existant file
    self.assertFalse( getMissingFileSizeRes['OK'] )
    expectedError = "File does not exist"
    self.assert_( expectedError in getMissingFileSizeRes['Message'] )
    # Check that metadata operation with a directory
    self.assertFalse( directorySizeRes['OK'] )
    expectedError = "Supplied path is not a file"
    self.assert_( expectedError in directorySizeRes['Message'] )

# Works only for SRM2 plugins
#   def test_prestageFile( self ):
#     print '\n\n#########################################################################\n\n\t\t\tPrestage file test\n'
#     destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
#     pfnForLfnRes = self.storageElement.getURL( destinationFilePath )
#     destinationPfn = pfnForLfnRes['Value']
#     fileDict = {destinationPfn:self.localSourceFile}
#     putFileRes = self.storageElement.putFile( fileDict, singleFile = True )
#     # Get the file metadata
#     prestageFileRes = self.storageElement.prestageFile( destinationPfn, singleFile = True )
#     # Now remove the destination file
#     removeFileRes = self.storageElement.removeFile( destinationPfn, singleFile = True )
#     # Get metadata for a removed file
#     missingPrestageFileRes = self.storageElement.prestageFile( destinationPfn, singleFile = True )
#
#     # Check that the put was done correctly
#     self.assert_( putFileRes['OK'] )
#     self.assert_( putFileRes['Value'] )
#     self.assertEqual( putFileRes['Value'], self.localFileSize )
#     # Check that the prestage was done correctly
#     self.assert_( prestageFileRes['OK'] )
#     self.assertEqual( type( prestageFileRes['Value'] ), types.StringType )
#     # Check that the removal was done correctly
#     self.assert_( removeFileRes['OK'] )
#     self.assert_( removeFileRes['Value'] )
#     # Check the prestage for non existant file
#     self.assertFalse( missingPrestageFileRes['OK'] )
#     expectedError = "No such file or directory"
#     self.assert_( expectedError in missingPrestageFileRes['Message'] )

# Works only for SRM2 plugins
#   def test_prestageStatus( self ):
#     print '\n\n#########################################################################\n\n\t\tPrestage status test\n'
#     destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
#     pfnForLfnRes = self.storageElement.getURL( destinationFilePath )
#     destinationPfn = pfnForLfnRes['Value']
#     fileDict = {destinationPfn:self.localSourceFile}
#     putFileRes = self.storageElement.putFile( fileDict, singleFile = True )
#     # Get the file metadata
#     prestageFileRes = self.storageElement.prestageFile( destinationPfn, singleFile = True )
#     srmID = ''
#     if prestageFileRes['OK']:
#       srmID = prestageFileRes['Value']
#     # Take a quick break to allow the SRM to realise the file is available
#     sleepTime = 10
#     print 'Sleeping for %s seconds' % sleepTime
#     time.sleep( sleepTime )
#     # Check that we can monitor the stage request
#     prestageStatusRes = self.storageElement.prestageFileStatus( {destinationPfn:srmID}, singleFile = True )
#     # Now remove the destination file
#     removeFileRes = self.storageElement.removeFile( destinationPfn, singleFile = True )
#
#     # Check that the put was done correctly
#     self.assert_( putFileRes['OK'] )
#     self.assert_( putFileRes['Value'] )
#     self.assertEqual( putFileRes['Value'], self.localFileSize )
#     # Check that the prestage was done correctly
#     self.assert_( prestageFileRes['OK'] )
#     self.assertEqual( type( prestageFileRes['Value'] ), types.StringType )
#     # Check the file was found to be staged
#     self.assert_( prestageStatusRes['OK'] )
#     self.assert_( prestageStatusRes['Value'] )
#     # Check that the removal was done correctly
#     self.assert_( removeFileRes['OK'] )
#     self.assert_( removeFileRes['Value'] )


# Works only for SRM2 plugins
#   def test_pinRelease( self ):
#     print '\n\n#########################################################################\n\n\t\tPin release test\n'
#     destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
#     pfnForLfnRes = self.storageElement.getURL( destinationFilePath )
#     destinationPfn = pfnForLfnRes['Value']
#     fileDict = {destinationPfn:self.localSourceFile}
#     putFileRes = self.storageElement.putFile( fileDict, singleFile = True )
#     # Get the file metadata
#     pinFileRes = self.storageElement.pinFile( destinationPfn, singleFile = True )
#     srmID = ''
#     if pinFileRes['OK']:
#       srmID = pinFileRes['Value']
#     # Check that we can release the file
#     releaseFileRes = self.storageElement.releaseFile( {destinationPfn:srmID}, singleFile = True )
#     # Now remove the destination file
#     removeFileRes = self.storageElement.removeFile( destinationPfn, singleFile = True )
#
#     # Check that the put was done correctly
#     self.assert_( putFileRes['OK'] )
#     self.assert_( putFileRes['Value'] )
#     self.assertEqual( putFileRes['Value'], self.localFileSize )
#     # Check that the file pin was done correctly
#     self.assert_( pinFileRes['OK'] )
#     self.assertEqual( type( pinFileRes['Value'] ), types.StringType )
#     # Check the file was found to be staged
#     self.assert_( releaseFileRes['OK'] )
#     self.assert_( releaseFileRes['Value'] )
#     # Check that the removal was done correctly
#     self.assert_( removeFileRes['OK'] )
#     self.assert_( removeFileRes['Value'] )

  def test_getURL( self ):
    print '\n\n#########################################################################\n\n\t\tGet access url test\n'
    destinationFilePath = '%s/testFile.%s' % ( self.destDirectory, time.time() )
    # pfnForLfnRes = returnSingleResult( self.storageElement.getURL( destinationFilePath ) )
    #destinationPfn = pfnForLfnRes['Value']
    fileDict = {destinationFilePath:self.localSourceFile}
    putFileRes = returnSingleResult( self.storageElement.putFile( fileDict ) )
    # Get a transfer url for the file
    getTurlRes = self.storageElement.getURL( destinationFilePath, protocol = 'dips' )
    # Remove the destination file
    removeFileRes = returnSingleResult( self.storageElement.removeFile( destinationFilePath ) )
    # Get missing turl res
    getMissingTurlRes = self.storageElement.getURL( destinationFilePath, protocol = 'dips' )

    # Check that the put was done correctly
    self.assert_( putFileRes['OK'] )
    self.assert_( putFileRes['Value'] )
    self.assertEqual( putFileRes['Value'], self.localFileSize )
    # Check that we can get the tURL properly
    self.assert_( getTurlRes['OK'] )
    self.assert_( getTurlRes['Value'] )
    self.assert_( type( getTurlRes['Value'] ) == types.DictType )
    self.assert_( type( getTurlRes['Value']['Successful'][destinationFilePath] ) in types.StringTypes )
    # Check that the removal was done correctly
    self.assert_( removeFileRes['OK'] )
    self.assert_( removeFileRes['Value'] )

    # Works only for SRM2 plugins
    # # Check that non-existant files are handled correctly
    # self.assertFalse( getMissingTurlRes['OK'] )
    # expectedError = "File does not exist"
    # self.assert_( expectedError in getMissingTurlRes['Message'] )

class DirectoryTestCases( StorageElementTestCase ):

  def test_createDirectory( self ):
    print '\n\n#########################################################################\n\n\t\t\tCreate directory test\n'
    directory = "%s/%s" % ( self.destDirectory, 'createDirectoryTest' )
    # pfnForLfnRes = returnSingleResult( self.storageElement.getURL( directory ) )
    #directoryPfn = pfnForLfnRes['Value']
    
    createDirRes = self.storageElement.createDirectory( directory )
    # Remove the target dir
    removeDirRes = self.storageElement.removeDirectory( directory, recursive = True )

    # Check that the creation was done correctly
    self.assert_( createDirRes['OK'] )
    self.assert_( createDirRes['Value'] )
    # Remove the directory
    self.assert_( removeDirRes['OK'] )
    self.assert_( removeDirRes['Value'] )

  def test_isDirectory( self ):
    print '\n\n#########################################################################\n\n\t\t\tIs directory test\n'
    destDirectory = self.destDirectory
    # Test that it is a directory
    isDirectoryRes = self.storageElement.isDirectory( destDirectory )
    # Test that no existant dirs are handled correctly
    nonExistantDir = "%s/%s" % ( destDirectory, 'NonExistant' )
    nonExistantDirRes = self.storageElement.isDirectory( nonExistantDir )

    # Check that it works with the existing dir
    self.assert_( isDirectoryRes['OK'] )
    self.assert_( isDirectoryRes['Value'] )
    # Check that we handle non existant correctly
    self.assert_( nonExistantDirRes['Value']['Failed'][nonExistantDir] in ['Path does not exist'] )

  def test_listDirectory( self ):
    print '\n\n#########################################################################\n\n\t\t\tList directory test\n'
    destDirectory = "%s/%s" % ( self.destDirectory, 'listDirectoryTest' )
    # destDirectory = returnSingleResult( self.storageElement.getURL( directory ) )['Value']
    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize( srcFile )
    if not os.path.exists( localDir ):
      os.mkdir( localDir )
    for i in range( self.numberOfFiles ):
      shutil.copy( srcFile, '%s/testFile.%s' % ( localDir, time.time() ) )
      time.sleep( 1 )
    # Check that we can successfully upload the directory to the storage element
    dirDict = {destDirectory:localDir}
    putDirRes = self.storageElement.putDirectory( dirDict )
    print putDirRes
    # List the remote directory
    listDirRes = self.storageElement.listDirectory( destDirectory )
    # Now remove the remove directory
    removeDirRes = self.storageElement.removeDirectory( destDirectory, recursive = True )
    print removeDirRes
    #Clean up the locally created directory
    shutil.rmtree( localDir )

    # Perform the checks for the put dir operation
    self.assert_( putDirRes['OK'] )
    self.assert_( putDirRes['Value'] )
    if putDirRes['Value']['Successful'][destDirectory]['Files']:
      self.assertEqual( putDirRes['Value']['Successful'][destDirectory]['Files'], self.numberOfFiles )
      self.assertEqual( putDirRes['Value']['Successful'][destDirectory]['Size'], self.numberOfFiles * sizeOfLocalFile )
    self.assert_( type( putDirRes['Value']['Successful'][destDirectory]['Files'] ) in [types.IntType, types.LongType] )
    self.assert_( type( putDirRes['Value']['Successful'][destDirectory]['Size'] ) in  [types.IntType, types.LongType] )
    # Perform the checks for the list dir operation
    self.assert_( listDirRes['OK'] )
    self.assert_( listDirRes['Value'] )
    self.assert_( listDirRes['Value']['Successful'][destDirectory].has_key( 'SubDirs' ) )
    self.assert_( listDirRes['Value']['Successful'][destDirectory].has_key( 'Files' ) )
    self.assertEqual( len( listDirRes['Value']['Successful'][destDirectory]['Files'].keys() ), self.numberOfFiles )
    # Perform the checks for the remove directory operation
    self.assert_( removeDirRes['OK'] )
    self.assert_( removeDirRes['Value'] )
    if removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved']:
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'], self.numberOfFiles )
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'], self.numberOfFiles * sizeOfLocalFile )
    self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'] ) in [types.IntType, types.LongType] )
    self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'] ) in [types.IntType, types.LongType] )

  def test_getDirectoryMetadata( self ):
    print '\n\n#########################################################################\n\n\t\t\tDirectory metadata test\n'
    destDirectory = "%s/%s" % ( self.destDirectory, 'getDirectoryMetadataTest' )
    # destDirectory = returnSingleResult( self.storageElement.getURL( directory ) )['Value']
    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize( srcFile )
    if not os.path.exists( localDir ):
      os.mkdir( localDir )
    for i in range( self.numberOfFiles ):
      shutil.copy( srcFile, '%s/testFile.%s' % ( localDir, time.time() ) )
      time.sleep( 1 )
    # Check that we can successfully upload the directory to the storage element
    dirDict = {destDirectory:localDir}
    putDirRes = self.storageElement.putDirectory( dirDict )
    # Get the directory metadata
    metadataDirRes = self.storageElement.getDirectoryMetadata( destDirectory )
    # Now remove the remove directory
    removeDirRes = self.storageElement.removeDirectory( destDirectory, recursive = True )
    #Clean up the locally created directory
    shutil.rmtree( localDir )

    # Perform the checks for the put dir operation
    self.assert_( putDirRes['OK'] )
    self.assert_( putDirRes['Value'] )
    if putDirRes['Value']['Successful'][destDirectory]['Files']:
      self.assertEqual( putDirRes['Value']['Successful'][destDirectory]['Files'], self.numberOfFiles )
      self.assertEqual( putDirRes['Value']['Successful'][destDirectory]['Size'], self.numberOfFiles * sizeOfLocalFile )
      self.assert_( type( putDirRes['Value']['Successful'][destDirectory]['Files'] ) in [types.IntType, types.LongType] )
      self.assert_( type( putDirRes['Value']['Successful'][destDirectory]['Size'] ) in  [types.IntType, types.LongType] )
    # Perform the checks for the list dir operation
    self.assert_( metadataDirRes['OK'] )
    self.assert_( metadataDirRes['Value'] )

    # Works only for the SRM2 plugin
    # self.assert_( metadataDirRes['Value']['Mode'] )
    # self.assert_( type( metadataDirRes['Value']['Mode'] ) == types.IntType )

    self.assert_( metadataDirRes['Value']['Successful'][destDirectory]['Exists'] )
    self.assertEqual( metadataDirRes['Value']['Successful'][destDirectory]['Type'], 'Directory' )
    # Perform the checks for the remove directory operation
    self.assert_( removeDirRes['OK'] )
    self.assert_( removeDirRes['Value'] )
    if removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved']:
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'], self.numberOfFiles )
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'], self.numberOfFiles * sizeOfLocalFile )
      self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'] ) in [types.IntType, types.LongType] )
      self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'] ) in [types.IntType, types.LongType] )

  def test_getDirectorySize( self ):
    print '\n\n#########################################################################\n\n\t\t\tGet directory size test\n'
    destDirectory = "%s/%s" % ( self.destDirectory, 'getDirectorySizeTest' )
    # destDirectory = returnSingleResult( self.storageElement.getURL( directory ) )['Value']
    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize( srcFile )
    if not os.path.exists( localDir ):
      os.mkdir( localDir )
    for i in range( self.numberOfFiles ):
      shutil.copy( srcFile, '%s/testFile.%s' % ( localDir, time.time() ) )
      time.sleep( 1 )
    # Check that we can successfully upload the directory to the storage element
    dirDict = {destDirectory:localDir}
    putDirRes = self.storageElement.putDirectory( dirDict )
    # Get the directory metadata
    getDirSizeRes = self.storageElement.getDirectorySize( destDirectory )
    # Now remove the remove directory
    removeDirRes = self.storageElement.removeDirectory( destDirectory, recursive = True )
    #Clean up the locally created directory
    shutil.rmtree( localDir )

    # Perform the checks for the put dir operation
    self.assert_( putDirRes['OK'] )
    self.assert_( putDirRes['Value'] )
    if putDirRes['Value']['Successful'][destDirectory]['Files']:
      self.assertEqual( putDirRes['Value']['Successful'][destDirectory]['Files'], self.numberOfFiles )
      self.assertEqual( putDirRes['Value']['Successful'][destDirectory]['Size'], self.numberOfFiles * sizeOfLocalFile )
      self.assert_( type( putDirRes['Value']['Successful'][destDirectory]['Files'] ) in [types.IntType, types.LongType] )
      self.assert_( type( putDirRes['Value']['Successful'][destDirectory]['Size'] ) in  [types.IntType, types.LongType] )
    # Perform the checks for the get dir size operation
    self.assert_( getDirSizeRes['OK'] )
    self.assert_( getDirSizeRes['Value'] )
    self.assertFalse( getDirSizeRes['Value']['Successful'][destDirectory]['SubDirs'] )
    self.assert_( type( getDirSizeRes['Value']['Successful'][destDirectory]['Files'] ) in [types.IntType, types.LongType] )
    self.assert_( type( getDirSizeRes['Value']['Successful'][destDirectory]['Size'] ) in [types.IntType, types.LongType] )
    # Perform the checks for the remove directory operation
    self.assert_( removeDirRes['OK'] )
    self.assert_( removeDirRes['Value'] )
    if removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved']:
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'], self.numberOfFiles )
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'], self.numberOfFiles * sizeOfLocalFile )
      self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'] ) in [types.IntType, types.LongType] )
      self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'] ) in [types.IntType, types.LongType] )

  def test_removeDirectory( self ):
    print '\n\n#########################################################################\n\n\t\t\tRemove directory test\n'
    destDirectory = "%s/%s" % ( self.destDirectory, 'removeDirectoryTest' )
    # destDirectory = returnSingleResult( self.storageElement.getURL( directory ) )['Value']
    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize( srcFile )
    if not os.path.exists( localDir ):
      os.mkdir( localDir )
    for i in range( self.numberOfFiles ):
      shutil.copy( srcFile, '%s/testFile.%s' % ( localDir, time.time() ) )
      time.sleep( 1 )
    # Check that we can successfully upload the directory to the storage element
    dirDict = {destDirectory:localDir}
    putDirRes = self.storageElement.putDirectory( dirDict )
    # Get the directory metadata
    # Now remove the remove directory
    removeDirRes = self.storageElement.removeDirectory( destDirectory, recursive = True )
    #Clean up the locally created directory
    shutil.rmtree( localDir )

    # Perform the checks for the put dir operation
    self.assert_( putDirRes['OK'] )
    self.assert_( putDirRes['Value'] )
    if putDirRes['Value']['Successful'][destDirectory]['Files']:
      self.assertEqual( putDirRes['Value']['Successful'][destDirectory]['Files'], self.numberOfFiles )
      self.assertEqual( putDirRes['Value']['Successful'][destDirectory]['Size'], self.numberOfFiles * sizeOfLocalFile )
      self.assert_( type( putDirRes['Value']['Successful'][destDirectory]['Files'] ) in [types.IntType, types.LongType] )
      self.assert_( type( putDirRes['Value']['Successful'][destDirectory]['Size'] ) in  [types.IntType, types.LongType] )
    # Perform the checks for the remove directory operation
    self.assert_( removeDirRes['OK'] )
    self.assert_( removeDirRes['Value'] )
    if removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved']:
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'], self.numberOfFiles )
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'], self.numberOfFiles * sizeOfLocalFile )
      self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'] ) in [types.IntType, types.LongType] )
      self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'] ) in [types.IntType, types.LongType] )

  def test_getDirectory( self ):
    print '\n\n#########################################################################\n\n\t\t\tGet directory test\n'
    destDirectory = "%s/%s" % ( self.destDirectory, 'getDirectoryTest' )
    # destDirectory = returnSingleResult( self.storageElement.getURL( directory ) )['Value']
    # Create a local directory to upload
    localDir = '/tmp/unit-test'
    srcFile = '/etc/group'
    sizeOfLocalFile = getSize( srcFile )
    if not os.path.exists( localDir ):
      os.mkdir( localDir )
    for i in range( self.numberOfFiles ):
      shutil.copy( srcFile, '%s/testFile.%s' % ( localDir, time.time() ) )
      time.sleep( 1 )
    # Check that we can successfully upload the directory to the storage element
    dirDict = {destDirectory:localDir}
    putDirRes = self.storageElement.putDirectory( dirDict )
    # Get the directory metadata
    #Clean up the locally created directory
    shutil.rmtree( localDir )
    getDirRes = self.storageElement.getDirectory( destDirectory, localPath = localDir )
    # Now remove the remove directory
    removeDirRes = self.storageElement.removeDirectory( destDirectory, recursive = True )
    #Clean up the locally created directory
    if os.path.exists( localDir ): 
      shutil.rmtree( localDir )

    # Perform the checks for the put dir operation
    self.assert_( putDirRes['OK'] )
    self.assert_( putDirRes['Value'] )
    for _dir in dirDict:
      if putDirRes['Value']['Successful'][_dir]['Files']:
        self.assertEqual( putDirRes['Value']['Successful'][_dir]['Files'], self.numberOfFiles )
        self.assertEqual( putDirRes['Value']['Successful'][_dir]['Size'], self.numberOfFiles * sizeOfLocalFile )
        self.assert_( type( putDirRes['Value']['Successful'][_dir]['Files'] ) in [types.IntType, types.LongType] )
        self.assert_( type( putDirRes['Value']['Successful'][_dir]['Size'] ) in  [types.IntType, types.LongType] )
    # Perform the checks for the get directory operation
    self.assert_( getDirRes['OK'] )
    self.assert_( getDirRes['Value'] )
    for _dir in dirDict:
      if getDirRes['Value']['Successful'][_dir]['Files']:
        self.assertEqual( getDirRes['Value']['Successful'][_dir]['Files'], self.numberOfFiles )
        self.assertEqual( getDirRes['Value']['Successful'][_dir]['Size'], self.numberOfFiles * sizeOfLocalFile )
        self.assert_( type( getDirRes['Value']['Successful'][_dir]['Files'] ) in [types.IntType, types.LongType] )
        self.assert_( type( getDirRes['Value']['Successful'][_dir]['Size'] ) in [types.IntType, types.LongType] )
    # Perform the checks for the remove directory operation
    self.assert_( removeDirRes['OK'] )
    self.assert_( removeDirRes['Value'] )
    if removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved']:
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'], self.numberOfFiles )
      self.assertEqual( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'], self.numberOfFiles * sizeOfLocalFile )
      self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['FilesRemoved'] ) in [types.IntType, types.LongType] )
      self.assert_( type( removeDirRes['Value']['Successful'][destDirectory]['SizeRemoved'] ) in [types.IntType, types.LongType] )


if __name__ == '__main__':
  gLogger.setLevel( "DEBUG" )
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( DirectoryTestCases )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FileTestCases ) )
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GetInfoTestCase))
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

