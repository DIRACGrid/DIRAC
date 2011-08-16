from DIRAC.Core.Base import Script
Script.parseCommandLine()

import unittest,types,time
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

testUser  = 'testuser'
testGroup = 'testgroup'
testFile  = '/testdir/testfile'

class FileCatalogDBTestCase(unittest.TestCase):
  """ Base class for the FileCatalogDB test cases
  """
  def setUp(self):
    self.fc = FileCatalogClient()

class UserGroupCase(FileCatalogDBTestCase):

  def test_userOperations(self):

    result = self.fc.addUser( testUser )    
    self.assert_( result['OK'] )
    result = self.fc.getUsers()
    self.assert_( result['OK'] )
    if result['OK']:
      self.assert_( testUser in result['Value'] )

  def test_groupOperations(self):

    result = self.fc.addGroup( testGroup )    
    self.assert_( result['OK'] )
    result = self.fc.getGroups()
    self.assert_( result['OK'] )
    if result['OK']:
      self.assert_( testGroup in result['Value'] )

class DirectoryCase(FileCatalogDBTestCase):

  def test_directoryOperations(self):

    result = self.fc.createDirectory( '/' )
    self.assert_( result['OK'] )
    result = self.fc.changePathOwner( { '/': testUser } )
    self.assert_( result['OK'] )
    result = self.fc.changePathGroup(  {'/': testGroup } )
    self.assert_( result['OK'] )
    result = self.fc.isDirectory('/')
    self.assert_( result['OK'])

class FileCase(FileCatalogDBTestCase):

  def test_fileOperations(self):
    """
      this test requires the SE to be properly defined in the CS
    """ 
    from DIRAC import gConfig
    testSE = 'testSE'
    rssClient = ResourceStatusClient()
    result = rssClient.getStorageElementsList( 'Read' )
    #result = gConfig.getSections( '/Resources/StorageElements' )
    #if result['OK'] and result['Value']:
    #  testSE = result['Value'][0]
    if result['Ok']:
      testSE = result['Value'][ 0 ] 
      
    result = self.fc.addFile( { testFile: { 'PFN': 'testfile', 
                                         'SE': testSE , 
                                         'Size':0, 
                                         'GUID':0, 
                                         'Checksum':'0' } } )
    self.assert_( result['OK'] )
    if gConfig.getValue( '/Resources/StorageElements/%s/AccessProtocol.1/Host' % testSE, '' ):
      result = self.fc.getReplicas( testFile )
      self.assert_( result['OK'] )
      self.assert_( testFile in result['Value']['Successful'] )
      

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UserGroupCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryCase))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FileCase))

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)


