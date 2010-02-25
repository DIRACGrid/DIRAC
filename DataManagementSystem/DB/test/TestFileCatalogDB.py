from DIRAC.Core.Base import Script
Script.parseCommandLine()

import unittest,types,time
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

testUser = 'testuser'
testGroup = 'testgroup'

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

    fname = '/lhcb/user/a/atsareg/first_test'
    result = self.fc.addFile({fname:{'PFN':'testfile', 'SE':'testSE', 'Size':0, 'GUID':0, 'Checksum':'0'}})
    self.assert_( result['OK'] )

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UserGroupCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryCase))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FileCase))

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)


