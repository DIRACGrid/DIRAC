from DIRAC.Core.Base import Script
Script.parseCommandLine()

import unittest,types,time
from DIRAC.DataManagementSystem.Client.FileCatalogClient import FileCatalogClient

class FileCatalogDBTestCase(unittest.TestCase):
  """ Base class for the FileCatalogDB test cases
  """
  def setUp(self):
    self.fc = FileCatalogClient()

class UserGroupCase(FileCatalogDBTestCase):

  def test_userOperations(self):

    user = 'atsareg'
    result = self.fc.addUser(user)    
    self.assert_( result['OK'])
    result = self.fc.getUsers()
    self.assert_( result['OK'])

  def test_groupOperations(self):

    result = self.fc.addGroup('lhcb')    
    self.assert_( result['OK'])
    result = self.fc.getGroups()
    self.assert_( result['OK'])

class DirectoryCase(FileCatalogDBTestCase):

  def test_directoryOperations(self):

    result = self.fc.createDirectory('/')
    self.assert_( result['OK'])
    result = self.fc.changePathOwner({'/':'atsareg'})
    self.assert_( result['OK'])
    result = self.fc.changePathGroup({'/':'lhcb'})
    self.assert_( result['OK'])
    result = self.fc.isDirectory('/')
    self.assert_( result['OK'])

class FileCase(FileCatalogDBTestCase):

  def test_fileOperations(self):

    fname = '/lhcb/user/a/atsareg/first_test'
    result = self.fc.addFile(fname)

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UserGroupCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryCase))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FileCase))

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)


