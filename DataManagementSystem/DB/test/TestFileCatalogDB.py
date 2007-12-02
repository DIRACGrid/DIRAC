import unittest,types,time
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB

class FileCatalogDBTestCase(unittest.TestCase):
  """ Base class for the FileCatalogDB test cases
  """
  def setUp(self):
    self.fcDB = FileCatalogDB()

class UserGroupCase(FileCatalogDBTestCase):

  def test_userOperations(self):

    user = 'atsareg'
    userDN = '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev'
    result = self.fcDB.addUser(user,userDN)
    self.assert_( result['OK'])
    result = self.fcDB.getUsers()
    self.assert_( result['OK'])

  def test_groupOperations(self):

    result = self.fcDB.addGroup('lhcb')
    self.assert_( result['OK'])
    result = self.fcDB.getGroups()
    self.assert_( result['OK'])

class DirectoryCase(FileCatalogDBTestCase):

  def test_directoryOperations(self):

    result = self.fcDB.mkdir('/')
    self.assert_( result['OK'])
    result = self.fcDB.setDirectoryOwner('/','atsareg')
    self.assert_( result['OK'])
    result = self.fcDB.setDirectoryGroup('/','lhcb')
    self.assert_( result['OK'])
    result = self.fcDB.getDirectory('/')
    self.assert_( result['OK'])

class FileCase(FileCatalogDBTestCase):

  def test_fileOperations(self):

    fname = '/lhcb/user/a/atsareg/first_test'
    result = self.fcDB.addFile(fname)

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UserGroupCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryCase))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(FileCase))

  testResult = unittest.TextTestRunner(verbosity=2).run(suite)


