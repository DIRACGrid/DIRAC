# imports
import unittest, mock, importlib
# sut
from DIRAC.Core.Utilities.checkCAOfUser import checkCAOfUser

class TestcheckCAOfUser( unittest.TestCase ):

  def setUp( self ):
    self.gConfigMock = mock.Mock()
    self.checkCAOfUser = importlib.import_module( 'DIRAC.Core.Utilities.checkCAOfUser' )
    self.checkCAOfUser.gConfig = self.gConfigMock

  def tearDown( self ):
    pass

class TestcheckCAOfUserSuccess(TestcheckCAOfUser):

  def test_success( self ):
    self.gConfigMock.getValue.return_value = 'attendedValue'
    res = checkCAOfUser( 'aUser', 'attendedValue' )
    self.assert_( res['OK'] )

  def test_failure( self ):
    self.gConfigMock.getValue.return_value = 'unAttendedValue'
    res = checkCAOfUser( 'aUser', 'attendedValue' )
    self.assertFalse( res['OK'] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestcheckCAOfUser )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestcheckCAOfUserSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )


