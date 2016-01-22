import unittest, importlib
from mock import Mock

from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform, getCompatiblePlatforms

class HelpersTestCase( unittest.TestCase ):
  """ Base class for the Helpers test cases
  """
  def setUp( self ):
    self.gConfigMock = Mock()
    self.resourcesHelper = importlib.import_module( 'DIRAC.ConfigurationSystem.Client.Helpers.Resources' )
    self.resourcesHelper.gConfig = self.gConfigMock

  def tearDown( self ):

    del self.resourcesHelper


class ResourcesSuccess( HelpersTestCase ):

  def test_getDIRACPlatform( self ):
    self.gConfigMock.getOptionsDict.return_value = {'OK':False, 'Value':''}
    res = getDIRACPlatform( 'plat' )
    self.assertFalse( res['OK'] )

    self.gConfigMock.getOptionsDict.return_value = {'OK':True, 'Value':''}
    res = getDIRACPlatform( 'plat' )
    self.assertFalse( res['OK'] )

    self.gConfigMock.getOptionsDict.return_value = {'OK':True, 'Value':{'plat1': 'OS1, OS2,  OS3',
                                                                        'plat2': 'OS4, OS5',
                                                                        'plat3': 'OS1, OS4'}}
    res = getDIRACPlatform( 'plat' )
    self.assertFalse( res['OK'] )

    res = getDIRACPlatform( 'OS1' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], ['plat3', 'plat1'] )

    res = getDIRACPlatform( 'OS2' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], ['plat1'] )

    res = getDIRACPlatform( 'OS3' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], ['plat1'] )

    res = getDIRACPlatform( 'OS4' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], ['plat3', 'plat2'] )

    res = getDIRACPlatform( 'OS5' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], ['plat2'] )

    res = getDIRACPlatform( 'plat1' )
    self.assertTrue( res['OK'] )

  def test_getCompatiblePlatforms( self ):
    self.gConfigMock.getOptionsDict.return_value = {'OK':False, 'Value':''}
    res = getCompatiblePlatforms( 'plat' )
    self.assertFalse( res['OK'] )

    self.gConfigMock.getOptionsDict.return_value = {'OK':True, 'Value':''}
    res = getCompatiblePlatforms( 'plat' )
    self.assertFalse( res['OK'] )

    self.gConfigMock.getOptionsDict.return_value = {'OK':True, 'Value':{'plat1': 'xOS1, xOS2,  xOS3',
                                                                        'plat2': 'sys2, xOS4, xOS5',
                                                                        'plat3': 'sys1, xOS1, xOS4'}}
    res = getCompatiblePlatforms( 'plat' )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value'], ['plat'] )

    res = getCompatiblePlatforms( 'plat1' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], ['plat1', 'xOS1', 'xOS2', 'xOS3'] )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( HelpersTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ResourcesSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
