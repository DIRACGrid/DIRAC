""" Test the FilenamePlugin class"""

__RCSID__ = "$Id $"

import unittest

from DIRAC.Resources.Catalog.ConditionPlugins.FilenamePlugin import FilenamePlugin

          
class TestfilenamePlugin( unittest.TestCase ):
  """ Test the FilenamePlugin class"""
  
  def setUp(self):
    self.lfns = [ '/lhcb/lfn1', '/lhcb/anotherlfn', '/otherVo/name']
  
  def test_01_endswith(self):
    """ Testing endswith (method with argument"""
    
    fnp = FilenamePlugin("endswith('n')")
    
    self.assert_( not fnp.eval( lfn = '/lhcb/lfn1' ) )
    self.assert_( fnp.eval( lfn = '/lhcb/lfn' ) )
    
  def test_02_find( self ):
    """ Testing special case of find"""

    fnp = FilenamePlugin( "find('lfn')" )

    self.assert_( fnp.eval( lfn = '/lhcb/lfn1' ) )
    self.assert_( not fnp.eval( lfn = '/lhcb/l0f0n' ) )


  def test_03_isalnum( self ):
    """ Testing isalnum (method without argument"""

    fnp = FilenamePlugin( "isalnum()" )

    self.assert_( fnp.eval( lfn = 'lhcblfn1' ) )
    self.assert_( not fnp.eval( lfn = '/lhcb/lf_n' ) )

  def test_04_nonExisting( self ):
    """ Testing non existing string method"""

    fnp = FilenamePlugin( "nonexisting()" )

    self.assert_( not fnp.eval( lfn = 'lhcblfn1' ) )
    self.assert_( not fnp.eval( lfn = '/lhcb/lf_n' ) )




if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestfilenamePlugin )

  unittest.TextTestRunner( verbosity = 2 ).run( suite )
