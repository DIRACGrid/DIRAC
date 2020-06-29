""" Test the FilenamePlugin class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from DIRAC.Resources.Catalog.ConditionPlugins.FilenamePlugin import FilenamePlugin

__RCSID__ = "$Id $"

class TestfilenamePlugin( unittest.TestCase ):
  """ Test the FilenamePlugin class"""

  def setUp(self):
    self.lfns = [ '/lhcb/lfn1', '/lhcb/anotherlfn', '/otherVo/name']

  def test_01_endswith(self):
    """ Testing endswith (method with argument"""

    fnp = FilenamePlugin("endswith('n')")

    self.assertTrue( not fnp.eval( lfn = '/lhcb/lfn1' ) )
    self.assertTrue( fnp.eval( lfn = '/lhcb/lfn' ) )

  def test_02_find( self ):
    """ Testing special case of find"""

    fnp = FilenamePlugin( "find('lfn')" )

    self.assertTrue( fnp.eval( lfn = '/lhcb/lfn1' ) )
    self.assertTrue( not fnp.eval( lfn = '/lhcb/l0f0n' ) )


  def test_03_isalnum( self ):
    """ Testing isalnum (method without argument"""

    fnp = FilenamePlugin( "isalnum()" )

    self.assertTrue( fnp.eval( lfn = 'lhcblfn1' ) )
    self.assertTrue( not fnp.eval( lfn = '/lhcb/lf_n' ) )

  def test_04_nonExisting( self ):
    """ Testing non existing string method"""

    fnp = FilenamePlugin( "nonexisting()" )

    self.assertTrue( not fnp.eval( lfn = 'lhcblfn1' ) )
    self.assertTrue( not fnp.eval( lfn = '/lhcb/lf_n' ) )




if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestfilenamePlugin )

  unittest.TextTestRunner( verbosity = 2 ).run( suite )
