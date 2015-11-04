""" Test class for plugins
"""

__RCSID__ = "$Id$"

# imports
import unittest

# sut
from DIRAC.Core.Utilities.Time import timeThis

class logClass(object):
  def __init__( self ):
    self._systemName = 'aSystemName'
    self._subName = 'sSubName'

@timeThis
def myMethod():
  print 'boh'

class myClass(object):
  def __init__( self ):
    self.log = logClass()

  @timeThis
  def myMethodInAClass( self ):
    print 'boh'

class myBetterClass( object ):
  def __init__( self ):
    self.log = logClass()
    self.log._subName = 'aSubName'

  @timeThis
  def myMethodInAClass( self ):
    print 'boh'

class myEvenBetterClass( object ):
  def __init__( self ):
    self.log = logClass()
    self.log._subName = 'aSubName'
    self.transString = 'this is a transString'

  @timeThis
  def myMethodInAClass( self ):
    print 'boh'


class TimeTestCase( unittest.TestCase ):
  """ Base class for the Agents test cases
  """
  def setUp( self ):
    pass

  def tearDown( self ):
    pass


class TimeSuccess( TimeTestCase ):

  def test_timeThis( self ):

    self.assertIsNone( myMethod() )
    self.assertIsNone( myClass().myMethodInAClass() )
    self.assertIsNone( myBetterClass().myMethodInAClass() )
    self.assertIsNone( myEvenBetterClass().myMethodInAClass() )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TimeTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TimeSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
