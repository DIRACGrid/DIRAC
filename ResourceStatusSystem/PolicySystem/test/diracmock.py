# $HeadURL: $
""" diracmock

  This module must be imported before running any unit tests. It provides all the
  necessary mocks.

"""

#import mock
import unittest

__RCSID__ = '$Id: $'

# The following is the check to make sure it is in place
#if mock.__version__ < '1.0.1':
#  raise ImportError( 'Too old version of mock, we need %s < 1.0.1' % mock.__version__ )

def sut( sutPath ):
  """ sut ( Software Under Test )
  
  Imports the module ( not the class ! ) to be tested and returns it.

  examples:
    >>> sut( 'DIRAC.ResourceStatusSytem.Client.ResourceStatusClient' )

  :Parameters:
    **sutPath** - `str`
      path to the module to be tested    

  """
  
  return __import__( sutPath, globals(), locals(), '*' ) 


#...............................................................................
# Standard TestCase


class DIRAC_TestCase( unittest.TestCase ):
  
  sutPath = ''
  
  def setUp( self ):
    
    self.moduleTested = sut( self.sutPath )
  
  def tearDown( self ):
    
    del self.moduleTested    

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF