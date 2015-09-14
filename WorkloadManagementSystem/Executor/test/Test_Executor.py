""" Unit Tests for Executors
"""

import unittest

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.Executor.JobScheduling import JobScheduling

class ExecutorTestCase( unittest.TestCase ):
  """ Base class for the Modules test cases
  """
  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )

  def tearDown( self ):
    pass

class JobSchedulingSuccess( ExecutorTestCase ):

  def test__applySiteFilter( self ):
    js = JobScheduling()

    sites = ['MY.Site1.org', 'MY.Site2.org']
    filtered = js._applySiteFilter( sites )
    self.assertEqual( set( filtered ), set( sites ) )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = []
    banned = []
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( sites ) )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = ['MY.Site1.org', 'MY.Site2.org']
    banned = []
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( sites ) )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = []
    banned = ['MY.Site1.org', 'MY.Site2.org']
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( filtered, [] )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = []
    banned = ['MY.Site2.org']
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( filtered, ['MY.Site1.org'] )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = []
    banned = []
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( sites ) )

    sites = []
    active = []
    banned = ['MY.Site1.org']
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( sites ) )

    sites = []
    active = ['MY.Site1.org']
    banned = []
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( sites ) )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = ['MY.Site3.org']
    banned = []
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( sites ) )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = ['MY.Site3.org', 'MY.Site1.org']
    banned = []
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( sites ) )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = ['MY.Site3.org', 'MY.Site1.org', 'MY.Site2.org']
    banned = []
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( sites ) )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = ['MY.Site3.org']
    banned = ['MY.Site1.org']
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( ['MY.Site2.org'] ) )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = ['MY.Site3.org']
    banned = ['MY.Site1.org', 'MY.Site3.org']
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( ['MY.Site2.org'] ) )

    sites = []
    active = ['MY.Site3.org']
    banned = ['MY.Site1.org', 'MY.Site3.org']
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set() )

    sites = ['MY.Site1.org', 'MY.Site2.org']
    active = []
    banned = ['MY.Site1.org', 'MY.Site3.org']
    filtered = js._applySiteFilter( sites, active, banned )
    self.assertEqual( set( filtered ), set( ['MY.Site2.org'] ) )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ExecutorTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( JobSchedulingSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

