#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest, os, shutil

from TestDIRAC.Utilities.utils import find_all

from TestDIRAC.Utilities.IntegrationTest import IntegrationTest

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

class RegressionTestCase( IntegrationTest ):
  """ Base class for the Regression test cases
  """
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    self.dirac = Dirac()

    exeScriptLoc = find_all( 'exe-script.py', '.', 'Regression' )[0]
    helloWorldLoc = find_all( 'helloWorld.py', '.', 'Regression' )[0]

    shutil.copyfile( exeScriptLoc, './exe-script.py' )
    shutil.copyfile( helloWorldLoc, './helloWorld.py' )

    helloWorldXMLLocation = find_all( 'helloWorld.xml', '.', 'Regression' )[0]
    self.j_u_hello = Job( helloWorldXMLLocation )

    helloWorldXMLFewMoreLocation = find_all( 'helloWorld.xml', '.', 'Regression' )[0]
    self.j_u_helloPlus = Job( helloWorldXMLFewMoreLocation )

  def tearDown( self ):
    os.remove( 'exe-script.py' )
    os.remove( 'helloWorld.py' )

class HelloWorldSuccess( RegressionTestCase ):
  def test_Regression_User( self ):
    res = self.j_u_hello.runLocal( self.dirac )
    self.assertTrue( res['OK'] )

class HelloWorldPlusSuccess( RegressionTestCase ):
  def test_Regression_User( self ):
    res = self.j_u_helloPlus.runLocal( self.dirac )
    self.assertTrue( res['OK'] )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RegressionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldPlusSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
