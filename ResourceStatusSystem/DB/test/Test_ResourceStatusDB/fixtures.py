import unittest, sys

from DIRAC.ResourceStatusSystem.DB.mock.DB                 import DB
from DIRAC.ResourceStatusSystem.Utilities.mock.MySQLMonkey import MySQLMonkey

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):    

    import DIRAC.ResourceStatusSystem.DB.ResourceStatusDB as mockedModule
    
    mockedModule.MySQLMonkey = MySQLMonkey

    self.db = mockedModule.ResourceStatusDB( DBin = DB() )
    
  def tearDown( self ):  
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.DB.ResourceStatusDB' ]