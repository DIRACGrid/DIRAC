import unittest, sys

from DIRAC.ResourceStatusSystem.DB.mock.DB                 import DB
from DIRAC.ResourceStatusSystem.Utilities.mock.MySQLMonkey import MySQLMonkey

class DescriptionFixture( unittest.TestCase ):
  
  def setUp( self ):   

    import DIRAC.ResourceStatusSystem.DB.ResourceManagementDB as mockedModule
  
    mockedModule.MySQLMonkey = MySQLMonkey
    
    self.db = mockedModule.ResourceManagementDB( DBin = DB() )
    
  def tearDown( self ):  
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.DB.ResourceManagementDB' ]