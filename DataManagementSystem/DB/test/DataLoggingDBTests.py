########################################################################
# $HeadURL $
# File: DataLoggingDBTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/12 13:01:10
########################################################################

""" :mod: DataLoggingDBTests 
    =======================
 
    .. module: DataLoggingDBTests
    :synopsis: unittests for DataLoggingDB class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittests for DataLoggingDB class
"""

__RCSID__ = "$Id $"

##
# @file DataLoggingDBTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/12 13:01:33
# @brief Definition of DataLoggingDBTests class.

## imports 
import os
import sys
import unittest
## from DIRAC
from DIRAC import gLogger, gConfig
from DIRAC.Core.Utilities import Time
## SUT
from DIRAC.DataManagementSystem.DB.DataLoggingDB import DataLoggingDB

########################################################################
class DataLoggingDBTestCase(unittest.TestCase):
  """
  .. class:: DataLoggingDBTests
  
  """
  ## db ref
  __db = None

  def setUp( self ):
    """ set up

    :param self: self reference
    """
    self.log = gLogger.getSubLogger( self.__class__.__name__ )

    if not self.__db:
      gConfig.setOptionValue( "/DIRAC/Setup", "Test" )
      gConfig.setOptionValue( "/DIRAC/Setups/Test/DataManagement", "Test" )
      spath = "/Systems/DataManagement/Test/Databases/DataLoggingDB" 
      gConfig.setOptionValue( "%s/%s" % ( spath, "Host" ), "127.0.0.1" )
      gConfig.setOptionValue( "%s/%s" % ( spath, "DBName" ), "AccountingDB" )
      gConfig.setOptionValue( "%s/%s" % ( spath, "User" ), "Dirac" )
      gConfig.setOptionValue( "%s/%s" % ( spath, "Password" ), "Dirac" )
      self.__db = DataLoggingDB()

  def test_01_ctor( self ):
    """ DataLoggingDB.__init__

    :param self: self reference
    """
    self.assertEqual( self.__db != None, True  )
    self.assertEqual( isinstance( self.__db, DataLoggingDB ), True )
    self.assertEqual( self.__db._connect()["OK"], True )

  def test_02_createTable( self ):
    """ DataLoggingDB._createTable

    :param self: self reference
    """
    self.assertEqual( self.__db._createTable()["OK"], True )
   
  def test_03_api( self ):
    """ DataLoggingDB API

    :param self: self reference
    """
    
    lfns = [ '/Test/00001234/File1', '/Test/00001234/File2' ] 
    fileTuples = tuple( [ ( lfn, "TestStatus", "MinorStatus", Time.toString(), Time.dateTime(), "Somewhere" ) 
                          for lfn in lfns ] )
    
    result = self.__db.addFileRecord( lfns, "TestStatus", date = '2012-04-28 09:49:02.545466' )
    self.assertEqual( result["OK"], True ) 
    self.assertEqual( result["Value"], 2 )
    self.assertEqual( result["lastRowId"], 2 )

    result = self.__db.addFileRecords( fileTuples )
    self.assertEqual( result["OK"], True )

    result = self.__db.getFileLoggingInfo( lfns[0] )
    self.assertEqual( result["OK"], True )
    self.assertEqual( len( result["Value"] ), 2 )

    result = self.__db.getFileLoggingInfo( lfns[1] )
    self.assertEqual( result["OK"], True )
    self.assertEqual( len( result["Value"] ), 2 )

    result = self.__db.getUniqueStates()
    self.assertEqual( result["OK"], True )
    self.assertEqual( result["Value"], [ "TestStatus" ] )

    result = self.__db._update( 'DROP TABLE `%s`' % self.__db.tableName )
    self.assertEqual( result["OK"], True )


## test execution
if __name__ == "__main__":
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  gLogger.setLevel( 'VERBOSE' )

  if 'PYTHONOPTIMIZE' in os.environ and os.environ['PYTHONOPTIMIZE']:
    gLogger.info( 'Unset python optimization "PYTHONOPTIMIZE"' )
    sys.exit( 0 )

  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( DataLoggingDBTestCase )
  unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
