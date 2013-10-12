# TO-DO: to be moved to TestDIRAC

########################################################################
# File: DataLoggingClientTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/12 12:28:30
########################################################################

""" :mod: DataLoggingClientTests
    =======================

    .. module: DataLoggingClientTests
    :synopsis: unitests for DataLoggingClient
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unitests for DataLoggingClient
"""

# #
# @file DataLoggingClientTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/12 12:28:48
# @brief Definition of DataLoggingClientTests class.

# # imports
import unittest
# # from DIRAC
from DIRAC import gLogger
# # SUT
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

########################################################################
class DataLoggingClientTestCase( unittest.TestCase ):
  """
  .. class:: DataLoggingClientTests

  """
  def setUp( self ):
    """ c'tor

    :param self: self reference
    """
    gLogger.setLevel( "VERBOSE" )
    self.log = gLogger.getSubLogger( self.__class__.__name__ )

  def test( self ):
    """ test

    :param self: self reference
    """
    dlc = DataLoggingClient()
    self.assertEqual( isinstance( dlc, DataLoggingClient ), True )
    self.assertEqual( dlc.getServer(), "DataManagement/DataLogging" )
    self.assertEqual( dlc.timeout, 120 )
    ping = dlc.ping()
    self.assertEqual( ping["OK"], True )

# # test execution
if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( DataLoggingClientTestCase )
  unittest.TextTestRunner( verbosity = 2 ).run( SUITE )

