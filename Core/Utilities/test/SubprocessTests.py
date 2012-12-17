########################################################################
# $HeadURL $
# File: SubprocessTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/11 18:04:25
########################################################################

""" :mod: SubprocessTests 
    =======================
 
    .. module: SubprocessTests
    :synopsis: unittest for Subprocess module
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for Subprocess module
"""

__RCSID__ = "$Id $"

##
# @file SubprocessTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/11 18:04:37
# @brief Definition of SubprocessTests class.

## imports 
import unittest
import time
## SUT
from DIRAC.Core.Utilities.Subprocess import systemCall, shellCall, pythonCall

########################################################################
class SubprocessTests(unittest.TestCase):
  """
  .. class:: SubprocessTests
  
  """

  def setUp( self ):
    """ test case setup

    :param self: self reference
    """
    self.cmd = [ "sleep", "10" ]
    self.timeout=3

  def testNoTimeouts( self ):
    """ tests no timeouts  """

    ## systemCall
    ret = systemCall( timeout=False, cmdSeq = self.cmd )
    self.assertEqual( ret, {'OK': True, 'Value': (0, '', '') } )
    
    ## shellCall
    ret  = shellCall( timeout=False, cmdSeq = " ".join( self.cmd ) )
    self.assertEqual( ret, {'OK': True, 'Value': (0, '', '') } )

    def pyfunc( name ):
      time.sleep(10)
      return name

    ## pythonCall
    ret = pythonCall( 0, pyfunc, "Krzysztof" )
    self.assertEqual( ret, {'OK': True, 'Value': 'Krzysztof'} )

  def testTimeouts( self ):
    """ test timeouts """
    
    ## systemCall
    ret = systemCall( timeout=self.timeout, cmdSeq = self.cmd )
    self.assertEqual( ret, {'Message': 'Timed out after 3 seconds', 'OK': False} )
    
    ## shellCall
    ret  = shellCall( timeout=self.timeout, cmdSeq = " ".join( self.cmd ) )
    self.assertEqual( ret, {'Message': 'Timed out after 3 seconds', 'OK': False} )

    def pyfunc( name ):
      time.sleep(10)
      return name

    ## pythonCall
    ret = pythonCall( self.timeout, pyfunc, "Krzysztof" )
    self.assertEqual( ret, {'Message': 'Timed out after 3 seconds', 'OK': False} )

## tests execution
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gTestSuite = gTestLoader.loadTestsFromTestCase( SubprocessTests )      
  unittest.TextTestRunner( verbosity=3 ).run( gTestSuite )
