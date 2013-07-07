########################################################################
# $HeadURL $
# File: StateMachineTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/07/03 15:17:23
########################################################################
""" :mod: StateMachineTests 
    =======================
 
    .. module: StateMachineTests
    :synopsis: unittest for StateMachine
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for StateMachine
"""
__RCSID__ = "$Id: $"
##
# @file StateMachineTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/07/03 15:17:33
# @brief Definition of StateMachineTests class.

## imports 
import unittest
## SUT
from DIRAC.Core.Utilities.StateMachine import State, StateMachine

########################################################################
class StateMachineTests(unittest.TestCase):
  """
  .. class:: StateMachineTests
  test case for state machine
  """

  def setUp( self ):
    """ test setup """
    
    class Waiting( State ):
      pass
    class Done( State ):
      pass
    class Failed( State ):
      pass

    def toDone( slist ):
      return list(set( slist )) == [ "Done" ]
    def toFailed( slist ):
      return "Failed" in slist
    def toWaiting( slist ):
      for st in slist:
        if st == "Done":
          continue
        if st == "Failed":
          return False
        if st == "Waiting":
          return True
      return False

    self.waiting = Waiting()
    self.done = Done()
    self.failed = Failed()


  def tearDown( self ):
    """ test case tear down """
    del self.waiting
    del self.done
    del self.failed
    

  def test01Ctor( self ):
    """ ctor tests """
    try:
      sm = StateMachine()
    except Exception, error:
      self.assertEqual( type(error), TypeError, "wrong exception" )
    

    sm = StateMachine( False )
      
      
    sm = StateMachine( self.waiting, {} )



# # test execution
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gSuite = gTestLoader.loadTestsFromTestCase( StateMachineTests )
  gSuite = unittest.TestSuite( [ gSuite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( gSuite )

  
