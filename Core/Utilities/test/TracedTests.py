########################################################################
# $HeadURL $
# File: TracedTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/08 15:21:32
########################################################################

""" :mod: TracedTests 
    =======================
 
    .. module: TracedTests
    :synopsis: Traced test cases
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Traced test cases
"""

__RCSID__ = "$Id $"

##
# @file TracedTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/08 15:21:44
# @brief Definition of TracedTests class.

## imports 
import unittest
## SUT
from DIRAC.Core.Utilities.Traced import Traced, TracedDict, TracedList

########################################################################
class TracedTests(unittest.TestCase):
  """
  .. class:: TracedTests
  
  """
  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    self.tracedDict = TracedDict( { 1 : 1 } )
    self.tracedList = TracedList( [ 1 ] )
    class TracedClass( object ):
      __metaclass__ = Traced 
      classArg = None
      def __init__( self ):
        instanceArg = None
    self.tracedClass = TracedClass()

  def testTarcedDict( self ):
    """ TracedDict tests """
    self.assertEqual( self.tracedDict.updated(), [] )
    ## update, not changing value
    self.tracedDict[1] = 1
    self.assertEqual( self.tracedDict.updated(), [] )
    ## update, changing value
    self.tracedDict[1] = 2
    self.assertEqual( self.tracedDict.updated(), [1] )
    ## set new
    self.tracedDict[2] = 2
    self.assertEqual( self.tracedDict.updated(), [ 1, 2 ] )
    ## update from diff dict
    self.tracedDict.update( { 3: 3 } )
    self.assertEqual( self.tracedDict.updated(), [ 1, 2, 3 ] )
    
  def testTracedList( self ):
    """ traced list  """
    self.assertEqual( self.tracedList.updated(), [] )
    ## no value change 
    self.tracedList[0] = 1
    self.assertEqual( self.tracedList.updated(), [] )
    ## value change
    self.tracedList[0] = 2
    self.assertEqual( self.tracedList.updated(), [0] )
    ## append
    self.tracedList.append( 1 )
    self.assertEqual( self.tracedList.updated(), [0, 1] )

  def testTracedClass( self ):
    """ traced class """
    self.assertEqual( self.tracedClass.updated(), [] )
    self.tracedClass.instanceArg = 1 
    self.assertEqual( self.tracedClass.updated(), [ "instanceArg" ] )
    self.tracedClass.classArg = 1 
    self.assertEqual( self.tracedClass.updated(), [ "instanceArg" , "classArg" ] )

## test execution
if __name__ == "__main__":
  TESTLOADER = unittest.TestLoader()
  SUITE = TESTLOADER.loadTestsFromTestCase( TracedTests )      
  unittest.TextTestRunner(verbosity=3).run( SUITE )
  


