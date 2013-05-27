########################################################################
# $HeadURL $
# File: TypedListTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/19 12:16:39
########################################################################

""" :mod: TypedListTests 
    =======================
 
    .. module: TypedListTests
    :synopsis: test case for TypedList
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for TypedList
"""

__RCSID__ = "$Id $"

##
# @file TypedListTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/19 12:16:48
# @brief Definition of unittests for TypedList 

## imports 
import unittest
## SUT
from DIRAC.Core.Utilities.TypedList import TypedList, TDeque

class TestClass( object ):
  """ dummy class """
  pass

########################################################################
class TypedListTestCase(unittest.TestCase):
  """
  .. class:: TypedlistTestCase
  
  """
  def setUp( self ):
    """ test setup """
    self.numericTypes = ( int, long, float )
    self.floatType = float 
    self.testClassType = TestClass

  def test01ctor( self ):
    """ c'tor test """
    NumericList = TypedList( allowedTypes = self.numericTypes )
    FloatList = TypedList( allowedTypes = self.floatType )
    TestClassList = TypedList( allowedTypes = self.testClassType )

    self.assertEqual( isinstance( NumericList, TypedList ),  True  )
    self.assertEqual( isinstance( FloatList, TypedList ),  True)
    self.assertEqual( isinstance( TestClassList, TypedList ),  True)
    
    self.assertEqual( NumericList.allowedTypes() == self.numericTypes, True )
    self.assertEqual( FloatList.allowedTypes() == self.floatType, True )
    self.assertEqual( TestClassList.allowedTypes() == self.testClassType, True )

    self.assertRaises( TypeError, 
                       TypedList.__init__, 
                       (), 
                       { "allowedTypes" : (1,2,3) } )
    

  def test02_add_iadd_radd( self ):
    """ += +lvalue +rvalue """
    NumericList = TypedList( (1, 1.0, 1L), self.numericTypes )
    ## +=
    NumericList += [ 2, 2.0, 2L ]
    self.assertEqual( len(NumericList), 6 )
    self.assertEqual( NumericList, [1, 1.0, 1L, 2, 2.0, 2L ] )
    ## +lvalue
    lList = NumericList + [ 3, 3.0, 3L ]
    self.assertEqual( len(lList), 9 )
    self.assertEqual( lList, [1, 1.0, 1L, 2, 2.0, 2L, 3, 3.0, 3L ] )

    ## rvalue+
    rList = [ 0, 0.0, 0L ] + NumericList
    self.assertEqual( len(rList), 9 )
    self.assertEqual( rList, [ 0, 0.0, 0L, 1, 1.0, 1L, 2, 2.0, 2L ] )

  def test03_setitem_append_extend_insert( self ):
    pass


  def test_deque( self ):
    class A( object ):
      def __init__(self, i):
        self.i = i
      def __str__( self ):
        return str(self.i)

    d = TDeque( [A(1), A(2)],allowedTypes=(A,) )
    d.append(A(3))
  

## test execution
if __name__ == "__main__":
  TESTLOADER = unittest.TestLoader()
  SUITE = TESTLOADER.loadTestsFromTestCase( TypedListTestCase )      
  unittest.TextTestRunner(verbosity=3).run( SUITE )
