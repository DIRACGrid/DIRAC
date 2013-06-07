""" StateMachine unit tests

"""

import diracmock

SUT_PATH = 'DIRAC.ResourceStatusSystem.PolicySystem.StateMachine'

# Tests ........................................................................

class State_test( diracmock.DIRAC_TestCase ):
  """ State_test
  
  """
  
  sutPath = SUT_PATH
  
  def test_instantiate( self ):
    """ test_instantiate
    
    Simple check that ensures the class can be instantiated
    """
    
    obj = self.moduleTested.State( 1 )
    self.assertEqual( 'State', obj.__class__.__name__ )
    
  def test_constructor( self ):
    """ test_constructor
    
    Checks the constructor does not make any changes on the inputs
    """  

    obj = self.moduleTested.State( 100 )
    self.assertEqual( obj.level, 100 )
    self.assertEqual( obj.stateMap, list() )
    self.assertEqual( obj.default, None )
    
    obj = self.moduleTested.State( 0, [ 'StateName1', 'StateName2' ] )
    self.assertEqual( obj.level, 0 )
    self.assertEqual( obj.stateMap, [ 'StateName1', 'StateName2' ] )
    self.assertEqual( obj.default, None )
    
    obj = self.moduleTested.State( 0, [ 'StateName1', 'StateName2' ], defState = 'defStateName' )
    self.assertEqual( obj.level, 0 )
    self.assertEqual( obj.stateMap, [ 'StateName1', 'StateName2' ] )
    self.assertEqual( obj.default, 'defStateName' )

  def test_transitionRule( self ):
    """ test_transitionRule
    
    """

    obj = self.moduleTested.State( 100 )
    res = obj.transitionRule( 'nextState' )
    self.assertEquals( res, 'nextState' )
    
    obj = self.moduleTested.State( 100, defState = 'defStateName' )
    res = obj.transitionRule( 'defStateName' )
    self.assertEquals( res, 'defStateName' )
    res = obj.transitionRule( 'nextState' )
    self.assertEquals( res, 'defStateName' )
    
    obj = self.moduleTested.State( 0, [ 'StateName1', 'StateName2' ] )
    res = obj.transitionRule( 'nextState' )
    self.assertEquals( res, 'nextState' )
    res = obj.transitionRule( 'StateName1' )
    self.assertEquals( res, 'StateName1' )
    res = obj.transitionRule( 'StateName2' )
    self.assertEquals( res, 'StateName2' )
    
    obj = self.moduleTested.State( 0, [ 'StateName1', 'StateName2' ], defState = 'defStateName' )
    res = obj.transitionRule( 'defStateName' )
    self.assertEquals( res, 'defStateName' )
    res = obj.transitionRule( 'nextState' )
    self.assertEquals( res, 'defStateName' )
    res = obj.transitionRule( 'StateName1' )
    self.assertEquals( res, 'StateName1' )
    res = obj.transitionRule( 'StateName2' )
    self.assertEquals( res, 'StateName2' )
    
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF