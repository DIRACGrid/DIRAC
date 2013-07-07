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
    self.assertEqual( res, 'nextState' )
    
    obj = self.moduleTested.State( 100, defState = 'defStateName' )
    res = obj.transitionRule( 'defStateName' )
    self.assertEqual( res, 'defStateName' )
    res = obj.transitionRule( 'nextState' )
    self.assertEqual( res, 'defStateName' )
    
    obj = self.moduleTested.State( 0, [ 'StateName1', 'StateName2' ] )
    for unknownState in [ 'nextState', '', 0, None ]:
      res = obj.transitionRule( unknownState )
      self.assertEqual( res, unknownState )
    res = obj.transitionRule( 'StateName1' )
    self.assertEqual( res, 'StateName1' )
    res = obj.transitionRule( 'StateName2' )
    self.assertEqual( res, 'StateName2' )
    
    obj = self.moduleTested.State( 0, [ 'StateName1', 'StateName2' ], defState = 'defStateName' )
    res = obj.transitionRule( 'defStateName' )
    self.assertEqual( res, 'defStateName' )
    res = obj.transitionRule( 'nextState' )
    self.assertEqual( res, 'defStateName' )
    res = obj.transitionRule( 'StateName1' )
    self.assertEqual( res, 'StateName1' )
    res = obj.transitionRule( 'StateName2' )
    self.assertEqual( res, 'StateName2' )


class StateMachine_test( diracmock.DIRAC_TestCase ):
  """ StateMachine_test
  
  """
  
  sutPath = SUT_PATH

  
  def test_instantiate( self ):
    """ test_instantiate
    
    Simple check that ensures the class can be instantiated
    """
    
    obj = self.moduleTested.StateMachine()
    self.assertEqual( 'StateMachine', obj.__class__.__name__ )


  def test_constructor( self ):
    """ test_constructor
  
    """  
 
    obj = self.moduleTested.StateMachine()
    self.assertEqual( obj.state, None )
    self.assertEqual( obj.states.keys(), [ 'Nirvana' ] )
    
    # We are not validating inputs, so it swallows anything.. will crash later.
    for newState in [ None, '', 1, 'Active' ]:
      obj = self.moduleTested.StateMachine( newState )
      self.assertEqual( obj.state, newState )
      self.assertEqual( obj.states.keys(), [ 'Nirvana' ] )
    
    
  def test_levelOfState( self ):
    """ test_levelOfState
    
    """   
    
    obj = self.moduleTested.StateMachine()
    res = obj.levelOfState( 'Nirvana' )
    self.assertEqual( res, 100 )
    
    for unknownState in [ None, -1, '', 'Unknown' ]:
      res = obj.levelOfState( unknownState )
      self.assertEqual( res, -1 )
      
      
  def test_setState( self ):
    """ test_setState
    
    """
    
    obj = self.moduleTested.StateMachine()
    res = obj.setState( None )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( obj.state, None )
    
    obj = self.moduleTested.StateMachine()
    res = obj.setState( 'Nirvana' )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( obj.state, 'Nirvana' )
    
    obj = self.moduleTested.StateMachine()
    res = obj.setState( 'Unknown' )
    self.assertEqual( res[ 'OK' ], False )
    self.assertEqual( obj.state, None )     
  
    
  def test_getStates( self ):
    """ test_getStates
    
    """
    
    obj = self.moduleTested.StateMachine()
    
    for states in [ obj.states, {}, { 'a': 'a', 'b' : 'b' } ]:
    
      obj.states = states
    
      res = obj.getStates()
      self.assertEqual( res, obj.states.keys() )   
    
  
  def test_getNextState( self ):
    """ test_getNextState
    
    """
    
    obj = self.moduleTested.StateMachine()
    
    res = obj.getNextState( 'Nirvana' )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], 'Nirvana' )
    
    res = obj.getNextState( 'UnknownState' )
    self.assertEqual( res[ 'OK' ], False )


class RSSMachine_test( diracmock.DIRAC_TestCase ):
  """ RSSMachine_test
  
  """

  sutPath = SUT_PATH

  
  def test_instantiate( self ):
    """ test_instantiate
    
    Simple check that ensures the class can be instantiated
    """
    
    obj = self.moduleTested.RSSMachine( None )
    self.assertEqual( 'RSSMachine', obj.__class__.__name__ )  


  def test_constructor( self ):
    """ test_constructor
    
    """
    
    for state in [ None, '', 'State', 'Active', 1 ]:    
      obj = self.moduleTested.RSSMachine( state )
      self.assertEqual( obj.state, state )
      self.assertEqual( set( obj.states.keys() ), set( [ 'Unknown', 'Active', 'Degraded', 
                                                         'Probing', 'Banned', 'Error' ] ) )
  
      
  def test_levelOfState( self ):
    """ test_levelOfState
    
    """
    
    obj = self.moduleTested.RSSMachine( None )
    
    for stateName, stateObj in obj.states.iteritems():
      res = obj.levelOfState( stateName )
      self.assertEqual( res, stateObj.level )   
    
    for unknownState in [ None, -1, '', 'NoIdea' ]:
      res = obj.levelOfState( unknownState )
      self.assertEqual( res, -1 )   
  
  
  def test_setState( self ):
    """ test_setState
    
    """         
    
    obj = self.moduleTested.RSSMachine( None )
    res = obj.setState( None )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( obj.state, None )
    
    # It does not change from previous
    res = obj.setState( 'SomethingElse' )
    self.assertEqual( res[ 'OK' ], False )
    self.assertEqual( obj.state, None )
    
    for stateName in obj.states.iterkeys():
      res = obj.setState( stateName )
      self.assertEqual( res[ 'OK' ], True )
      self.assertEqual( obj.state, stateName )   
    

  def test_getStates( self ):
    """ test_getStates
    
    """
    
    obj = self.moduleTested.RSSMachine( None )
    
    for states in [ obj.states, {}, { 'a': 'a', 'b' : 'b' } ]:
    
      obj.states = states
    
      res = obj.getStates()
      self.assertEqual( res, obj.states.keys() )     

  def test_getNextState( self ):
    """ test_getNextState
    
    """
    
    obj = self.moduleTested.RSSMachine( None )
    
    #state is None
    for nextState in obj.states.iterkeys():
      res = obj.getNextState( nextState )
      self.assertEqual( res[ 'OK' ], True  )
      self.assertEqual( res[ 'Value' ], nextState )
    
    # for each state, a new RSSMachine
    for rssState in obj.states.iterkeys():
      
      obj.setState( rssState )

      for nextState in obj.states.iterkeys():
      
        res = obj.getNextState( nextState )
        self.assertEqual( res[ 'OK' ], True )
        if ( nextState in obj.states[ rssState ].stateMap ) or ( obj.states[ rssState ].default is None ):
          self.assertEqual( res[ 'Value' ], nextState )
        else:
          self.assertEqual( res[ 'Value' ], obj.states[ rssState ].default )
      
  def test_orderPolicyResults( self ):
    """ test_orderPolicyResults
    
    """

    obj = self.moduleTested.RSSMachine( None )
    
    res = obj.orderPolicyResults( [] )
    self.assertEqual( res, None )
    
    policyList = [ { 'Status' : '1' } ]
    obj.orderPolicyResults( policyList )
    self.assertEqual( policyList, [ { 'Status' : '1' } ] )
    
    policyList = [ { 'Status' : '1' }, { 'Status' : 'Active' } ]
    res = obj.orderPolicyResults( policyList )
    self.assertEqual( policyList, [ { 'Status' : '1' }, { 'Status' : 'Active' } ] )
    
    policyList = [ { 'Status' : 'Error' }, { 'Status' : '1' },{ 'Status' : 'Active' } ]
    res = obj.orderPolicyResults( policyList )
    self.assertEqual( policyList, [ { 'Status' : '1' }, { 'Status' : 'Error' }, { 'Status' : 'Active' } ] )

          
  def test_levelOfPolicyState( self ):
    """ test_levelOfPolicyState
    
    """
    
    obj = self.moduleTested.RSSMachine( None )
    
    for policyDict in [ { 'Status' : 'Active' }, { 'Status' : 'Active', 'A' : 'A' },
                        { 'Status' : None }, { 'Status' : 1, 'Z' : 1} ]:
      
      res = obj.levelOfPolicyState( policyDict )
      self.assertEqual( res, obj.levelOfState( policyDict[ 'Status' ] ) )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF