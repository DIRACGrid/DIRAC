import unittest
import inspect

class TestCase_Description( unittest.TestCase ):

  def test_init_definition( self ):
    
    ins = inspect.getargspec( self.pdp.__init__ )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, 'clients' )
    self.assertEqual( ins.defaults, None )

  def test_setup_definition( self ):
    
    ins = inspect.getargspec( self.pdp.setup )   
    self.assertEqual( ins.args, [ 'self', 'VOExtension', 'granularity', 'name', 
                                  'statusType', 'status', 'formerStatus', 
                                  'reason', 'siteType', 'serviceType', 
                                  'resourceType', 'useNewRes' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None,None,None,None,None,None,None,None,None,False ) )

  def test_takeDecision_definition( self ):
    
    ins = inspect.getargspec( self.pdp.takeDecision )   
    self.assertEqual( ins.args, [ 'self', 'policyIn', 'argsIn', 'knownInfo' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, None, None, ) )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   