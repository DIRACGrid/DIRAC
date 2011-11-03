import unittest
import inspect

class TestCase_Description( unittest.TestCase ):

  def test_init_definition( self ):
    
    ins = inspect.getargspec( self.pep.__init__ )   
    self.assertEqual( ins.args, [ 'self', 'VOExtension', 'pdp', 'nc', 'setup', 'da', 
                                  'csAPI', 'knownInfo', 'clients' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, {},) )

  def test_enforce_definition( self ):
    
    ins = inspect.getargspec( self.pep.enforce )   
    self.assertEqual( ins.args, [ 'self', 'granularity', 'name', 'statusType',
                                  'status', 'formerStatus', 'reason', 'siteType',
                                  'serviceType', 'resourceType', 'tokenOwner',
                                  'useNewRes', 'knownInfo' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, None, None, None, None, None, 
                                      None, None, None, None, False, None,) )


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   