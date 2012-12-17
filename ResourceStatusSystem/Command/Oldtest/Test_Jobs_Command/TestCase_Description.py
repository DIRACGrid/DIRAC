import unittest
import inspect

class TestCase_Description_JobsStats_Command( unittest.TestCase ):

  me = 'JobsStats_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'JobsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_JobsEff_Command( unittest.TestCase ):

  me = 'JobsEff_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'JobsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################
    
class TestCase_Description_SystemCharge_Command( unittest.TestCase ):

  me = 'SystemCharge_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'JobsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################    
    
class TestCase_Description_JobsEffSimple_Command( unittest.TestCase ):

  me = 'JobsEffSimple_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'JobsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################    
    
class TestCase_Description_JobsEffSimpleCached_Command( unittest.TestCase ):

  me = 'JobsEffSimpleCached_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )    
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    