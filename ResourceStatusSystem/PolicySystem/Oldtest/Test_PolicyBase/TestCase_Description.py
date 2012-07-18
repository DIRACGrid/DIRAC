import unittest
import inspect

class TestCase_Description( unittest.TestCase ):

  def test_init_definition( self ):
    
    ins = inspect.getargspec( self.pb.__init__ )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_setArgs_definition( self ):
    
    ins = inspect.getargspec( self.pb.setArgs )   
    self.assertEqual( ins.args, [ 'self', 'argsIn' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
  def test_setCommand_definition( self ):
    
    ins = inspect.getargspec( self.pb.setCommand )   
    self.assertEqual( ins.args, [ 'self', 'commandIn' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_setCommandName_definition( self ):
    
    ins = inspect.getargspec( self.pb.setCommandName )   
    self.assertEqual( ins.args, [ 'self', 'commandNameIn' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_setKnownInfo_definition( self ):
    
    ins = inspect.getargspec( self.pb.setKnownInfo )   
    self.assertEqual( ins.args, [ 'self', 'knownInfoIn' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_setInfoName_definition( self ):
    
    ins = inspect.getargspec( self.pb.setInfoName )   
    self.assertEqual( ins.args, [ 'self', 'infoNameIn' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_evaluate_definition( self ):
    
    ins = inspect.getargspec( self.pb.evaluate )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   