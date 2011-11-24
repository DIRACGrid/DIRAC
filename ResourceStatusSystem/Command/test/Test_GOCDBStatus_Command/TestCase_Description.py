import unittest
import inspect

class TestCase_Description_GOCDBStatus_Command( unittest.TestCase ):

  me = 'GOCDBStatus_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'GOCDBClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_DTCached_Command( unittest.TestCase ):

  me = 'DTCached_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceManagementClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_DTInfo_Cached_Command( unittest.TestCase ):

  me = 'DTInfo_Cached_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceManagementClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    