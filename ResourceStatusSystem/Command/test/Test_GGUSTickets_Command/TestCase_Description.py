import unittest
import inspect

class TestCase_Description_GGUSTickets_Open( unittest.TestCase ):

  me = 'GGUSTickets_Open'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'GGUSTicketsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_GGUSTickets_Link( unittest.TestCase ):

  me = 'GGUSTickets_Link'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'GGUSTicketsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_GGUSTickets_Info( unittest.TestCase ):

  me = 'GGUSTickets_Info'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'GGUSTicketsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    