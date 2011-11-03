import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):

  def test_setArgs_ok( self ):

    self.pb.setArgs( ( '', ) )
    self.assertEquals( self.pb.args, ( '', ) )

  def test_setArgs_nok( self ):

    self.assertRaises( self._mockMods[ 'InvalidRes' ], self.pb.setArgs, ( 1, ) )

################################################################################

  def test_setCommand_ok( self ):

    self.pb.setCommand( )
    self.assertEquals( self.pb.command, None )
    self.pb.setCommand( 1 )
    self.assertEquals( self.pb.command, 1 )

################################################################################

  def test_setCommandName_ok( self ):

    self.pb.setCommandName( )
    self.assertEquals( self.pb.commandName, None )
    self.pb.setCommandName( 1 )
    self.assertEquals( self.pb.commandName, 1 )

################################################################################

  def test_setKnownInfo_ok( self ):

    self.pb.setKnownInfo( )
    self.assertEquals( self.pb.knownInfo, None )
    self.pb.setKnownInfo( 1 )
    self.assertEquals( self.pb.knownInfo, 1 )

################################################################################

  def test_setInfoName_ok( self ):

    self.pb.setInfoName( )
    self.assertEquals( self.pb.infoName, None )
    self.pb.setInfoName( 1 )
    self.assertEquals( self.pb.infoName, 1 )

################################################################################

  def test_evaluate_ok( self ):

    self.pb.knownInfo = None
    self.pb.infoName  = None

    res = self.pb.evaluate()
    self.assertEquals( res, {} )

    self.pb.knownInfo = { 'Result' : None }
    res = self.pb.evaluate()
    self.assertEquals( res, None )

    self.pb.infoName  = 'Result'
    res = self.pb.evaluate()
    self.assertEquals( res, None )

    self.pb.infoName  = 'Result1'
    self.assertRaises( self._mockMods[ 'RSSException' ], self.pb.evaluate, )
    #res = self.pb.evaluate()
    #self.assertEquals( res, None )
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
