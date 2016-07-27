#!/bin/env python
"""
Tests for Bdii2CSAgent module
"""

import unittest
from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Agent import Bdii2CSAgent

MODNAME= "DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent"

class Bdii2CSTests( unittest.TestCase ):

  def setUp( self ):
    with patch( "DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent.AgentModule.__init__", new=Mock() ):
      self.agent = Bdii2CSAgent.Bdii2CSAgent( agentName="Configuration/testing", loadName="Configuration/testing" )

      ## as we ignore the init from the baseclass some agent variables might not be present so we set them here
      ## in any case with this we can check that log is called with proper error messages
      self.agent.log = Mock()


  def tearDown( self ):
    pass


  def test__getBdiiCEInfo_success( self ):

    bdiiInfo1 = {'site1': {'CEs': { 'ce1': { 'Queues': { 'queue1': "SomeValues" }}}}}
    bdiiInfo2 = {'site2': {'CEs': { 'ce2': { 'Queues': { 'queue2': "SomeValues" }}}}}
    expectedResult = {}
    expectedResult.update(bdiiInfo1)
    expectedResult.update(bdiiInfo2)

    self.agent.alternativeBDIIs = [ "server2" ]
    with patch( MODNAME+".getBdiiCEInfo",
                new=Mock( side_effect=[
                  S_OK( bdiiInfo1 ),
                  S_OK( bdiiInfo2 ),
                ] )
              ) as infoMock:
      ret = self.agent._Bdii2CSAgent__getBdiiCEInfo( "vo" ) #pylint: disable=no-member
      infoMock.assert_any_call( "vo")
      infoMock.assert_any_call( "vo", host="server2" )
    self.assertTrue( ret['OK'] )
    self.assertEqual( expectedResult, ret['Value'] )


  def test__getBdiiCEInfo_fail_10( self ):

    bdiiInfo2 = {'site2': {'CEs': { 'ce2': { 'Queues': { 'queue2': "SomeValues" }}}}}

    self.agent.alternativeBDIIs = [ "server2" ]
    with patch( MODNAME+".getBdiiCEInfo",
                new=Mock( side_effect=[
                  S_ERROR( "error" ),
                  S_OK( bdiiInfo2 ),
                ] )
              ) as infoMock:
      ret = self.agent._Bdii2CSAgent__getBdiiCEInfo( "vo" ) #pylint: disable=no-member
      infoMock.assert_any_call( "vo")
      infoMock.assert_any_call( "vo", host="server2" )
      self.assertTrue( any ( "Failed getting information from default" in str(args) \
                             for args in self.agent.log.error.call_args_list ),
                       self.agent.log.error.call_args_list )
    self.assertTrue( ret['OK'] )
    self.assertEqual( bdiiInfo2, ret['Value'] )

  def test__getBdiiCEInfo_fail_01( self ):

    bdiiInfo1 = {'site1': {'CEs': { 'ce1': { 'Queues': { 'queue1': "SomeValues" }}}}}

    self.agent.alternativeBDIIs = [ "server2" ]
    with patch( MODNAME+".getBdiiCEInfo",
                new=Mock( side_effect=[
                  S_OK( bdiiInfo1 ),
                  S_ERROR( "error" ),
                ] )
              ) as infoMock:
      ret = self.agent._Bdii2CSAgent__getBdiiCEInfo( "vo" ) #pylint: disable=no-member
      infoMock.assert_any_call( "vo")
      infoMock.assert_any_call( "vo", host="server2" )
      self.assertTrue( any ( "Failed getting information from server2" in str(args) \
                             for args in self.agent.log.error.call_args_list ),
                       self.agent.log.error.call_args_list )
    self.assertTrue( ret['OK'] )
    self.assertEqual( bdiiInfo1, ret['Value'] )


  def test__getBdiiCEInfo_fail_11( self ):

    self.agent.alternativeBDIIs = [ "server2" ]
    with patch( MODNAME+".getBdiiCEInfo",
                new=Mock( side_effect=[
                  S_ERROR( "error1" ),
                  S_ERROR( "error2" ),
                ] )
              ) as infoMock:
      ret = self.agent._Bdii2CSAgent__getBdiiCEInfo( "vo" ) #pylint: disable=no-member
      infoMock.assert_any_call( "vo")
      infoMock.assert_any_call( "vo", host="server2" )
      self.assertTrue( any ( "Failed getting information from server2" in str(args) \
                             for args in self.agent.log.error.call_args_list ),
                       self.agent.log.error.call_args_list )
      self.assertTrue( any ( "Failed getting information from default" in str(args) \
                             for args in self.agent.log.error.call_args_list ),
                       self.agent.log.error.call_args_list )

    self.assertFalse( ret['OK'] )
    self.assertIn( "error1\nerror2", ret['Message'] )


if __name__ == '__main__':
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( Bdii2CSTests )
  unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
