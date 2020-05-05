""" Contains unit tests of NetworkAgent module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import unittest
import sys

import DIRAC.AccountingSystem.Agent.NetworkAgent as module

from mock.mock import MagicMock

__RCSID__ = "$Id$"

MQURI1 = 'mq.dirac.net::Topics::perfsonar.summary.packet-loss-rate'
MQURI2 = 'mq.dirac.net::Queues::perfsonar.summary.histogram-owdelay'

ROOT_PATH = '/Resources/Sites'
SITE1 = 'LCG.Dirac.net'
SITE2 = 'LCG.DiracToRemove.net'
SITE3 = 'VAC.DiracToAdd.org'
SITE1_HOST1 = 'perfsonar.diracold.net'
SITE1_HOST2 = 'perfsonar-to-disable.diracold.net'
SITE2_HOST1 = 'perfsonar.diractoremove.net'
SITE3_HOST1 = 'perfsonar.diractoadd.org'

INITIAL_CONFIG = \
{
  '%s/LCG/%s/Network/%s/Enabled' % ( ROOT_PATH, SITE1, SITE1_HOST1 ): 'True',
  '%s/LCG/%s/Network/%s/Enabled' % ( ROOT_PATH, SITE1, SITE1_HOST2 ): 'True',
  '%s/LCG/%s/Network/%s/Enabled' % ( ROOT_PATH, SITE2, SITE2_HOST1 ): 'True'
}

UPDATED_CONFIG = \
{
  '%s/LCG/%s/Network/%s/Enabled' % ( ROOT_PATH, SITE1, SITE1_HOST1 ): 'True',
  '%s/LCG/%s/Network/%s/Enabled' % ( ROOT_PATH, SITE1, SITE1_HOST2 ): 'False',
  '%s/LCG/%s/Network/%s/Enabled' % ( ROOT_PATH, SITE3, SITE3_HOST1 ): 'True'
}



class NetworkAgentSuccessTestCase( unittest.TestCase ):
  """ Test class to check success scenarios.
  """

  def setUp( self ):

    # external dependencies
    module.datetime = MagicMock()

    # internal dependencies
    module.S_ERROR = MagicMock()
    module.S_OK = MagicMock()
    module.gLogger = MagicMock()
    module.AgentModule = MagicMock()
    module.Network = MagicMock()
    module.gConfig = MagicMock()
    module.CSAPI = MagicMock()
    module.createConsumer = MagicMock()

    # prepare test object
    module.NetworkAgent.__init__ = MagicMock( return_value = None )
    module.NetworkAgent.am_getOption = MagicMock( return_value = 100 )  # buffer timeout

    self.agent = module.NetworkAgent()
    self.agent.initialize()

  @classmethod
  def tearDownClass(cls):
    sys.modules.pop('DIRAC.AccountingSystem.Agent.NetworkAgent')

  def test_updateNameDictionary( self ):

    module.gConfig.getConfigurationTree.side_effect = [
                                                        {'OK': True, 'Value': INITIAL_CONFIG },
                                                        {'OK': True, 'Value': UPDATED_CONFIG },
                                                      ]

    # check if name dictionary is empty
    self.assertFalse( self.agent.nameDictionary )

    self.agent.updateNameDictionary()
    self.assertEqual( self.agent.nameDictionary[SITE1_HOST1], SITE1 )
    self.assertEqual( self.agent.nameDictionary[SITE1_HOST2], SITE1 )
    self.assertEqual( self.agent.nameDictionary[SITE2_HOST1], SITE2 )

    self.agent.updateNameDictionary()
    self.assertEqual( self.agent.nameDictionary[SITE1_HOST1], SITE1 )
    self.assertEqual( self.agent.nameDictionary[SITE3_HOST1], SITE3 )

    # check if hosts were removed form dictionary
    self.assertRaises( KeyError, lambda: self.agent.nameDictionary[SITE1_HOST2] )
    self.assertRaises( KeyError, lambda: self.agent.nameDictionary[SITE2_HOST1] )

  def test_agentExecute( self ):

    module.NetworkAgent.am_getOption.return_value = '%s, %s' % ( MQURI1, MQURI2 )
    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': INITIAL_CONFIG }

    # first run
    result = self.agent.execute()
    self.assertTrue( result['OK'] )

    # second run (simulate new messages)
    self.agent.messagesCount += 10
    result = self.agent.execute()
    self.assertTrue( result['OK'] )

    # third run (no new messages - restart consumers)
    result = self.agent.execute()
    self.assertTrue( result['OK'] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( NetworkAgentSuccessTestCase )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
