"""
Unit tests of utility functions in the DIRAC.Resources.MessageQueue.Utilities
"""

import DIRAC.Resources.MessageQueue.Utilities as module
import unittest

from mock import MagicMock

__RCSID__ = "$Id$"


ROOT_PATH = '/Resources/MQService/'
MQSERVICE_NAME = 'mq.dirac.net'
QUEUE_NAME = 'Test'

CS_MQSERVICE_OPTIONS = { 'Host': MQSERVICE_NAME }
CS_QUEUE_OPTIONS = { 'Acknowledgement': True }

QUEUE_CONFIG = \
{
  '%s/%s/Queues/%s' % ( ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME ): None,
  '%s/%s/Queues/%s/Acknowledgement' % ( ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME ): True
}

TOPIC_CONFIG = \
{
  '%s/%s/Topics/%s' % ( ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME ): None,
  '%s/%s/Topics/%s/Acknowledgement' % ( ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME ): True
}

SIMILAR_QUEUE_CONFIG = QUEUE_CONFIG.copy()
SIMILAR_QUEUE_CONFIG.update(
{
  '%s/%s/Queues/%s1' % ( ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME ): None,
  '%s/%s/Queues/%s1/Acknowledgement' % ( ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME ): True,
})

DIFFERENT_MQSERVICE_NAME = 'different-mq.dirac.net'
AMBIGIOUS_QUEUE_CONFIG = QUEUE_CONFIG.copy()
AMBIGIOUS_QUEUE_CONFIG.update(
{
  '%s/%s/Queues/%s' % ( ROOT_PATH, DIFFERENT_MQSERVICE_NAME, QUEUE_NAME ): None,
  '%s/%s/Queues/%s/Acknowledgement' % ( ROOT_PATH, DIFFERENT_MQSERVICE_NAME, QUEUE_NAME ): True,
} )

"""
Used CS example:
Resources
{
  MQServices
  {
    mq.dirac.net
    {
      Host = mq.dirac.net
      Queues
      {
        Test
        {
          Acknowledgement = True
        }
      }
      Topics
      {
        Test
        {
          Acknowledgement = True
        }
      }
    }
    different-mq.dirac.net
    {
      Host = different-mq.dirac.net
      Queues
      {
        Test
        {
          Acknowledgement = True
        }
      }
      Topics
      {
        Test
        {
          Acknowledgement = True
        }
      }
    }
  }
}
"""

class _getMQParamFromCSSuccessTestCase( unittest.TestCase ):
  """ Test class to check success scenarios.
  """

  def setUp(self):

    # external dependencies
    module.CSAPI = MagicMock()
    module.gConfig = MagicMock()

    module.gConfig.getOptionsDict.side_effect = [
                                                  {'OK': True, 'Value': CS_MQSERVICE_OPTIONS},
                                                  {'OK': True, 'Value': CS_QUEUE_OPTIONS}
                                                ]

  def test_getQueueByProperName( self ):
    """ Try to get a queue by a proper queue name only
    """

    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': QUEUE_CONFIG}

    # check returned value
    result = module.getMQParamsFromCS( QUEUE_NAME )
    self.assertTrue( result['OK'] )

    # check queue parameters
    self.assertEqual( result['Value']['Queue'], QUEUE_NAME )
    self.assertEqual( result['Value']['Host'], MQSERVICE_NAME )
    self.assertTrue( result['Value']['Acknowledgement'] )

    with self.assertRaises( KeyError ):
      result['Value']['Topic']

  def test_getQueueByProperName2( self ):
    """ Try to get a queue by a service and queue name
    """

    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': QUEUE_CONFIG}

    # check returned value
    result = module.getMQParamsFromCS( '%s::%s' % ( MQSERVICE_NAME, QUEUE_NAME ) )
    self.assertTrue( result['OK'] )

    # check queue parameters
    self.assertEqual( result['Value']['Queue'], QUEUE_NAME )
    self.assertEqual( result['Value']['Host'], MQSERVICE_NAME )
    self.assertTrue( result['Value']['Acknowledgement'] )

    with self.assertRaises( KeyError ):
      result['Value']['Topic']


  def test_getTopicByProperName( self ):
    """ Try to get a topic by a proper name
    """

    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': TOPIC_CONFIG}

    # check returned value
    result = module.getMQParamsFromCS( QUEUE_NAME )
    self.assertTrue( result['OK'] )

    # check queue parameters
    self.assertEqual( result['Value']['Topic'], QUEUE_NAME )
    self.assertEqual( result['Value']['Host'], MQSERVICE_NAME )
    self.assertTrue( result['Value']['Acknowledgement'] )

    with self.assertRaises( KeyError ):
      result['Value']['Queue']

  def test_getQueueBySimilarName( self ):
    """ Try to get a queue by a similar name
    """

    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': SIMILAR_QUEUE_CONFIG}

    # check returned value
    result = module.getMQParamsFromCS( QUEUE_NAME )
    self.assertTrue( result['OK'] )
    self.assertEqual( result['Value']['Queue'], QUEUE_NAME )

class _getMQParamFromCSFailureTestCase( unittest.TestCase ):
  """ Test class to check known failure scenarios.
  """

  def setUp( self ):

    # external dependencies
    module.CSAPI = MagicMock()
    module.gConfig = MagicMock()

  def test_getQueueByInvalidName( self ):
    """ Try to get a queue by invalid names
    """

    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': {}}

    # try different possibilities
    result = module.getMQParamsFromCS( QUEUE_NAME )
    self.assertFalse( result['OK'] )

    result = module.getMQParamsFromCS( '%s::' % MQSERVICE_NAME )
    self.assertFalse( result['OK'] )

    result = module.getMQParamsFromCS( '%s::%s' % ( MQSERVICE_NAME, QUEUE_NAME ) )
    self.assertFalse( result['OK'] )

  def test_getQueueByAmbiguousName( self ):
    """ Try to get a queue by an ambiguous name
    """

    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': AMBIGIOUS_QUEUE_CONFIG }

    result = module.getMQParamsFromCS( QUEUE_NAME )
    self.assertFalse( result['OK'] )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( _getMQParamFromCSSuccessTestCase )
  suite.addTests( unittest.defaultTestLoader.loadTestsFromTestCase( _getMQParamFromCSFailureTestCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
