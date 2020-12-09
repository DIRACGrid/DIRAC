"""
Unit tests of utility functions in the DIRAC.Resources.MessageQueue.Utilities
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import DIRAC.Resources.MessageQueue.Utilities as module
import unittest
from six.moves import queue as Queue

from mock import MagicMock

__RCSID__ = "$Id$"


ROOT_PATH = '/Resources/MQServices/'
MQSERVICE_NAME = 'mq.dirac.net'
QUEUE_TYPE = 'Queue'
TOPIC_TYPE = 'Topic'
QUEUE_NAME = 'Test'

CS_MQSERVICE_OPTIONS = {'Host': MQSERVICE_NAME}
CS_QUEUE_OPTIONS = {'Acknowledgement': True}

QUEUE_CONFIG = \
    {
        '%s/%s/Queues/%s' % (ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME): None,
        '%s/%s/Queues/%s/Acknowledgement' % (ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME): True
    }

TOPIC_CONFIG = \
    {
        '%s/%s/Topics/%s' % (ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME): None,
        '%s/%s/Topics/%s/Acknowledgement' % (ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME): True
    }

SIMILAR_QUEUE_CONFIG = QUEUE_CONFIG.copy()
SIMILAR_QUEUE_CONFIG.update(
    {
        '%s/%s/Queues/%s1' % (ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME): None,
        '%s/%s/Queues/%s1/Acknowledgement' % (ROOT_PATH, MQSERVICE_NAME, QUEUE_NAME): True,
    })

DIFFERENT_MQSERVICE_NAME = 'different-mq.dirac.net'
AMBIGIOUS_QUEUE_CONFIG = QUEUE_CONFIG.copy()
AMBIGIOUS_QUEUE_CONFIG.update(
    {
        '%s/%s/Queues/%s' % (ROOT_PATH, DIFFERENT_MQSERVICE_NAME, QUEUE_NAME): None,
        '%s/%s/Queues/%s/Acknowledgement' % (ROOT_PATH, DIFFERENT_MQSERVICE_NAME, QUEUE_NAME): True,
    })

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


class Test_getMQParamFromCSSuccessTestCase(unittest.TestCase):
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

  def test_getQueue(self):

    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': QUEUE_CONFIG}

    # check returned value
    result = module.getMQParamsFromCS(mqURI=MQSERVICE_NAME + "::" + QUEUE_TYPE + "::" + QUEUE_NAME)
    self.assertTrue(result['OK'])

    # check queue parameters
    self.assertEqual(result['Value']['Queue'], QUEUE_NAME)
    self.assertEqual(result['Value']['Host'], MQSERVICE_NAME)
    self.assertTrue(result['Value']['Acknowledgement'])

    with self.assertRaises(KeyError):
      result['Value']['Topic']

  def test_getTopic(self):
    """ Try to get a topic
    """
    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': TOPIC_CONFIG}

    # check returned value
    TOPIC_NAME = QUEUE_NAME
    result = module.getMQParamsFromCS(mqURI=MQSERVICE_NAME + "::" + TOPIC_TYPE + "::" + TOPIC_NAME)
    self.assertTrue(result['OK'])

    # check topic parameters
    self.assertEqual(result['Value']['Topic'], TOPIC_NAME)
    self.assertEqual(result['Value']['Host'], MQSERVICE_NAME)
    self.assertTrue(result['Value']['Acknowledgement'])

    with self.assertRaises(KeyError):
      result['Value']['Queue']

  def test_getMQService(self):
    self.assertEqual(module.getMQService("bblabl.ch::Topics::MyTopic"), "bblabl.ch")
    self.assertEqual(module.getMQService("bblabl.ch::Queues::MyQueue"), "bblabl.ch")

  def test_getDestinationType(self):
    self.assertEqual(module.getDestinationType("bblabl.ch::Topics::MyTopic"), "Topics")
    self.assertEqual(module.getDestinationType("bblabl.ch::Queues::MyQueue"), "Queues")

  def test_getDestinationName(self):
    self.assertEqual(module.getDestinationName("bblabl.ch::Topics::MyTopic"), "MyTopic")
    self.assertEqual(module.getDestinationName("bblabl.ch::Queues::MyQueue"), "MyQueue")

  def test_getDestinationAddress(self):
    self.assertEqual(module.getDestinationAddress("bblabl.ch::Topics::MyTopic"), "/topic/MyTopic")
    self.assertEqual(module.getDestinationAddress("bblabl.ch::Queues::MyQueue"), "/queue/MyQueue")


class Test_getMQParamFromCSFailureTestCase(unittest.TestCase):
  """ Test class to check known failure scenarios.
  """

  def setUp(self):

    # external dependencies
    module.CSAPI = MagicMock()
    module.gConfig = MagicMock()

  def test_getQueueByInvalidName(self):
    """ Try to get a queue by invalid names
    """

    module.gConfig.getConfigurationTree.return_value = {'OK': True, 'Value': {}}

    # try different possibilities
    result = module.getMQParamsFromCS('%s' % QUEUE_NAME)
    self.assertFalse(result['OK'])

    result = module.getMQParamsFromCS(mqURI=MQSERVICE_NAME + "::" + QUEUE_TYPE + "::" + "InvalidName")
    self.assertFalse(result['OK'])

    result = module.getMQParamsFromCS('%s::' % MQSERVICE_NAME)
    self.assertFalse(result['OK'])

    result = module.getMQParamsFromCS('%s::%s' % (MQSERVICE_NAME, QUEUE_NAME))
    self.assertFalse(result['OK'])


class Test_generateDefaultCallbackTestCase(unittest.TestCase):
  """ Check default callback behaviour.
  """

  def test_EmptyMessage(self):
    myCallback = module.generateDefaultCallback()
    self.assertRaises(Queue.Empty, myCallback.get)

  def test_putOneGetOneMessage(self):
    myCallback = module.generateDefaultCallback()
    myCallback("", "test message")
    self.assertEqual(myCallback.get(), "test message")

  def test_severalMessages(self):
    myCallback = module.generateDefaultCallback()
    myCallback("", "test message1")
    myCallback("", "test message2")
    myCallback("", "test message3")
    myCallback("", "test message4")
    self.assertEqual(myCallback.get(), "test message1")
    self.assertEqual(myCallback.get(), "test message2")
    self.assertEqual(myCallback.get(), "test message3")
    self.assertEqual(myCallback.get(), "test message4")
    self.assertRaises(Queue.Empty, myCallback.get)

  def test_twoDifferentCallbacks(self):
    myCallback = module.generateDefaultCallback()
    myCallback2 = module.generateDefaultCallback()
    myCallback("", "test message")
    myCallback2("", "test message2")
    self.assertEqual(myCallback.get(), "test message")
    self.assertRaises(Queue.Empty, myCallback.get)
    self.assertEqual(myCallback2.get(), "test message2")
    self.assertRaises(Queue.Empty, myCallback2.get)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_getMQParamFromCSSuccessTestCase)
  suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(Test_getMQParamFromCSFailureTestCase))
  suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(Test_generateDefaultCallbackTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
