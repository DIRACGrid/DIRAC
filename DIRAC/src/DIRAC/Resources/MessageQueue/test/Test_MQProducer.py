"""Unit tests of MQProducer interface in the DIRAC.Resources.MessageQueue.MProducerQ
"""
import unittest
from DIRAC import S_OK
from DIRAC.Resources.MessageQueue.MQProducer import MQProducer
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager
from DIRAC.Resources.MessageQueue.MQConnector import MQConnector


class FakeMQConnector(MQConnector):
    def __init__(self, params={}):
        super().__init__()

    def disconnect(self):
        return S_OK("FakeMQConnection disconnecting")

    def get(self, destination=""):
        return "FakeMQConnection getting message"

    def put(self, message, parameters=None):
        return S_OK("FakeMQConnection sending message: " + str(message))


class TestMQProducer(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None  # To show full difference between structures in  case of error
        dest = {}
        dest.update({"/queue/FakeQueue": ["producer4", "producer2"]})
        dest4 = {"/queue/test3": ["producer1", "consumer2", "consumer3", "consumer4"]}
        conn1 = {"MQConnector": FakeMQConnector(), "destinations": dest}
        conn2 = {"MQConnector": FakeMQConnector(), "destinations": dest4}
        storage = {"fake.cern.ch": conn1, "testdir.blabla.ch": conn2}
        self.myManager = MQConnectionManager(connectionStorage=storage)

    def tearDown(self):
        pass


class TestMQProducer_put(TestMQProducer):
    def test_success(self):
        producer = MQProducer(mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", producerId="producer4")
        result = producer.put("wow!")
        self.assertTrue(result["OK"])
        self.assertEqual(result["Value"], "FakeMQConnection sending message: wow!")

    def test_failure(self):
        producer = MQProducer(mqManager=self.myManager, mqURI="bad.cern.ch::Queues::FakeQueue", producerId="producer4")
        result = producer.put("wow!")
        self.assertFalse(result["OK"])


class TestMQProducer_close(TestMQProducer):
    def test_success(self):
        producer = MQProducer(mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", producerId="producer4")
        result = producer.close()
        self.assertTrue(result["OK"])
        # producer is still able to sent cause the connection is still active (producer2 is connected)
        result = producer.put("wow!")
        self.assertTrue(result["OK"])

    def test_failure(self):
        producer = MQProducer(mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", producerId="producer4")
        result = producer.close()
        self.assertTrue(result["OK"])
        result = producer.close()
        self.assertFalse(result["OK"])
        self.assertEqual(
            result["Message"],
            "MQ connection failure ( 1142 : Failed to stop the connection!The messenger producer4 does not exist!)",
        )

    def test_failure2(self):
        producer = MQProducer(mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", producerId="producer4")
        producer2 = MQProducer(
            mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", producerId="producer2"
        )
        result = producer.close()
        self.assertTrue(result["OK"])
        result = producer.close()
        self.assertFalse(result["OK"])
        self.assertEqual(
            result["Message"],
            "MQ connection failure ( 1142 : Failed to stop the connection!The messenger producer4 does not exist!)",
        )
        result = producer2.close()
        self.assertTrue(result["OK"])
        result = producer2.close()
        self.assertFalse(result["OK"])
        self.assertEqual(
            result["Message"],
            "MQ connection failure ( 1142 : Failed to stop the connection!The messenger producer2 does not exist!)",
        )
        # connection does not exist so put will not work
        result = producer.put("wow!")
        self.assertFalse(result["OK"])
        self.assertEqual(result["Message"], "Failed to get the MQConnector!")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestMQProducer)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQProducer_put))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQProducer_close))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
