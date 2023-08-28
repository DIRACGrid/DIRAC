"""Unit tests of MQConsumer interface in the DIRAC.Resources.MessageQueue.MQConsumer
"""
import unittest
from DIRAC import S_OK
from DIRAC.Resources.MessageQueue.MQConsumer import MQConsumer
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager
from DIRAC.Resources.MessageQueue.MQConnector import MQConnector


class FakeMQConnector(MQConnector):
    def __init__(self, params={}):
        super().__init__()

    def disconnect(self):
        return S_OK("FakeMQConnection disconnecting")

    def get(self, destination=""):
        return "FakeMQConnection getting message"

    def subscribe(self, parameters=None):
        return S_OK("Subscription successful")

    def unsubscribe(self, parameters):
        return S_OK("Unsubscription successful")


class TestMQConsumer(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None  # To show full difference between structures in  case of error
        dest = {}
        dest.update({"/queue/FakeQueue": ["consumer4", "consumer2"]})
        dest4 = {"/queue/test3": ["consumer1", "consumer2", "consumer3", "consumer4"]}
        conn1 = {"MQConnector": FakeMQConnector(), "destinations": dest}
        conn2 = {"MQConnector": FakeMQConnector(), "destinations": dest4}
        storage = {"fake.cern.ch": conn1, "testdir.blabla.ch": conn2}
        self.myManager = MQConnectionManager(connectionStorage=storage)

    def tearDown(self):
        pass


class TestMQConsumer_get(TestMQConsumer):
    def test_failure(self):
        consumer = MQConsumer(mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", consumerId="consumer1")
        result = consumer.get()
        self.assertFalse(result["OK"])
        self.assertEqual(result["Message"], "No messages ( 1141 : No messages in queue)")

    def test_sucess(self):
        consumer = MQConsumer(mqManager=self.myManager, mqURI="bad.cern.ch::Queues::FakeQueue", consumerId="consumer1")
        result = consumer.get()
        self.assertFalse(result["OK"])


class TestMQConsumer_close(TestMQConsumer):
    def test_success(self):
        consumer = MQConsumer(mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", consumerId="consumer4")
        result = consumer.close()
        self.assertTrue(result["OK"])

    def test_failure(self):
        consumer = MQConsumer(mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", consumerId="consumer4")
        result = consumer.close()
        self.assertTrue(result["OK"])
        result = consumer.close()
        self.assertFalse(result["OK"])
        self.assertEqual(
            result["Message"],
            "MQ connection failure ( 1142 : Failed to stop the connection!The messenger consumer4 does not exist!)",
        )

    def test_failure2(self):
        consumer = MQConsumer(mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", consumerId="consumer4")
        consumer2 = MQConsumer(
            mqManager=self.myManager, mqURI="fake.cern.ch::Queues::FakeQueue", consumerId="consumer2"
        )
        result = consumer.close()
        self.assertTrue(result["OK"])
        result = consumer.close()
        self.assertFalse(result["OK"])
        self.assertEqual(
            result["Message"],
            "MQ connection failure ( 1142 : Failed to stop the connection!The messenger consumer4 does not exist!)",
        )
        result = consumer2.close()
        self.assertTrue(result["OK"])
        result = consumer2.close()
        self.assertFalse(result["OK"])
        self.assertEqual(
            result["Message"],
            "MQ connection failure ( 1142 : Failed to stop the connection!The messenger consumer2 does not exist!)",
        )


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestMQConsumer)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQConsumer_get))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQConsumer_close))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
