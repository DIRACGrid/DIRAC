.. _testing_services:

=====================================
Testing a service while developing it
=====================================

As described in :ref:`testing_environment` a way to test a service is to run an integration test, that can run when the service is actually running. It is also possible to write a proper unit test, but this is not the usually recommended way. Reasons are:

* It's not trivial to write a unit test for a service: reason being, the DIRAC framework can't be easily mocked.
* The code inside a service is (should be) simple, no logic should be embedded in there: so, what you want to test, is its integration.

Exercise 1:
-----------

Write an integration test for HelloHandler. This test should use python unittest, and should assume that the Hello service is running. The test stub follows:

.. code-block:: python

   # imports
   import unittest
   # sut
   from DIRAC.Core.DISET.RPCClient import RPCClient


   class TestHelloHandler(unittest.TestCase):
       def setUp(self):
	   self.helloService = RPCClient("Framework/Hello")

       def tearDown(self):
	   pass

   class TestHelloHandlerSuccess(TestHelloHandler):
       def test_success(self):
	   pass

   class TestHelloHandlerFailure(TestHelloHandler):
       def test_failure(self):
	   pass


   if __name__ == '__main__':
       suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestHelloHandler)
       suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase(TestHelloHandlerSuccess))
       suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase(TestHelloHandlerFailure))
       testResult = unittest.TextTestRunner(verbosity = 2).run(suite)


As said, examples can be found in the DIRAC/tests package.
