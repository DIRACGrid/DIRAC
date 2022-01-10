.. _testing_services:

=====================================
Testing a service while developing it
=====================================

As described in :ref:`testing_environment` a way to test a service is to run an integration test, that can run when the service is actually running. It is also possible to write a proper unit test, but this is not always the recommended way. Reasons are:

* It's not always trivial to write a unit test for a service (while they exists)
* The code inside a service is (should be) simple, no logic should be embedded in there: so, what you want to test, is its integration.

Exercise 1:
-----------

Write an integration test for HelloHandler. This test uses pytest, and assumes that the Hello service is running. The test stub follows:

.. code-block:: python

   # sut
   from DIRAC.Core.Base.Client import Client

   helloService = Client(url="Framework/Hello")

   def test_success():
       pass

   def test_failure():
       pass

As said, examples can be found in the DIRAC/tests/Integration/ package.
