.. _testing_dbs:

=====================================
Testing a DB while developing it
=====================================

For testing a DB code, we suggests to follow similar paths of what is explained in :ref:`testing_services`, so to run an integration test.
In any case, to test the DB class you'll need... the DB! And, on top of that, in DIRAC, it makes very little sense to have DB class functionalities not exposed by a service, so you might even want to test the service and DB together.


Exercise 1:
-----------

Write an integration test for AtomDB using python unittest. Then, write a service for AtomDB and its integration test.
