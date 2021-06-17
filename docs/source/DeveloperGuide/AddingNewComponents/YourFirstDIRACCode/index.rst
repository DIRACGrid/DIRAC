.. _your_first_dirac_code:

=====================
Your first DIRAC code
=====================

We will now code some very simple exercises, based on what we have seen in the previous section.

Exercise 1
----------

Code a python module in DIRAC.Core.Utilities.checkCAOfUser where there is only the following function:


.. code-block:: python

   def checkCAOfUser(user, CA):
     """ user, and CA are string
     """

This function should:

* Get from the CS the registered Certification Authority for the user
* if the CA is the expected one return S_OK, else return S_ERROR

To code this exercise, albeit very simple, we will use TDD (Test Driven Development),
and we will use the *unittest* and *mock* python packages, as explained in :ref:`testing_environment`.
What we will code here will be a real *unit test*, in the sense that we will test only this function, in isolation.
In general, it is always an excellent idea to code a unit test for every development you do.
We will put the unit test in DIRAC.Core.Utilities.test. The unit test has been fully coded already:


.. code-block:: python

   # imports
   import unittest
   import mock
   import importlib

   # sut
   from DIRAC.Core.Utilities.checkCAOfUser import checkCAOfUser


   class TestcheckCAOfUser(unittest.TestCase):
     def setUp(self):
       self.gConfigMock = mock.Mock()
       self.checkCAOfUser = importlib.import_module(
           "DIRAC.Core.Utilities.checkCAOfUser"
       )
       self.checkCAOfUser.gConfig = self.gConfigMock

     def tearDown(self):
       pass


   class TestcheckCAOfUserSuccess(TestcheckCAOfUser):
     def test_success(self):
       self.gConfigMock.getValue.return_value = "attendedValue"
       res = checkCAOfUser("aUser", "attendedValue")
       self.assertTrue(res["OK"])

     def test_failure(self):
       self.gConfigMock.getValue.return_value = "unAttendedValue"
       res = checkCAOfUser("aUser", "attendedValue")
       self.assertFalse(res["OK"])


   if __name__ == "__main__":
     suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestcheckCAOfUser)
     suite.addTest(
         unittest.defaultTestLoader.loadTestsFromTestCase(TestcheckCAOfUserSuccess)
     )
     testResult = unittest.TextTestRunner(verbosity=2).run(suite)


Now, try to run it. In case you are using Eclipse, it's time to try to run this test within Eclipse itself (run as: Python unit-test): it shows a graphical interface that you can find convenient. If you won't manage to run, it's probably because there is a missing configuration of the PYTHONPATH within Eclipse.

Then, code ``checkCAOfUser`` and run the test again.


Exercise 2
----------

As a continuation of the previous exercise, code a python script that will:

* call DIRAC.Core.Utilities.checkCAOfUser.checkCAOfUser
* log wih info or error mode depending on the result

Remember to start the script with:


.. code-block:: python

   #!/usr/bin/env python
   """ Some doc: what does this script should do?
   """
   from DIRAC.Core.Base import Script
   Script.parseCommandLine()


Then run it.
