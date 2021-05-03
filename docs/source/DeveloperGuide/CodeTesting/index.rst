.. _testing_environment:

====================
Testing (VO)DIRAC
====================

.. contents::


Introduction
````````````


Who should read this document
==============================

- *All (VO)DIRAC developers* should read, at least, the sections about unit tests and integration tests
- *All software testers* should read fully this document
- *All (VO)DIRAC developers coordinators* should read fully this document

NB: if you are a developer coordinator, you better be most and foremost, a development instructor, and a software tester.


Why this document should be interesting for you
===============================================

- Because you want your code to work as expected
- Because preventing disasters is better than fixing them afterwards
- Because it's your duty, as developer, to verify that a new version of DIRAC fits your VO needs.


What we mean by testing
========================

Every large enough software project needs to be carefully tested,
monitored and evaluated to assure that minimum standards of quality are being attained by the development process.
A primary purpose of that is to detect software and configuration failures
so that defects may be discovered and corrected before making official release and to check if software meets requirements and works as expected.
Testing itself could also speed up the development process rapidly tracing problems introduced with the new code.

DIRAC is not different from that scenario, with the exception that service-oriented architecture paradigm,
which is one of the basic concepts of the project, making the quality assurance and testing process the real challenge.
However as DIRAC becomes more and more popular and now is being used by several different communities,
the main question is not: *to test or not to test?*, but rather: *how to test in an efficient way?*

The topic of software testing is very complicated by its own nature, but depending on the testing method employed,
the testing process itself can be implemented at any time in the development phase and ideally should cover many different levels of the system:

- *unit tests*, in which the responsible person for one source file is proving that his code is written in a right way,
- *integration tests* that should cover whole group of modules combined together to accomplish one well defined task,
- *regression tests* that seek for errors in existing functionality after patches, functionality enhancements and or configuration changes have been made to the software,
- *certification tests* (or *system tests*), which are run against the integrated and compiled system, treating it as a black box and trying to evaluate the system's compliance with its specified requirements.

If your unit tests are not passing, you should not think yet to start the integration tests.
Similarly, if your integration tests show some broken software, you should not bother running any system test.



Who should write (and run) the tests
========================================

In DIRAC the unit tests should be prepared for the developer herself,
integration tests could be developed in groups of code responsible persons,
for regression tests the responsible person should be a complete subsystem (i.e. WMS, DMS, SMS etc..) manager,
while certification tests should be prepared and performed by release managers.



Tools and methodology
`````````````````````

Unit tests
==========

In DIRAC unit tests should be prepared by the developer herself. As the main implementation language is Python, the developers should
use its default tool for unit testing, which is already a part of any Python distributions: the unittest_ module.

This module provides a rich set of tools for constructing and running tests, supporting some very important concepts, like:

- *fixtures*: initialisation needed for setting up a group of tests together with appropriate clean-up after the execution
- *cases*: the smallest unit of testing for one use case scenario
- *suites*: collection of test cases for aggregation of test that should be executed together
- *runners*: classes for executing tests, checking all the spotted asserts and providing output results to the user.

The developers are encouraged to make themselves familiar with unittest_ module documentation, which could be found
`here <http://docs.python.org/library/unittest.html>`__. It is suggested to read at least documentation for TestCase_, TestSuite_
and TestLoader_ classes and follow the examples over there.

One of the requirements for writing a suitable test is an isolation from depended-on code and the same time from production environment.
This could be obtained by objects mocking technique, where all fragile components used in a particular test suite are replaced by their false and dummy
equivalents - test doubles. For that it is recommended to use mock_ module, which should be accessible in DIRAC externals for server installation.
Hence it is clear that knowledge of mock_ module API is essential.

Unit tests are typically created by the developer who will also write the code that is being tested. The tests may therefore share the same blind spots with the code: for example, a developer does not realize that certain input parameters must be checked, most likely neither the test nor the code will verify these input parameters. If the developer misinterprets the requirements specification for the module being developed, both the tests and the code will be wrong. Hence if the developer is going to prepare her own unit tests, she should pay attention and take extra care to implement proper testing suite, checking for every spot of possible failure (i.e. interactions with other components) and not trusting that someone else's code is always returning proper type and/or values.

Testing the code, and so proper code developing cycle, can be done in four well defined steps:

Step 1. **Preparation**

The first step on such occasions is to find all possible use cases scenarios. The code [#]_ should be read carefully to isolate all the paths of executions. For each of such cases the developer should prepare, formulate and define all required inputs and outputs, configurations, internal and external objects states, underlying components etc.. Spending more time on this preparation phase will help to understand all possible branches, paths and points of possible failures inside the code and accelerate the second step, which is the test suite implementation.

Amongst all scenarios one is very special - so special, that it even has got its own name: *the main success scenario*. This is the path in execution process, in which it is assumed that all components are working fine so the  system is producing results correct to the last bit. The developer should focus on this scenario first, as all the others are most probably branching from it if some error condition would appear.

Step 2. **Implementation**

Once the set of use cases is well defined, the developer should prepare and implement test case for each of use cases which should define:

- initial and final states of the system being tested,
- runtime configuration,
- set of input values, associated objects and their internal states,
- correct behaviour,
- set of output results.

Each test case should be instrumented with a special method: *setUp*,  which is preparing the testing environment. This is the correct place
for constructing input and output data stubs, mock objects that the production code is using from the outside world and initial state of object
being tested. It is a good practice to implement also second special method: *tearDown*, which is doing a clean up after the tests execution, destroying all
objects created inside *setUp* function.

A test case should try to cover as much as possible the API of software under test and the developer is free to decide how many tests
and asserts she would be implementing and executing, but of course there should be at least one test method inside each of test cases and at least
one assert in every test method. The developer should also keep in her mind that being greedy is not a good practice: her test cases should check
only her own code and nothing else.

Step 3. **Test execution**

Every developer is encouraged to execute her test suites by herself. Execution code of test suite should be put into unit test module in a various ways. Of course once the test results are obtained, it is the high time for fixing all places in the tested code, in which tests have failed.


Step 4. **Refactoring**

Once the code is tested and all tests are passed, the developer can start thinking about evolution of the code. This includes
performance issues, cleaning up the code from repetitions, new features, patching, removing obsolete or not used methods.
So from this point the whole developing cycle can start again and again and again...

Test doubles
============

Unit tests should run in *isolation*. Which means that they should run without having DIRAC fully installed, because, remember, they should just test the code logic. If, to run a unit test in DIRAC, you need a dirac.cfg file to be present, you are failing your goal.

To isolate the code being tested from depended-on components it is convenient and sometimes necessary to use *test doubles*:
simplified objects or procedures, that behaves and looks like the their real-intended counterparts, but are actually simplified versions
that reduce the complexity and facilitate testing [#]_. Those fake objects meet the interface requirements of, and stand in for, more complex real ones,
allowing programmers to write and unit-test functionality in one area without actually calling complex underlying or collaborating classes.
The isolation itself help developers to focus their tests on the behaviour of their classes without worrying about its dependencies, but also may be
required under many different circumstance, i.e.:

- if depended-on component may return values or throw exceptions that affect the behaviour of code being tested, but it is impossible or
  difficult for such cases to occur,
- if results or states from depended-on component are unpredictable (like date, weather conditions, absence of certain records in database etc..),
- if communication with internal states of depended-on component is impossible,
- if call to depended-on component has unacceptable side effects ,
- if interactions with depended-on component is resource consuming operation (i.e. database connections and queries),
- if depended-on component is not available or even not existing in the test environment (i.e. the component's implementation hasn't stared yet,
  but its API is well defined).

It is clear that in such cases the developer should try to instrument the test suite with a set doubles, which come is several flavours:

**Dummy**
   A *dummy object* is an object that is used when method being tested has required object of some type as a parameter, but apart of
   that neither test suite nor code being tested care about it.

**Stub**
   A *test stub* is a piece of code that doesn't actually do anything other than declare itself and the parameters it accepts
   and returns something that is usually the values expected in one of the scenarios for the caller. This is probably the most popular double
   used in a test-driven development.

**Mock**
   A *mock object* is a piece of code, that is used to verify the correct behaviour of code that undergo tests, paying more attention
   on how it was called and executed inside the test suite. Typically it also includes the functionality of a test stub in that it must return
   values to the test suite, but the difference is it should also validate if actions that cannot be observed through the public API of code being
   tested are performed in a correct order.

In a dynamically typed language like Python_ every test double is easy to create as there is no need to simulate the full API of depended-on
components and the developer can freely choose only those that are used in her own code.


Example
-------

NOTA BENE: the example that follows suppose that the reader has already a basic familiarity with some DIRAC constructs. If this is not the case, we suggest the reader to first read :ref:`adding_new_components`.

Let's assume we are coding a client to the ``CheeseShopSystem`` inside DIRAC. The depended-on components are ``CheeseShopSystem.Service.CheeseShopOwner`` with
``CheeseShopSystem.DB.CheeseShopDB`` database behind it. Our ``CheeseShopSystem.Client.CheeseShopClient`` could only ask the owner for a specific cheese or try to buy it [#]_.
We know the answers for all question that have been asked already, there was no cheese at all in original script, but here for teaching
purposes we can just pretend for a while that the owner is really checking the shop's depot and even more, the Cheddar is present. The code
for ``CheeseShopOwner``:

.. code-block:: python

   import six

   from DIRAC import S_OK, S_ERROR, gLogger, gConfig
   from DIRAC.Core.DISET.RequestHandler import RequestHandler
   from DIRAC.CheeseShopSystem.DB.CheeseShopDB import CheeseShopDB

   # global instance of a cheese shop database
   cheeseShopDB = False

   # initialize it first
   def initializeCheeseShopOwner(serviceInfo):
     global cheeseShopDB
     cheeseShopDB = CheeseShopDB()
     return S_OK()

   class CheeseShopOwner(RequestHandler):

     types_isThere = [six.string_types]
     def export_isThere(self, cheese):
       return cheeseShopDB.isThere(cheese)

     types_buyCheese = [six.string_types, float]
     def export_buyCheese(self, cheese, quantity):
       return cheeseShopDB.buyCheese(cheese, quantity)

     # ... and so on, so on and so on, i.e:
     types_insertCheese = [six.string_types, float, float]
     def export_insertCheese(self, cheeseName, price, quantity):
       return cheeseShopDB.insertCheese(cheeseName, price, quantity)



And here for ``CheeseShopClient`` class:

.. code-block:: python

   from DIRAC import S_OK, S_ERROR, gLogger, gConfig
   from DIRAC.Core.Base.Client import Client

   class Cheese(object):

     def __init__(self, name):
       self.name = name

   class SpanishInquisitionError(Exception):
     pass

   class CheeseShopClient(Client):

     def __init__(self, money, shopOwner = None):
       self.__money = money
       self.shopOwner = shopOwner

     def buy(self, cheese, quantity = 1.0):

       # is it really cheese, you're asking for?
       if not isinstance(cheese, Cheese):
         raise SpanishInquisitionError("It's stone dead!")

       # and the owner is in?
       if not self.shopOwner:
         return S_ERROR("Shop is closed!")

       # and cheese is in the shop depot?
       res = self.shopOwner.isThere(cheese.name)
       if not res["OK"]:
         return res

       # and you are not asking for too much?
       if quantity > res["Value"]["Quantity"]:
         return S_ERROR("Not enough %s, sorry!" % cheese.name)

       # and you have got enough money perhaps?
       price = quantity * res["Value"]["Price"]
       if self.__money < price:
         return S_ERROR("Not enough money in your pocket, get lost!")

       # so we're buying
       res = self.shopOwner.buyCheese(cheese.name, quantity)
       if not res["OK"]:
         return res
       self.__money -= price

       # finally transaction is over
       return S_OK(self.__money)

This maybe oversimplified code example already has several hot spots of failure for chess buying task: first of all, your input parameters
could be wrong (i.e. you want to buy rather parrot, not cheese); the shop owner could be out; they haven't got cheese you are asking for in the store;
or maybe it is there, but not enough for your order; or you haven't got enough money to pay and at least the transaction itself could be interrupted
for some reason (connection lost, database operation failure etc.).

We have skipped ``CheeseShopDB`` class implementation on purpose: our ``CheeseShopClient`` directly depends on ``CheeseShopOwner`` and we shoudn't
care on any deeper dependencies.

Now for our test suite we will assume that there is a 20 lbs of Cheddar priced 9.95 pounds, hence the test case for success is i.e. asking for
1 lb of Cheddar (the main success scenario) having at least 9.95 pounds in a wallet:

  - input: ``Cheese("Cheddar")``, 1.0 lb, 9.95 pounds in your pocket
  - expected output: ``S_OK = {"OK" : True, "Value" : 0.0 }``

Other scenarios are:

1. Wrong order [#]_:

  * Want to buy Norwegian blue parrot:

    - input: ``Parrot("Norwegian Blue")``
    - expected output: an exception ``SpanishInquisitionError("It's stone dead!")`` thrown in a client

  * Asking for wrong quantity:

    - input: ``Cheese("Cheddar")``, ``quantity = "not a number"`` or ``quantity = 0``
    - expected output: an exception ``SpanishInquisitionError("It's stone dead!")`` thrown in a client

3. The shop is closed:

  - input: ``Cheese("Cheddar")``
  - expected output: ``S_ERROR = {"OK" : False, "Message": "Shop is closed!"}``

4. Asking for any other cheese:

  - input: ``Cheese("Greek feta")``, 1.0 lb
  - expected output: ``S_ERROR = {"OK" : False, "Message": "Ah, not as such!"}``

5. Asking for too much of Cheddar:

  - input: ``Cheese("Cheddar")``, 21.0 lb
  - expected output: ``S_ERROR = {"OK" : False, "Message": "Not enough Cheddar, sorry!"}``

6. No money on you to pay the bill:

  - input: ``Cheese("Cheddar")``, 1.0 lb, 8.0 pounds in your pocket
  - expected output: ``S_ERROR = {"OK" : False, "Message": "Not enough money in your pocket, get lost!"}``

7. Some other unexpected problems in underlying components, which by the way we are not going to be test or explore here. *You just can't test everything,
keep track on testing your code!*

The test suite code itself follows:


.. code-block:: python

   import unittest
   from mock import Mock

   from DIRAC import S_OK, S_ERROR
   from DIRAC.CheeseShopSystem.Client.CheeseShopClient import Cheese, CheeseShopClient
   from DIRAC.CheeseShopSystem.Service.CheeseShopOwner import CheeseShopOwner

   class CheeseClientMainSuccessScenario(unittest.TestCase):

     def setUp(self):
       # stub, as we are going to use it's name but nothing else
       self.cheese = Chesse("Cheddar")
       # money, dummy
       self.money = 9.95
       # amount, dummy
       self.amount = 1.0
       # real object to use
       self.shopOwner = CheeseShopOwner("CheeseShop/CheeseShopOwner")
       # but with mocking of isThere
       self.shopOwner.isThere = Mock(return_value = S_OK({"Price" : 9.95, "Quantity" : 20.0}))
       # and buyCheese methods
       self.shopOwner.buyCheese = Mock()

     def tearDown(self):
       del self.shopOwner
       del self.money
       del self.amount
       del self.cheese

     def test_buy(self):
        client = CheeseShopClient(money = self.money, shopOwner = self.shopOwner)
        # check if test object has been created
        self.assertEqual(isinstance(client, CheeseShopClient), True)
        # and works as expected
        self.assertEqual(client.buy(self.cheese, self.amount), {"OK" : True, "Value" : 0.0})
        ## and now for mocked objects
        # asking for cheese
        self.shopOwner.isThere.assert_called_once_with(self.cheese.name)
        # and buying it
        self.shopOwner.buyCheese.assert_called_once_with(self.cheese.name, self.amount)


   if __name__ == "__main__":
     unittest.main()
     #testSuite = unittest.TestSuite(["CheeseClientMainSuccessScenario"])


Conventions
-----------

All test modules should follow those conventions:

**T1**
  Test environment should be shielded from the production one and the same time should mimic it as far as possible.

**T2**
  All possible interactions with someone else's code or system components should be dummy and artificial. This could be obtained by proper use of
  stubs, mock objects and proper set of input data.

**T3**
  Tests defined in one unit test module should cover one module (in DIRAC case one class) and nothing else.

**T4**
  The test file name convention should follow the rule: *test* word concatenated with module name, i.e. in case of *CheeseClient* module,
  which implementation is kept *CheeseClient.py* disk file, the unit test file should be named *testCheeseClient.py*

**T5**
  Each TestCase_ derived class should be named after module name and scenario it is going to test and *Scenario* world, i.e.:
  *CheeseClientMainSuccessScenario*, *CheeseClientWrongInputScenario* and so on.

**T6**
  Each unit test module should hold at least one TestCase_ derived class, ideally a set of test cases or test suites.

**T7**
  The test modules should be kept as close as possible to the modules they are testing, preferably in a *test* subdirectory on DIRAC subsystem
  package directory, i.e: all tests modules for WMS should be kept in *DIRAC/WMS/tests* directory.


Integration and System tests
============================

Integration and system tests should not be defined at the same level of the unit tests.
The reason is that, in order to properly run such tests, an environment might need to be defined.

Integration and system tests do not just run a single module's code.
Instead, they evaluate that the connection between several modules, or the defined environment, is correctly coded.


The DIRAC/tests part of DIRAC repository
----------------------------------------

The DIRAC repository contains a tests section ``https://github.com/DIRACGrid/DIRAC/tree/integration/tests`` that holds
integration, regression, workflow, system, and performance tests.
These tests are not only used for the certification process. Some of them, in fact, might be extremely useful for the developers.


Integration tests for jobs
--------------------------

**Integration** is a quite vague term. Within DIRAC, we define as integration test every test that does not fall in the unit test category,
but that does not need external systems to complete. Usually, for example, you won't be able to run an integration test if you have not added something in the CS.
This is still vague, so better look at an `example <https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Workflow/Integration/Test_UserJobs.py>`_

This test submits few very simple jobs. Where? Locally. The API ``DIRAC.Interfaces.API.Job.Job`` contains a ``runLocal()`` method.
Admittently, this method is here almost only for testing purposes.

Submitting a job locally means instructing DIRAC to consider your machine as a worker node.
To run this test, you'll have to add few lines to your local dirac.cfg::

   LocalSite
   {
     Site = DIRAC.mySite.local
     CPUScalingFactor = 0.0
     #SharedArea = /cvmfs/lhcb.cern.ch/lib
     #LocalArea =/home/some/local/LocalArea
     GridCE = my.CE.local
     CEQueue = myQueue
     Architecture = x86_64-slc5
     #CPUTimeLeft = 200000
     CPUNormalizationFactor = 10.0
   }

These kind of tests can be extremely useful if you use the Job API and the DIRAC workflow to make your jobs.


Integration tests for services
------------------------------

Another example of integration tests are tests of the chain:

   ``Client -> Service -> DB``

They supposes that the DB is present, and that the service is running. Indeed, usually in DIRAC you need to access a DB, write and read from it.
So, you develop a DB class holding such basic interaction. Then, you develop a Service (Handler) that will look into it.
Lastly, a Client will hold the logic, and will use the Service to connect to the DB. Just to say, an example of such a chain is:

   ``TransformationClient -> TransformationManagerHandler -> TransformationDB``

And this is tested in this `test file <https://github.com/DIRACGrid/DIRAC/blob/integration/tests/Integration/TransformationSystem/Test_Client_Transformation.py>`_

The test code itself contains something as simple as a series of put/delete,
but running such test can solve you few headaches before committing your code.

Tipically, other requirements might be needed for the integration tests to run.
For example, one requirement might be that the DB should be empty.

Integration tests, as unit tests, are coded by the developers.
Suppose you modified the code of a DB for which its integration test already exist:
it is a good idea to run the test, and verify its result.

Within section :ref:`adding_new_components` we will develop one of these tests as an exercise.

Integration tests are a good example of the type of tests that can be run by a machinery.
Continuous integration tools like Jenkins are indeed used for running these type of tests.


Continuous Integration software
-------------------------------

There are several tools, on the free market, for so-called *Continuous Integration*, or simply CI_.
One possibility is to use Jenkins, but today (from branch *rel-v7r0*) all DIRAC integration tests are run
by `GitHub Actions <https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22Integration+tests%22>`_

If you have looked in the `DIRAC/tests <https://github.com/DIRACGrid/DIRAC/tree/integration/tests>`_ 
(and if you haven't yet, you should, now!) you will see also a folder called Jenkins, and one called CI.
These 2 folders together are used for running all the integration tests. 
Such tests can be run on GitHub Actions, on GitLab-CI, and on Jenkins.

What can a tool like GitHub Actions, GitLab-CI, and Jenkins do for you? Several things, in fact:

- it can run all the unit tests
- it can run `Pylint <http://www.pylint.org/>`_ (of which we didn't talk about yet, but, that you should use, and for which it exists a nice documentation that you should probably read) (ah, use `this file <https://github.com/DIRACGrid/DIRAC/blob/integration/.pylintrc>`_ as configuration file.
- (not so surprisingly) it can run all the integration tests
- (with some tuning) it can run some of the system tests

For example, the DIRAC.tests.Jenkins.dirac_ci.sh adds some nice stuff, like:

- a function to install DIRAC (yes, fully), configure it, install all the databases, install all the services, and run them!
- a function that runs the Pilot, so that a worker node will look exactly like a Grid WN. Just, it will not start running the JobAgent

What can you do with those above? You can run the Integration tests you read above!

How do I do that?

- you need a MySQL DB somewhere, empty, to be used only for testing purposes (in GitHub Actions and GitLab-CI a docker container is instantiated for the purpose)
- you need a ElasticSearch instance running somewhere, empty, to be used only for testing purposes (in GitHub Actions and GitLab-CI a docker container is instantiated for the purpose)
- if you have tests that need to access other DBs, you should also have them ready, again used for testing purposes.

The files ``DIRAC/tests/Integration/all_integration_client_tests.sh`` and ``DIRAC/tests/Integration/all_integration_server_tests.sh``
contain all the integration tests that will be executed. 

If you are a developer you should be able to extrapolate from the above those parts that you need,
in case you are testing only one specific service.


Running integration tests locally
---------------------------------

The integration tests which are ran on GitHub/GitLab can be ran locally using docker.

To run all tests in one command, which takes around 20 minutes, create a development environment, position yourself in the DIRAC root directory and then run:

.. code-block:: bash

    ./integration_tests.py create [FLAGS]

Where ``[FLAGS]`` is one or more feature flags ``SERVER_USE_PYTHON3=Yes``.
See ``.github/workflows/integration.yml`` for the available feature flags for your release.

Once finished the containers can be removed using ``./integration_tests.py destroy``.

See ``./integration_tests.py --help`` for more information.

Running the above might take a while. Supposing you are interested in running one single integration test, let's say for the sake of example a server integration test, you can:

.. code-block:: bash

    ./integration_tests.py prepare-environment
    ./integration_tests.py install-server

which will give you a full dockerized server setup (`docker container ls` will list the created container, and you can see what's going on inside with the standard `docker exec -it server /bin/bash`). Now, suppose that you want to run `WorkloadManagementSystem/Test_JobDB.py`. You can run it with:

.. code-block:: bash

    ./integration_tests.py exec-server

If you're using Python 2 you will also need to copy the test code (Python 3 installations automatically pick up external changes to the DIRAC code and tests):

.. code-block:: bash
    cp -r TestCode/DIRAC/tests/ ServerInstallDIR/DIRAC/

and then run the test with:

.. code-block:: bash

    pytest ServerInstallDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py



Validation and System tests
---------------------------

Validation and System tests are black-box tests. As such, coding them should not require knowledge of the inner design of the code or logic.
At the same time, to run them you'll require a DIRAC server installation.
Examples of a system test might be: send jobs on the Grid, and expecting them to be completed after hours. Or, replicate a file or two.

Validation and system tests are usually coded by software testers. The DIRAC repository contains, in the *tests* `package <https://github.com/DIRACGrid/DIRAC/tree/integration/tests/System>`_
a minimal set of test jobs, but since most of the test jobs that you can run are VO specific, we suggest you to expand the list.

The server `lbcertifdirac70.cern.ch <lbcertifdirac70.cern.ch>`_ is used as "DIRAC certification machine".
With "certification machine" we mean that it is a full DIRAC installation, that connects to grid resources, and through which we certify pre-production versions.
Normally, the latest DIRAC pre-releases are installed there.
Its access is restricted to some power users, for now, but do request access if you need to do some specific system test.
This installation is usually not done for running private tests, but in a controlled way can be sometimes tried.



The certification process
============================

Each DIRAC release go through a long and detailed certification process.
A certification process is a series of steps that include unit, integration, validation and system tests.
We use detailed trello boards and slack channel. Please DO ASK to be included in such process.

The template for DIRAC certification process can be found at the trello `board <https://trello.com/b/cp8ULOhQ/dirac-certification-template>`_
and the slack channel is `here <https://lhcbdirac.slack.com/messages/C3AGWCA8J/>`__



Footnotes
============

.. [#] Or even better software requirements document, if any of such exists. Otherwise this is a great opportunity to prepare one.
.. [#] To better understand this term, think about a movie industry: if a scene movie makers are going to film is potentially dangerous and unsafe for the leading actor, his place is taken over by a stunt double.
.. [#] And eventually is killing him with a gun. At least in a TV show.
.. [#] You may ask: *isn't it silly?* No, in fact it isn't. Validation of input parameters is one of the most important tasks during testing.


.. _Python: http://www.python.org/
.. _unittest: http://docs.python.org/library/unittest.html
.. _TestCase: http://docs.python.org/library/unittest.html#unittest.TestCase
.. _TestSuite: http://docs.python.org/library/unittest.html#unittest.TestSuite
.. _TestLoader: http://docs.python.org/library/unittest.html#unittest.TestLoader
.. _mock: http://www.voidspace.org.uk/python/mock/
.. _CI: https://en.wikipedia.org/wiki/Continuous_integration
.. _Jenkins: https://jenkins-ci.org/
