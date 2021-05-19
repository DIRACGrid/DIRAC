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

The developers are encouraged to make themselves familiar with pytest_ module documentation, which could be found
`here <https://docs.pytest.org/en/latest/>`__.

One of the requirements for writing a suitable test is an isolation from depended-on code and the same time from production environment.
This could be obtained by objects mocking technique, where all fragile components used in a particular test suite are replaced by their false and dummy
equivalents - test doubles. For that it is recommended to use mock_ module.
Hence it is clear that knowledge of mock_ module API is essential.

Unit tests are typically created by the developer who will also write the code that is being tested. The tests may therefore share the same blind spots with the code: for example, a developer does not realize that certain input parameters must be checked, most likely neither the test nor the code will verify these input parameters. If the developer misinterprets the requirements specification for the module being developed, both the tests and the code will be wrong. Hence if the developer is going to prepare her own unit tests, she should pay attention and take extra care to implement proper testing suite, checking for every spot of possible failure (i.e. interactions with other components) and not trusting that someone else's code is always returning proper type and/or values.


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

There are several excellent guides on how to write good and meaningful unit tests out there. Make sure to follow one of them.


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
  package directory, i.e: all tests modules for WMS should be kept in *src/DIRAC/WorkloadManegementSystem/Client/test* directory.


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

The integration tests which are ran on GitHub/GitLab can be ran locally using docker. So, start by installing docker, if you haven't.

To run all tests in one command, which takes around 20 minutes, create a development environment, position yourself in the DIRAC root directory and then run:

.. code-block:: bash

    ./integration_tests.py create [FLAGS]

Where ``[FLAGS]`` is one or more feature flags ``SERVER_USE_PYTHON3=Yes``.
See ``.github/workflows/integration.yml`` for the available feature flags for your release.

Once finished the containers can be removed using ``./integration_tests.py destroy``.

See ``./integration_tests.py --help`` for more information.

Running the above might take a while. Supposing you are interested in running one single integration test, let's say for the sake of example a server integration test, you can:

.. code-block:: bash

    ./integration_tests.py prepare-environment [FLAGS]
    ./integration_tests.py install-server

which (in some minutes) will give you a fully dockerized server setup (`docker container ls` will list the created container, and you can see what's going on inside with the standard `docker exec -it server /bin/bash`). Now, suppose that you want to run `WorkloadManagementSystem/Test_JobDB.py`.
The first thing to do is that you should first login in the docker container, by doing:

.. code-block:: bash

    ./integration_tests.py exec-server

Now, if you're using Python 2 you will need to copy the test code:

.. code-block:: bash

    cp -r TestCode/DIRAC/tests/ ServerInstallDIR/DIRAC/

while if you are using Python 3 server, the installations automatically pick up external changes to the DIRAC code and tests)

Now you can run the test with:

.. code-block:: bash

    pytest ServerInstallDIR/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py (py2)
    pytest LocalRepo/ALTERNATIVE_MODULES/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py (py3)

For py3 installations, You can find the logs of the services in `/home/dirac/ServerInstallDIR/diracos/runit/`


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
