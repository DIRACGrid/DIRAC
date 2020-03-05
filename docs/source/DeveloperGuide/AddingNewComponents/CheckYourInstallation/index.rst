.. _check_your_installation:

======================================
Check your installation
======================================

If you are here, we suppose you have read the documentation that came before. Specifically:

* you should know about our :ref:`development_model`
* you should have read about :ref:`development_environment`, at least until the :ref:`editing_code` part.

Within this part we'll check the basics, and we'll do few exercises.


Is my installation correctly done?
--------------------------------------

We will now do few, very simple checks. The first can be done by using the python interactive shell.
For these examples I will actually use `iPython <http://ipython.org/>`_, which is a highly recommended shell.

Make sure that you are running these commands inside the python virtual environment
that you have created with *virtualenv* as explained in :ref:`editing_code`.

.. code-block:: python

   In [1]: import pyparsing
   In [2]: import MySQLdb
   In [3]: import DIRAC

Were these imports OK? If not, then you should probably hit the "previous" button of this guide,
or check the *pip install* log.


The real basic stuff
--------------------

Let's start with the **logger**

.. code-block:: python

   In [3]: from DIRAC import gLogger

   In [4]: gLogger.notice('Hello world')
   Hello world
   Out[4]: True

What's that? It is a `singleton <http://en.wikipedia.org/wiki/Singleton_pattern>`_ object for logging in DIRAC.
Needless to say, you'll use it a lot.

.. code-block:: python

   In [5]: gLogger.info('Hello world')
   Out[5]: False

Why "Hello world" was not printed? Because the logging level is too high:

.. code-block:: python

   In [6]: gLogger.getLevel()
   Out[6]: 'NOTICE'

But we can increase it simply doing, for example:

.. code-block:: python

   In [7]: gLogger.setLevel('VERBOSE')
   Out[7]: True

   In [8]: gLogger.info('Hello world')
   Hello world
   Out[8]: True

In DIRAC, you should not use print. Use the gLogger instead.
You will find more details on gLogger in the :ref:`gLogger_gLogger` documentation.


Let's continue, and we have a look at the **return codes**:

.. code-block:: python

   In [11]: from DIRAC import S_OK, S_ERROR

These 2 are the basic return codes that you should use. How do they work?

.. code-block:: python

   In [12]: S_OK('All is good')
   Out[12]: {'OK': True, 'Value': 'All is good'}

   In [13]: S_ERROR('Damn it')
   Out[13]:  {'Errno': 0, 'Message': 'Damn it', 'OK': False, 'CallStack': ['  File "<stdin>", line 1, in <module>\n']}

   In [14]: S_ERROR( errno.EPERM, 'But I want to!')
   Out[14]:  {'Errno': 1, 'Message': 'Operation not permitted ( 1 : But I want to!)', 'OK': False, 'CallStack': ['  File "<stdin>", line 1, in <module>\n']}

Quite clear, isn't it? Often, you'll end up doing a lot of code like that:

.. code-block:: python

   result = aDIRACMethod()
   if not result['OK']:
     gLogger.error('aDIRACMethod-Fail', "Call to aDIRACMethod() failed with message %s" %result['Message'])
     return result
   else:
     returnedValue = result['Value']



Playing with the Configuration Service
--------------------------------------

Note: please, read and complete :ref:`stuff_that_run` before continuing.

If you are here, it means that your developer installation contains a **dirac.cfg** file,
that should stay in the $DIRACDEVS/etc directory. We'll play a bit with it now.

You have already done this:

.. code-block:: python

   In [14]: from DIRAC import gConfig

   In [15]: gConfig.getValue('/DIRAC/Setup')
   Out[15]: 'DeveloperSetup'

Where does 'DeveloperSetup' come from? Open that dirac.cfg and search for it. Got it? it's in::

   DIRAC
   {
     ...
     Setup = DeveloperSetup
     ...
   }

Easy, huh? Try to get something else now, still using gConfig.getValue().

So, gConfig is another singleton: it is the guy you need to call for basic interactions with the `Configuration Service <needAReference>`_.
If you are here, we assume you already know about the CS servers and layers. More information can be found in the Administration guide.
We remind that, for a developer installation, we will work in ISOLATION, so with only the local dirac.cfg

Mostly, gConfig exposes *get* type of methods:

.. code-block:: python

   In [2]: gConfig.get
   gConfig.getOption       gConfig.getOptionsDict  gConfig.getServersList
   gConfig.getOptions      gConfig.getSections     gConfig.getValue

for example, try:

.. code-block:: python

   In [2]: gConfig.getOptionsDict('/DIRAC')

In the next section we will modify a bit the dirac.cfg file. Before doing that, have a look at it.
It's important what's in there, but for the developer installation it is also important what it is NOT there. We said we will work in isolation.
So, it's important that this file does not contain any URL to server infrastructure (at least, not at this level: later, when you will feel more confortable, you can add some).

A very important option of the cfg file is "DIRAC/Configuration/Server": this option can contain the URL(s) of the running Configuration Server.
But, as said, for doing development, this option should stay empty.


Getting a Proxy
---------------------

We assume that you have already your public and private certificates key in $HOME/.globus.
Then, do the following::

   dirac-proxy-init

if you got something like::

  > dirac-proxy-init
  Traceback (most recent call last):
    File "/home/dirac/diracInstallation/scripts/dirac-proxy-init", line 22, in <module>
      for entry in os.listdir( baseLibPath ):
  OSError: [Errno 2] No such file or directory: '/home/dirac/diracInstallation/Linux_x86_64_glibc-2.12/lib'

just create the directory by hand.

Now, if try again you will probably get something like::

   > dirac-proxy-init
   Generating proxy...
   Enter Certificate password:
   DN /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fstagni/CN=693025/CN=Federico Stagni is not registered

This is because DIRAC still doesn't know you exist. You should add yourself to the CS. For example, I had add the following section::

   Registry
   {
     Users
     {
       fstagni
       {
         DN = /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fstagni/CN=693025/CN=Federico Stagni
         CA = /DC=ch/DC=cern/CN=CERN Trusted Certification Authority
         Email = federico.stagni@cern.ch
       }
     }


All the info you want and much more in::

   openssl x509 -in usercert.pem -text


Now, it's time to issue again::

   toffo@pclhcb181:~/.globus$ dirac-proxy-init
   Generating proxy...
   Enter Certificate password:
   User fstagni has no groups defined

So, let's add the groups within the /Registry section::

       Groups
       {
         devGroup
         {
           Users = fstagni
         }
       }

You can keep playing with it (e.g. adding some properties), but for the moment this is enough.
