.. _stuff_that_run:

===========================
Developing "stuff that run"
===========================

Which means developing for databases, services, and agents. But also for the configuration service.

We'll guide you through using what we made in section :ref:`editing_code`
for developing and testing for databases, services, and agents. To do that, we'll create a "developer installation".
A developer installation is a *closed* installation: an installation that
can even be used while being disconnected from the Internet.


What is this for?
-----------------

Here we describe the suggested method for developing those part of DIRAC that "run", e.g. databases, services, and agents.
You need this type of installation for running so-called unit tests.


Do I need this?
-----------------

Maybe. It depends from you want to develop.

If you only need to develop "clients" and "utilities" code, you won't need this.
But if you are going to change databases and DIRAC components, and if you want to run integration tests,
you better keep reading.



Notes before continuing, on top of what is in section :ref:`editing_code`
---------------------------------------------------------------------------

*OS*: a DIRAC server can be installed, as of today, only on SLC6/CC7 OS.
The reason being some binaries are proved to work only there,
and this includes several DMS (Data Management) libraries.
If you have to do many DMS development, you should consider using SLC6 or CC7.
Or, using a Virtual Machine, or a docker instance. We'll go through this.


Stuff you need to have installed, on top of what is in section :ref:`editing_code`
-------------------------------------------------------------------------------------

*docker*: `docker <https://docs.docker.com/>`_ is as of today a "standard" for applications' containerization.
The following examples use a DIRAC's base docker image for running DIRAC components.

If you don't want to use *docker*, you can use a virtual machine.


Setting up your development installation
==================================================

The following steps will try to guide
you on setting up a development environment for DIRAC (or its extensions)
that combines what you have learned in :ref:`editing_code`
with a docker image with which you will run code that you develop.


Install the DIRAC docker image
------------------------------

[to expand]


What's in this image?
----------------------

[to expand]


Sorting out the PATHs
---------------------

[to expand]


Configure DIRAC
---------------

We'll configure DIRAC to work in isolation. At this point, the key
becomes understanding how the DIRAC
`Configuration Service (CS) <http://diracgrid.org/files/docs/AdministratorGuide/Configuration/ConfigurationStructure/index.html>`_
works. I'll explain here briefly. The CS is a layered structure: whenever
you access a CS information (e.g. using a "gConfig" object, see later),
DIRAC will first check into your local "dirac.cfg" file (it can be in your
home as .dirac.cfg, or in etc/ directory, see the link above). If this
will not be found, it will look for such info in the CS servers available.

When you develop locally, you don't need to access any CS server: instead, you need to have total control.
So, you need to work a bit on the local dirac.cfg file. There is not much else needed, just create your own etc/dirac.cfg.
The example that follows might not be easy to understand at a first sight, but it will become easy soon.
The syntax is extremely simple, yet verbose: simply, only brackets and equalities are used.

If you want to create an isolated installation just create a
*$DEVROOT/etc/dirac.cfg* file with (create the etc directory first)::

   DIRAC
   {
     Setup = DeveloperSetup
     Setups
     {
       DeveloperSetup
       {
         Framework = DevInstance
         Test = DevInstance
       }
     }
   }
   Systems
   {
     Framework
     {
       DevInstance
       {
         URLs
         {
         }
         Services
         {
         }
       }
     }
     Test
     {
       DevInstance
       {
         URLs
         {
         }
         Services
         {
         }
       }
     }
   }
   Registry
   {
     Users
     {
       yourusername
       {
         DN = /your/dn/goes/here
         Email = youremail@yourprovider.com
       }
     }
     Groups
     {
       devGroup
       {
         Users = yourusername
         Properties = CSAdministrator, JobAdministrator, ServiceAdministrator, ProxyDelegation, FullDelegation
       }
     }
     Hosts
     {
       mydevbox
       {
         DN = /your/box/dn/goes/here
         Properties = CSAdministrator, JobAdministrator, ServiceAdministrator, ProxyDelegation, FullDelegation
       }
     }
   }


Certificates
-------------

DIRAC understands certificates in *pem* format. That means that a certificate set will consist of two files.
Files ending in *cert.pem* can be world readable but just user writable since it contains the certificate and public key.
Files ending in *key.pem* should be only user readable since they contain
the private key. You will need two different sets certificates and the CA certificate that signed the sets.
The following commands should do the trick for you, by creating a fake CA, a fake user certificate, and a fake host certificate::

   cd $DEVROOT/DIRAC
   git checkout release/integration
   source tests/Jenkins/utilities.sh
   generateCertificates 365
   generateUserCredentials
   mkdir -p ~/.globus/
   cp $DEVROOT/user/*.{pem,key} ~/.globus/
   mv ~/.globus/client.key ~/.globus/userkey.pem
   mv ~/.globus/client.pem ~/.globus/usercert.pem

Now we need to register those certificates in DIRAC. To do so you
must modify *$DEVROOT/etc/dirac.cfg* file and set the correct
certificate DNs for you and your development box. 
To register the host, replace "/your/box/dn/goes/here"
(/Registry/Hosts/mydevbox/DN option) with the result of::

   openssl x509 -noout -subject -in $DEVROOT/etc/grid-security/hostcert.pem | sed 's:^subject= ::g'

Same process to register yourself, replace "/your/box/dn/goes/here"
(/Registry/Users/yourusername/DN option) with the result of::

   openssl x509 -noout -subject -in ~/.globus/usercert.pem | sed 's:^subject= ::g'

Is my installation correctly done?
--------------------------------------

We will now do few, very simple checks. The first can be done by using the python interactive shell.
For these examples I will actually use `iPython <http://ipython.org/>`_, which is a highly recommended shell.

.. code-block:: python

  In [1]: from DIRAC.Core.Base.Script import parseCommandLine

  In [2]: parseCommandLine()
  Out[2]: True

Was this good? If it wasn't, then you should probably hit the "previous" button of this guide.

So, what's that about? These 2 lines will initialize DIRAC.
They are used in several places, especially for the scripts: each and every script in DIRAC start with those 2 lines above.

Let's do one more check:

.. code-block:: python

  In [14]: from DIRAC import gConfig

  In [15]: gConfig.getValue('/DIRAC/Setup')
  Out[15]: 'DeveloperSetup'

Was this good? If it wasn't, again, then you should probably hit the "previous" button of this guide.

Do not think about you just typed right now. It will become more clear later. 
Please, look into :ref:`check_your_installation` section for further checks. 


Ready!
------

You're (even more) ready for DIRAC development!
