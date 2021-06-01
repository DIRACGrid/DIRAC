.. _stuff_that_run:

============================
Developing *stuff that runs*
============================

Which means developing for databases, services, agents, and executors. But also for the configuration service.

We'll guide you through using what we made in section :ref:`editing_code`
for developing and testing for databases, services, and agents. To do that, we'll create a "developer installation".
A developer installation is a *closed* installation: an installation that
can even be used while being disconnected from the Internet.

What is this for?
~~~~~~~~~~~~~~~~~~

Here we describe the suggested method for developing those part of DIRAC that "run", e.g. databases, services, and agents.
You need this type of installation for running so-called integration tests.


Do I need this?
~~~~~~~~~~~~~~~~~~

Maybe. It depends from what you want to develop.

If you only need to develop "clients" and "utilities" code, you won't need this.
But if you are going to change databases and DIRAC components, and if you want to run integration tests,
you better keep reading.


A docker-based isolated environment
===================================

position yourself in the DIRAC root directory and then run:

.. code-block:: bash

    ./integration_tests.py --help

This is a tool for running integration tests, that can be used also for developing purposes.
If you are interested in running one single integration test, let's say for the sake of example a server integration test, you can:

.. code-block:: bash

    ./integration_tests.py prepare-environment [FLAGS]
    ./integration_tests.py install-server

which (in some minutes) will give you a fully dockerized server setup (`docker container ls` will list the created container, and you can see what's going on inside with the standard `docker exec -it server /bin/bash`). Now, suppose that you want to run `WorkloadManagementSystem/Test_JobDB.py`.
The first thing to do is that you should first login in the docker container, by doing:

.. code-block:: bash

    ./integration_tests.py exec-server

Now you can run the test with:

.. code-block:: bash

    pytest LocalRepo/ALTERNATIVE_MODULES/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py (py3)

For py3 installations, You can find the logs of the services in `/home/dirac/ServerInstallDIR/diracos/runit/`


The Configuration Server (the CS)
=================================

At some point you'll need to understand how the DIRAC
`Configuration Service (CS) :ref:`dirac-cs-structure` works. I'll explain here briefly.

The CS is a layered structure: whenever
you access a CS information (e.g. using a "gConfig" object, see later),
DIRAC will first check into your local "dirac.cfg" file (it can be in your
home as ".dirac.cfg", or in *etc/* directory, see the link above). If this
will not be found, it will look for such info in the CS servers available.

When you develop locally, you don't need to access any CS server: instead, you need to have total control.
So, you need to work a bit on the local dirac.cfg file. There is not much else needed, just create your own etc/dirac.cfg.
The example that follows might not be easy to understand at a first sight, but it will become easy soon.
The syntax is extremely simple, yet verbose: simply, only brackets and equalities are used.

If you want to create an isolated installation just create a
*$DEVROOT/etc/dirac.cfg* file with::

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

Within the code we also provide a pre-filled example of dirac.cfg. You can get it simply doing (on the host)::

  cp $DEVROOT/DIRAC/docs/source/DeveloperGuide/AddingNewComponents/dirac.cfg.basic.example $DEVROOT/etc/dirac.cfg


Certificates
============

The docker-based setup will take care of the security layer, and the certificates. What's below is here for education.

By default, all connections to/from DIRAC services are secured, by with TLS/SSL security, so X.509 certificates need to be used.
This sub-section explains how to create (with few openSSL commands) a Certification Authority (CA), and with that sign user and host certificates.
This CA would be a in-house CA, so its certificates won't be trusted by anyone.

Still, you CAN run DIRAC services without any certificate.
The reason is that, while the use of TLS/SSL and certificates is the default, you can still go away without it,
simply disabling TLS/SSL. You'll see how later. So, if you find difficulties with this subsection, the good news is that you don't strictly need it.


Anyway: DIRAC understands certificates in *pem* format. That means that a certificate set will consist of two files.
Files ending in *cert.pem* can be world readable but just user writable since it contains the certificate and public key.
Files ending in *key.pem* should be only user readable since they contain
the private key. You will need two different sets certificates and the CA certificate that signed the sets.
The following commands should do the trick for you, by creating a fake CA, a fake user certificate, and a fake host certificate::

   cd $DEVROOT/DIRAC
   git checkout release/integration
   source tests/Jenkins/utilities.sh
   generateCA
   generateCertificates 365
   generateUserCredentials 365
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

Same process to register yourself, replace "/your/dn/goes/here"
(/Registry/Users/yourusername/DN option) with the result of::

   openssl x509 -noout -subject -in ~/.globus/usercert.pem | sed 's:^subject= ::g'

Is my installation correctly done?
==================================

A few, very simple checks. The first can be done by using the python interactive shell.
For these examples we use `iPython <http://ipython.org/>`_.

From the host:

.. code-block:: python

  In [1]: from DIRAC.Core.Base.Script import parseCommandLine

  In [2]: parseCommandLine()
  Out[2]: True

Was this good? If it wasn't, then you should probably hit the "previous" button of this guide.

So, what's that about? These 2 lines will initialize DIRAC.
They are used in several places, especially for the scripts: each and every script in DIRAC start with those 2 lines above.

Let's do one more check, still from the host:

.. code-block:: python

  In [14]: from DIRAC import gConfig

  In [15]: gConfig.getValue('/DIRAC/Setup')
  Out[15]: 'DeveloperSetup'

Was this good? If it wasn't, again, then you should probably hit the "previous" button of this guide.

The next test, also executed from the host,
will verify if you will be able to produce a proxy starting from the user certificates that you have created above::

   X509_CERT_DIR=$DEVROOT/etc/grid-security/certificates ./FrameworkSystem/scripts/dirac-proxy-init.py -ddd

Should return you a user proxy. You can verify the content and location of the proxy with::

   X509_CERT_DIR=$DEVROOT/etc/grid-security/certificates ./FrameworkSystem/scripts/dirac-proxy-info.py

Then, you can login on your running image (or your local installation) and try running a service, using the dips protocol.

Do not think about you just typed right now. It will become more clear later.
Please, look into :ref:`check_your_installation` section for further checks.


Ready!
======

You're (even more) ready for DIRAC development! What can you do with what you have just done?
Everything that was in the previous section, and on top:

1. Developing and testing code that "run"
2. Developing and testing code that requires integration between different components, like services and databases, but also agents
3. Running integration tests: please refer to :ref:`testing_environment` (towards the end) for more info.

And what you CAN'T do (yet)?

- you can't interact with a ''production'' setup, unless you use valid certificates
- you can't develop for web portal pages, because browsers won't accept self-signed certificates
