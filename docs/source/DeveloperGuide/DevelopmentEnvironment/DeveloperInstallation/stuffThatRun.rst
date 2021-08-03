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



Notes before continuing, on top of what is in section :ref:`editing_code`
=========================================================================

*OS*: a DIRAC server can be installed, as of today, only on SLC6 (Scientific Linux Cern 6) or CC7 (Cern CentOS 7).

The reason is that some binaries are proved to work only there (and TBH, support for CC7 is still partial),
and this includes several WMS (Workload Management) and DMS (Data Management), like *arc* or *gfal2*.
If you have to do many DMS (and partly WMS) developments, you should consider using SLC6 or CC7.
Or, using a Virtual Machine, or a docker instance. We'll go through this.


Stuff you need to have installed, on top of what is in section :ref:`editing_code`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your development machine is a SLC6 or a CC7, probably nothing of what follows.

If your development machine is a Scientific Linux 6 or a CentOS 7, probably nothing of what follows, but I wouldn't be too sure about it.

If your development machine is a CentOS 6 or a RedHat "equivalent", maybe nothing of what follows, but I am even less sure about it.

If you are not in any of the above cases, you still have a chance:
that, while developing for services or agents, none of them will need any "externals" library.
If this is your case, then you can still run locally on your development machine, which can be for example Ubuntu, or Debian, or also macOS.

Do you need to develop using external, compiled libraries like *arc*, *cream*, *gfal2*, *fts3*?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Then you probably need a SLC6 or CC7 image. If your development box is not one of them,
then you are presented with the alternatives of either using Virtual Machines, or Containers,
and so in this case you'll need to install something:

*docker*: `docker <https://docs.docker.com/>`_ is as of today a "standard" for applications' containerization.
The following examples use a DIRAC's base docker image for running DIRAC components.

*a hypervisor*, like *virtualbox*: if you don't want (or can't) use *docker*, you can use a virtual machine.

Whatever you need/decide, we will keep referring to your desktop as ''the host'', opposed to ''running image''
(which, as just explained, may coincide with the host).


General principles while using a virtual machine or a container
===============================================================

* You keep editing the code on your host
* $DEVROOT should be mounted from the host to the running image
* The host and the running image should share the same configuration (dirac.cfg file)
* The DIRAC components are going to run on the running image
* The clients that contact the running components can be started on the host
* The running image should have a ''host certificate'' for TLS verification and for running the components
* The host should have a ''user certificate''
* The user proxy should be created on the host for identifying the client

You can implement all the principles above in more than one way.


Using a Docker container [to expand]
====================================

The following steps will try to guide
you on setting up a development environment for DIRAC (or its extensions)
that combines what you have learned in :ref:`editing_code`
with a docker image with which you will run code that you develop.

Please see the Dockerfile that DIRAC provides at https://github.com/DIRACGrid/DIRAC/tree/integration/container
and Docker hub []


[to expand]


What's in this image?
~~~~~~~~~~~~~~~~~~~~~~

An dirac-install installed version of DIRAC (server).

[to expand]






Using a virtual machine [to expand]
===================================

Alternatively to docker...







Configuring DIRAC for running in an isolated environment
============================================================

We'll configure DIRAC to work in isolation. At this point, the key
becomes understanding how the DIRAC
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



Scripts (DIRAC commands)
=========================

DIRAC scripts can be found in (almost) every DIRAC package. For example in DIRAC.WorkloadManagementSystem.scripts.
You can invoke them directly, or you can run the command::

  dirac-deploy-scripts

which will inspect all these directories (including possible DIRAC extensions) and deploy the found scripts in $DEVROOT/scripts.
Developers can then persist this directory in the $PATH.


Certificates
============

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

We will now do few, very simple checks. The first can be done by using the python interactive shell.
For these examples I will actually use `iPython <http://ipython.org/>`_.

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
