.. _stuff_that_run:

============================
Developing *stuff that runs*
============================

Which means developing for databases, services, agents, and executors. But also for the configuration service.

The procedure described in :ref:`running_integration_tests` is what we advice to use for this case.


Do I need this?
~~~~~~~~~~~~~~~~~~

Maybe. It depends from what you want to develop.

If you only need to develop "clients" and "utilities" code, you won't need this.
But if you are going to change databases and DIRAC components, and if you want to run integration tests,
you better keep reading.


A docker-based isolated environment
===================================

Position yourself in the DIRAC root directory and then run:

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

    pytest LocalRepo/ALTERNATIVE_MODULES/DIRAC/tests/Integration/WorkloadManagementSystem/Test_JobDB.py

You can find the logs of the services in `/home/dirac/ServerInstallDIR/diracos/runit/`


The Configuration Server (the CS)
=================================

At some point you'll need to understand how the DIRAC
Configuration Service (CS) :ref:`dirac-cs-structure` works. I'll explain here briefly.

The CS is a layered structure: whenever
you access a CS information (e.g. using a "gConfig" object, see later),
DIRAC will first check into your local "dirac.cfg" file (it can be in your
home as ".dirac.cfg", or in *etc/* directory, see the link above). If this
will not be found, it will look for such info in the CS servers available.

When you develop locally, you don't need to access any CS server: instead, you need to have total control.
The docker-based setup created by `integration_tests.py` will take care of creating the dirac.cfg file for you.

In case you want to work outside of the setup created by `integration_tests.py`,
we also provide a pre-filled example of dirac.cfg. You can get it simply doing::

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
