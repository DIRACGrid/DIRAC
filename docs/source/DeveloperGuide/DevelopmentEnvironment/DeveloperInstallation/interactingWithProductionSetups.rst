.. _interacting_with_prod_env:

===========================================
Interacting with the production environment
===========================================

Which means developing, while interacting with an existing production environment.

In the end, it's a matter of being correctly authenticated and authorized.
So, the only real thing that you need to have is:

- a DIRAC developer installation
- a (real) certificate, that is recognized by your server installation
- a dirac.cfg that include the (real) setup of the production environment that you want to connect to (in DIRAC/Setup section) 
- a dirac.cfg that include the (real) URL of the production Configuration server.

The last 2 bullets can be achieved with the following command::

   dirac-configure -S MyProductionSetup -C dips://some.whe.re:9135/Configuration/Server --SkipCAChecks

Or simply by manual editing the dirac.cfg file.

From now on, you need to be extremely careful with whatever you do, 
because your development installation ends up not being anymore a "close" installation.
