.. -*- mode: rst -*-

.. image:: https://travis-ci.org/DIRACGrid/DIRAC.svg?branch=master
   :target: https://travis-ci.org/DIRACGrid/DIRAC
   :alt: Build Status

.. image:: https://readthedocs.org/projects/dirac/badge/?version=latest
   :target: http://dirac.readthedocs.io/en/latest/
   :alt: Documentation Status


Integration branch:

.. image:: https://travis-ci.org/DIRACGrid/DIRAC.svg?branch=integration
  :target: https://travis-ci.org/DIRACGrid/DIRAC
  :alt: Build Status

.. image:: https://readthedocs.org/projects/dirac/badge/?version=integration
  :target: http://dirac.readthedocs.io/en/integration/
  :alt: Documentation Status



DIRAC
=====

DIRAC (Distributed Infrastructure with Remote Agent Control) INTERWARE is a software framework for distributed computing providing a complete solution to one or more user community requiring access to distributed resources. DIRAC builds a layer between the users and the resources offering a common interface to a number of heterogeneous providers, integrating them in a seamless manner, providing interoperability, at the same time as an optimized, transparent and reliable usage of the resources.

DIRAC has been started by the `LHCb collaboration <https://lhcb.web.cern.ch/lhcb/>`_ who still maintains it. It is now used by several communities (AKA VO=Virtual Organizations) for their distributed computing workflows.


Important links
===============

- Official source code repo: https://github.com/DIRACGrid/DIRAC
- HTML documentation (stable release): http://diracgrid.org (http://dirac.readthedocs.io/en/latest/index.html)
- Issue tracker: https://github.com/DIRACGrid/DIRAC/issues
- Support Mailing list: https://groups.google.com/forum/#!forum/diracgrid-forum
- Developers Mailing list: https://groups.google.com/forum/#!forum/diracgrid-develop

Install
=======

There are basically 2 types of installations: client, and server.

For DIRAC client installation instructions, see the `web page <http://dirac.readthedocs.io/en/latest/UserGuide/GettingStarted/InstallingClient/index.html>`_.

For DIRAC server installation instructions, see the `web page <http://dirac.readthedocs.io/en/latest/AdministratorGuide/InstallingDIRACService/index.html>`_.

Development
===========

Contributing
~~~~~~~~~~~~

DIRAC is a fully open source project, and you are welcome to contribute to it. A list of its main authors can be found `here <AUTHORS.rst>`_ A detailed explanation on how to contribute to DIRAC can be found in `this page <http://dirac.readthedocs.io/en/latest/DeveloperGuide/index.html>`_. For a quick'n dirty guide on how to contribute, simply:

- fork the project
- work on a branch
- create a Pull Request, target the "integration" branch.

Code quality
~~~~~~~~~~~~

The contributions are subject to reviews.

Pylint, and pep8 style checker are run regularly on the source code. The .pylintrc file defines the expected coding rules and peculiarities (e.g.: tabs consists of 2 spaces instead of 4). Each Pull Request is checked for pylint and pep8 compliance.

Testing
~~~~~~~

Unit tests are provided within the source code. Integration, regression and system tests are instead in the tests directory. Run pytest to run all unit tests.
