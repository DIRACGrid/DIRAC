.. -*- mode: rst -*-

DIRAC
=====

DIRAC (Distributed Infrastructure with Remote Agent Control) INTERWARE is a software framework for distributed computing providing a complete solution to one or more user community requiring access to distributed resources. DIRAC builds a layer between the users and the resources offering a common interface to a number of heterogeneous providers, integrating them in a seamless manner, providing interoperability, at the same time as an optimized, transparent and reliable usage of the resources.

DIRAC has been started by the `LHCb collaboration <https://lhcb.web.cern.ch/lhcb/>`_ who still maintains it. It is now used by several communities (AKA VO=Virtual Organizations) for their distributed computing workflows.


Important links
===============

- Official source code repo: https://github.com/DIRACGrid/DIRAC
- HTML documentation (stable release): http://diracgrid.org
- Issue tracker: https://github.com/DIRACGrid/DIRAC/issues
- Support Mailing list: https://groups.google.com/forum/#!forum/diracgrid-forum
- Developers Mailing list: https://groups.google.com/forum/#!forum/diracgrid-develop

Install
=======

For more detailed installation instructions, see the `web page <http://diracgrid.org/files/docs/DeveloperGuide/>`_.

Development
===========

Contributing
~~~~~~~~~~~~

A tutorial on how to contribute to DIRAC can be found in `this page <http://diracgrid.org/files/docs/DeveloperGuide/AddingNewComponents/index.html>`_. 

Code quality
~~~~~~~~~~~~

The contributions are subject to reviews.

Pylint is run regularly on the source code. The .pylintrc file defines the expected coding rules and peculiarities (e.g.: tabs consists of 2 spaces instead of 4)

Testing
~~~~~~~

Unit tests are provided within the source code. Integration, regression and system tests are instead in the tests directory. py.test is an excellent library for running the tests.
