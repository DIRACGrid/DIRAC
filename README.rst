.. -*- mode: rst -*-

DIRAC
=====

.. image:: https://badge.fury.io/py/DIRAC.svg
    :target: https://badge.fury.io/py/DIRAC
.. image:: https://img.shields.io/conda/vn/conda-forge/dirac-grid
    :target: https://github.com/conda-forge/dirac-grid-feedstock
.. image:: https://zenodo.org/badge/DOI/10.5281/zenodo.1451647.svg
    :target: https://doi.org/10.5281/zenodo.1451647

DIRAC is an interware, meaning a software framework for distributed computing.

DIRAC provides a complete solution to one or more user community requiring access to distributed resources. DIRAC builds a layer between the users and the resources offering a common interface to a number of heterogeneous providers, integrating them in a seamless manner, providing interoperability, at the same time as an optimized, transparent and reliable usage of the resources.

DIRAC has been started by the `LHCb collaboration <https://lhcb.web.cern.ch/lhcb/>`_ who still maintains it. It is now used by several communities (AKA VO=Virtual Organizations) for their distributed computing workflows.

DIRAC is written in python 2.7.13 and transitioning to python 3.

Status master branch (stable):

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/Basic%20tests/badge.svg?branch=rel-v7r1
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22Basic+tests%22+branch%3Arel-v7r1
   :alt: Basic Tests Status

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/dirac-install/badge.svg?branch=rel-v7r1
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22dirac-install%22+branch%3Arel-v7r1
   :alt: Dirac Install Status

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/pilot%20wrapper/badge.svg?branch=rel-v7r1
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22pilot+wrapper%22+branch%3Arel-v7r1
   :alt: Pilot Wrapper Status

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/Integration%20tests/badge.svg?branch=rel-v7r1
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22Integration+tests%22+branch%3Arel-v7r1
   :alt: Integration Tests Status

.. image:: https://readthedocs.org/projects/dirac/badge/?version=latest
   :target: http://dirac.readthedocs.io/en/latest/
   :alt: Documentation Status


Status integration branch (devel):

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/Basic%20tests/badge.svg?branch=integration
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22Basic+tests%22+branch%3Aintegration
   :alt: Basic Tests Status

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/dirac-install/badge.svg?branch=integration
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22dirac-install%22+branch%3Aintegration
   :alt: Dirac Install Status

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/pilot%20wrapper/badge.svg?branch=integration
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22pilot+wrapper%22+branch%3Aintegration
   :alt: Pilot Wrapper Status

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/Integration%20tests/badge.svg?branch=integration
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22Integration+tests%22+branch%3Aintegration
   :alt: Integration Tests Status

.. image:: https://readthedocs.org/projects/dirac/badge/?version=integration
   :target: http://dirac.readthedocs.io/en/integration/
   :alt: Documentation Status

Important links
===============

- Official source code repo: https://github.com/DIRACGrid/DIRAC
- HTML documentation (stable release): http://diracgrid.org (http://dirac.readthedocs.io/en/latest/index.html)
- Issue tracker: https://github.com/DIRACGrid/DIRAC/issues
- Support Mailing list: https://groups.google.com/forum/#!forum/diracgrid-forum

Install
=======

There are basically 2 types of installations: client, and server.

For DIRAC client installation instructions, see the `web page <http://dirac.readthedocs.io/en/latest/UserGuide/GettingStarted/InstallingClient/index.html>`__.

For DIRAC server installation instructions, see the `web page <https://dirac.readthedocs.io/en/latest/AdministratorGuide/ServerInstallations/InstallingDiracServer.html>`__.

The supported distributions are EL6 (e.g. SLC6) and EL7 (e.g. CC7).

As of DIRAC 7.2 there is also **experimental** support for Python 3 based clients. There are three available options for installation:

.. _conda: https://conda.io/en/latest/index.html
.. |conda| replace:: **Conda**
.. _mamba: https://github.com/mamba-org/mamba#the-fast-cross-platform-package-manager
.. |mamba| replace:: **Mamba**
.. _condaforge: https://github.com/mamba-org/mamba#the-fast-cross-platform-package-manager
.. |condaforge| replace:: **conda-forge**

1. **DIRACOS2:** This is the only fully supported method, see the `DIRACOS 2 documentation <https://github.com/DIRACGrid/DIRACOS2/#installing-diracos2>`__.
2. |conda|_ **/** |mamba|_ **from** |condaforge|_ **:**
   We recommend making a new environment for DIRAC using

   .. code-block:: bash

     mamba create --name my-dirac-env -c conda-forge dirac-grid
     conda activate my-dirac-env

3. **Pip:** Provided suitable dependencies are available DIRAC can be installed with ``pip install DIRAC``. Support for installing the dependencies should be sought from the upstream projects.

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

Pylint, and pep8 style checker are run regularly on the source code. The .pylintrc file defines the expected coding rules and peculiarities (e.g.: tabs consists of 2 spaces instead of 4).
Each Pull Request is checked for pylint and pep8 compliance.

Each PR is a also subject to check for python 3 compatibility.
If you are issuing PRs that are devoted to future versions of DIRAC (so, not for patch releases),
for each of the python files touched please run (and react to)::

   pylint --rcfile=tests/.pylintrc3k --py3k --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" --extension-pkg-whitelist=numpy path/to/file.py


Testing
~~~~~~~

Unit tests are provided within the source code. Integration, regression and system tests are instead in the DIRAC/tests/ directory.
Run pytest to run all unit tests (it will include the coverage).

Acknowledgements
~~~~~~~~~~~~~~~~

This work is co-funded by the EOSC-hub project (Horizon 2020) under Grant number 777536

|eu-logo| |eosc-hub-web|

.. |eu-logo| image:: https://github.com/DIRACGrid/DIRAC/raw/integration/docs/source/_static/eu-logo.jpeg

.. |eosc-hub-web| image:: https://github.com/DIRACGrid/DIRAC/raw/integration/docs/source/_static/eosc-hub-web.png
