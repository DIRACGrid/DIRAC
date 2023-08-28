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

DIRAC is written in python 3.9.

Status rel-v8r0 series (stable, recommended):

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/Basic%20tests/badge.svg?branch=rel-v8r0
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22Basic+tests%22+branch%3Arel-v8r0
   :alt: Basic Tests Status

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/pilot%20wrapper/badge.svg?branch=rel-v8r0
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22pilot+wrapper%22+branch%3Arel-v8r0
   :alt: Pilot Wrapper Status

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/Integration%20tests/badge.svg?branch=rel-v8r0
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22Integration+tests%22+branch%3Arel-v8r0
   :alt: Integration Tests Status

.. image:: https://readthedocs.org/projects/dirac/badge/?version=rel-v8r0
   :target: http://dirac.readthedocs.io/en/rel-v8r0/
   :alt: Documentation Status


Status integration branch (devel):

.. image:: https://github.com/DIRACGrid/DIRAC/workflows/Basic%20tests/badge.svg?branch=integration
   :target: https://github.com/DIRACGrid/DIRAC/actions?query=workflow%3A%22Basic+tests%22+branch%3Aintegration
   :alt: Basic Tests Status

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
- Discussions: https://github.com/DIRACGrid/DIRAC/discussions
- [ARCHIVED] Support Mailing list: https://groups.google.com/forum/#!forum/diracgrid-forum

Install
=======

There are basically 2 types of installations: client, and server.

For DIRAC client installation instructions, see the `web page <http://dirac.readthedocs.io/en/latest/UserGuide/GettingStarted/InstallingClient/index.html>`__.

For DIRAC server installation instructions, see the `web page <https://dirac.readthedocs.io/en/latest/AdministratorGuide/ServerInstallations/InstallingDiracServer.html>`__.

DIRAC 8.0 drops support for Python 2 based clients and servers.

There are three available options for installation:

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

For the full development guide see `here <https://dirac.readthedocs.io/en/integration/DeveloperGuide/DevelopmentEnvironment/index.html>`__, some of the most important details are included below.

Contributing
~~~~~~~~~~~~

DIRAC is a fully open source project, and you are welcome to contribute to it. A list of its main authors can be found `here <AUTHORS.rst>`__ A detailed explanation on how to contribute to DIRAC can be found in `this page <http://dirac.readthedocs.io/en/latest/DeveloperGuide/index.html>`_. For a quick'n dirty guide on how to contribute, simply:

- `Fork the project <https://docs.github.com/en/get-started/quickstart/fork-a-repo>`_ inside the GitHub UI
- Clone locally and create a branch for each change

   .. code-block:: bash

      git clone git@github.com:$GITHUB_USERNAME/DIRAC.git
      cd DIRAC
      git remote add upstream git@github.com:DIRACGrid/DIRAC.git
      git fetch --all
      git checkout upstream/integration
      git checkout -b my-feature-branch
      git push -u origin my-feature-branch

- `Create a Pull Request <https://docs.github.com/en/articles/about-pull-requests>`_, targeting the "integration" branch.

Code quality
~~~~~~~~~~~~

To ensure the code meets DIRAC's coding conventions we recommend installing ``pre-commit`` system wide using your operating system's package manager.
Alteratively, ``pre-commit`` is included in the Python 3 development environment, see the `development guide <https://dirac.readthedocs.io/en/integration/DeveloperGuide/DevelopmentEnvironment/DeveloperInstallation/editingCode.html>`_ for details on how to create one.

Once ``pre-commit`` is installed you can enable it by running:

.. code-block:: bash

   pre-commit install --allow-missing-config

Code formatting will now be automatically applied before each commit.

Testing
~~~~~~~

Unit tests are provided within the source code and can be ran using ``pytest``.
Integration, regression and system tests are instead in the ``DIRAC/tests/`` directory.

Acknowledgements
~~~~~~~~~~~~~~~~

This work is co-funded by the EOSC-hub project (Horizon 2020) under Grant number 777536

|eu-logo| |eosc-hub-web|

.. |eu-logo| image:: https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/docs/source/_static/eu-logo.jpeg

.. |eosc-hub-web| image:: https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/docs/source/_static/eosc-hub-web.png
