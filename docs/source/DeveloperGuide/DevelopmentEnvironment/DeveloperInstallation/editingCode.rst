.. _editing_code:

==================
Editing DIRAC code
==================

.. warning::
    This document has recently been updated for Python 3.
    If you need to do legacy development you can refer to the `v7r1 version of this page <https://dirac.readthedocs.io/en/rel-v7r1/DeveloperGuide/DevelopmentEnvironment/DeveloperInstallation/editingCode.html>`_.

******************************
Introduction and prerequisites
******************************

What is this for?
=================

Here we describe the suggested method for editing and unit-testing DIRAC code, and direct extensions of it.

What is this NOT for?
=====================

* This method is NOT specific for the WebAppDIRAC or Pilot code development, although several things that are described here can be applied.
* There will be no explanation of how to use Git. If you're unfamiliar with Git there are many excellent resources online. A particularly nice interactive tutorial can be found at `learngitbranching.js.org <https://learngitbranching.js.org/>`_.

Notes before continuing
=======================

*OS*: Linux or macOS should both be fine.
If you wish to develop on Windows the easiest solution is probably to use the `Windows Subsystem for Linux <https://docs.microsoft.com/en-us/windows/wsl/>`_.

*shell*: These instructions will work out of the box in any bourne compatible shell (bash/zsh/ksh) but other shells, such as fish/csh/tcsh, will work with only minor adjustments.

*repository*: DIRAC's GIT repositories are hosted on `GitHub <https://github.com/DIRACGrid>`_ and you will need a GitHub account to contribute.

Stuff you need to have installed
================================

*git*: DIRAC's version control system of choice is git and most computers will already have this installed. If not, you can easily install it by searching online for your operating system name and "install git".

*editor*: Almost any text editor is fine so if you already have a preference for vim/emacs/sublime/eclipse/pycharm/atom use that.
If you're new to development, Visual Studio Code is free modern editor with an extensive plugin ecosystem and many nice IDE like features.
Regardless of which editor you choose it's worth investigating what plugins are available.

*python*: DIRAC is written in Python so you will need to have a Python environment available. While there are many possible ways of doing this, the simplest option for most people is to use either conda or DIRACOS2 (Linux only). Details about how to use both options are included below.

.. _create-dev-env-conda:

Creating a development environment with Conda
---------------------------------------------

.. note::
    The Python packaging ecosystem is very old and as a result there has been a lot of fragmentation. Here are a few points that might be worthwhile to clarify:

    * **anaconda** vs **miniconda** vs **miniforge** vs **mambaforge**: `Anaconda Inc. <https://anaconda.com/>`_ is the original company behind the conda package manager, however the project has slowly evolved to be community project. Anaconda Inc. provides two free installers (**anaconda**/**miniconda**) as well as various paid support plans. Community efforts are centered around `conda-forge <https://conda-forge.org/>`_, a NumFOCUS sponsored project to provide recipes, build infrastructure and distributions for the conda package manager. While anaconda and conda-forge provided packages are mostly compatible there can be issues when mixing them. As a result, it is often easier to use the official `conda-forge installer bundles <https://github.com/conda-forge/miniforge/#download>`_ **miniforge** (``conda`` only) or **mambaforge** (with both ``conda`` and ``mamba``).
    * **conda** vs **mamba**: As conda has grown, some applications have had issues with ``conda`` being slow to solve and install some environments. Mamba is built upon ``conda`` and uses ``libsolv`` to provide a fast alternative install command. The two are interoperable and a common workflow is to replace calls to ``conda install``/``conda create``/``conda env create`` with ``mamba install``/``mamba create``/``mamba env create``. The only command which cannot be substituted is ``conda activate`` as it is implemented as a shell function to allow it to change environment variables.

The conda package manager's main selling point is that you can install binary packages in any location without needing system level privileges.
Ideally users should create an "environment" for a given purpose and "activate" it whenever they want that software to be used.
This has the advantage that unrelated activities (e.g. DIRAC development and physics analysis) can each have their own environments and any software installed won't cause the other activity to break.

Installing conda
^^^^^^^^^^^^^^^^

.. note::
    This step can be skipped if you already have conda installed.

Download and run one of the `"mambaforge" installers <https://github.com/conda-forge/miniforge/#mambaforge>`_.
The following snippet should work on most UNIX-like systems provided you adjust the URL on line one to download the installer for the appropriate platform:

.. code-block:: bash
    :linenos:

    wget https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh
    bash Mambaforge-Linux-x86_64.sh -b -p $HOME/mambaforge
    rm Mambaforge-Linux-x86_64.sh
    # Activate the environment manually the first time
    eval "$("$TMPDIR/mambaforge/bin/conda" shell.bash hook)"
    # Make it so that conda doesn't automatically activate the base environment
    conda config --set auto_activate_base false
    # Automatically make the "conda" shell function available
    conda init bash


Assuming you have already cloned DIRAC, you can create an environment for DIRAC development by running:

.. code-block:: bash
    :linenos:

    mamba env create --name dirac-development --file environment-py3.yml


Whenever you open a new terminal you can then activate the development environment by running:

.. code-block:: bash
    :linenos:

    conda activate dirac-development

Creating a development environment with DIRACOS2
------------------------------------------------

You can create a development environment in a new directory named ``diracos/`` by running:

.. code-block:: bash
    :linenos:

    wget https://github.com/DIRACGrid/DIRACOS2/releases/download/latest/DIRACOS-Linux-x86_64.sh
    bash DIRACOS-Linux-x86_64.sh
    rm DIRACOS-Linux-x86_64.sh

Whenever you wish to use the new environment you just need to source the ``diracos/diracosrc`` file:


.. code-block:: bash
    :linenos:

    source diracos/diracosrc

****************************************
Setting up your development installation
****************************************

The following steps will try to guide you on setting up a development installation for DIRAC.

Checking out the source
=======================

0. Go to a directory where you would like to do your development work (replacing ``MY_DEV_DIRECTORY`` as appropriate:

.. code-block:: bash
    :linenos:

    mkdir $HOME/MY_DEV_DIRECTORY/
    cd $HOME/MY_DEV_DIRECTORY/

1. Check out DIRAC source code inside a directory of your choosing. DIRAC source is hosted on *github.com*. Fork it (online!), then:

.. code-block:: bash
    :linenos:

    git clone https://github.com/YOUR_GITHUB_USERNAME/DIRAC.git

You must replace ``YOUR_GITHUB_USERNAME`` with the username that we have registered on github.
This will create a folder named ``DIRAC`` containing the DIRAC source code.

3. Now create a *remote* (in git terms) in the local repository called *upstream* that points to your source repository on GitHub.

.. code-block:: bash
    :linenos:

    cd DIRAC
    git remote add upstream https://github.com/DIRACGrid/DIRAC.git
    git fetch upstream

4. The next release of the DIRAC code lives on the ``integration`` branch and you can *checkout* it locally by running:

.. code-block:: bash
    :linenos:

    git checkout upstream/integration

Maintenance branches for existing releases are named ``rel-vXrY``.

Installing the local version
============================

The locally cloned source code can be installed inside your ``conda`` or ``DIRACOS2`` environment by running the following from inside your local repository:

.. code-block:: bash
    :linenos:

    pip install -e .[testing]

This creates an *editable* installation meaning any changes you make will be automatically discovered whenever you next ``import DIRAC``. Additionally the ``testing`` extra causes ``pip`` to install useful dependencies such as ``pytest`` and ``pycodestyle``.

Running unit tests
==================

Unit tests are used to provide simple, quick to run, tests which don't require any special environments. All of the unit tests are kept along side the DIRAC sources so you can run them with ``pytest`` by running:

.. code-block:: bash
    :linenos:

    pytest src/

Note the ``src/`` is important to avoid running the integration and certification tests. For more information about testing DIRAC please refer to :ref:`testing_environment`.

Linting
=======

A linter is a tool which *statically* (i.e. without executing it) makes checks on code for problems and bad practices.
In Python they are especially useful for catching errors before running the code, similarly to how a compiler can find issues with C++ code before it is executed. Linters are also often used for enforcing stylistic standards, you can find more about the conventions used with DIRAC at :ref:`coding_conventions`.

The main tools used in DIRAC are:

``pylint``
----------

This looks for code which might be invalid for some reason (e.g. undefined variable names or missing methods).
It can by ran with:

.. code-block:: bash
    :linenos:

    pylint src/

``pycodestyle``
---------------

This helps ensure the code meets the DIRAC style guidelines and can be executed by running:

.. code-block:: bash
    :linenos:

    pycodestyle

Repository structure
====================

Looking at the root directory:

   ls -al $DEVROOT/DIRAC/

will tell you a lot about the DIRAC code structure. Note that:

* there is a ``tests/`` directory
* there is a ``docs/`` directory
* there is a ``src/`` directory
* there are some base files (README, LICENCE, etc.) and some dotfiles, which will become useful reading further.

Unsurprisingly:

* ``tests`` contains tests - and specifically, it contains all the non-unit tests
* ``docs`` contains... documentation (including this very same page!)
* the ``src/`` directory contain the (python) code of the DIRAC systems

Ready!
======

You're ready for DIRAC development! (or at least, good part of it). What can you do with what you have just done?

1. Editing code (this is the obvious!)
2. Running unit tests
3. Running linters

So, this is "pure code"! And what you CAN'T do (yet)?

- You can't get a proxy
- you can't interact with configuration files nor with the Configuration System
- you can't run services, nor agents (no DIRAC components)

Next?
-----

This depends from you: if you are a casual developer, you can stop here,
and look into sections :ref:`check_your_installation` and the following :ref:`your_first_dirac_code`

Alternatively, if you want to do more, you may proceed to the section :ref:`stuff_that_run`.
