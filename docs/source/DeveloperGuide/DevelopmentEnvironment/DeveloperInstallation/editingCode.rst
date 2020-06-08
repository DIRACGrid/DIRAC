.. _editing_code:

==================
Editing DIRAC code
==================

What is this for?
=================

Here we describe the suggested method for editing and unit-testing DIRAC code, and direct extensions of it.


What is this NOT for?
=====================

* This method is NOT specific for the WebAppDIRAC or Pilot code development, although several things that are described here can be applied.
* This method will NOT work out of box if you need 3rd party python libraries that are not pip installable.



Notes before continuing
=======================

*OS*: any \*nix should be fine
(maybe even windows is fine but I would not know how).
Examples below are given for Linux (and specifically, the writer used Ubuntu)

*shell*: examples below are given in bash, and are proven to work also in zsh.
Any csh like tcsh should not pose a problem.

*repository*: as already explained,
DIRAC's GIT repositories are hosted on `GitHub <https://github.com/DIRACGrid>`_.
for which you need to have an account before continuing.



Stuff you need to have installed
================================

*python*: make sure python 2.7.9+ (possibly 2.7.13) is installed and set as default
(beware: the latest versions of Ubuntu use python 3.X as default, SLC6 use python 2.6 as default).

*python-pip*: the tool for installing python packages hosted
on `PyPi <https://pypi.python.org/pypi>`_.

*git*: DIRAC's version control system of choice is git, so install it.

*basic packages*: you will need at least gcc, python-devel (python all-dev),
openssl-devel (libssl-dev), mysql-client, libmysqlclient-dev,
libfreetype6-dev, libncurses5-dev, libjpeg-dev.
The names above are OS-dependent, distribution dependent, and version dependent,
so you'll need to figure it out by yourself how to install them.
Some packages you may need to add for Ubuntu: libcurl4-gnutls-dev, libgcrypt20-dev, libnghttp2-dev, libpsl-dev, libldap2-dev.
Some equivalent packages for Fedora/CentOS: python-devel, openssl-devel, mysql, ncurses-libs freetype, libjpeg-devel, MySQL-python.
If you are using a OSX machine, then you may end up in more problems than using a linux box.

*editor*: get your favorite one.
Examples include IDE like Eclipse or PyCharm, or whatever you prefer
(vim, sublime, atom...) - anyway you'll need some plug-ins!
I think atom and especially sublime (with the *anaconda* plugin) are excellent choices.




Setting up your development installation
----------------------------------------

The following steps will try to guide
you on setting up a development installation for DIRAC


Checking out the source
=======================

0. Go to a clean directory, e.g. $HOME/pyDevs/.

From now on we will call that directory *$DEVROOT*, just for our own convenience

1. ::

     export DEVROOT=$PWD && export WORKSPACE=$PWD

(persist this in the way you prefer)

2. Check out DIRAC source code. DIRAC source is hosted on *github.com*. Fork it (online!), then::

     git clone https://github.com/YOUR_GITHUB_USERNAME/DIRAC.git

Obviously, you must replace 'YOUR_GITHUB_USERNAME' with the username that we have registered on github.
This will create a *$DEVROOT/DIRAC* for you and the git repository will be cloned in.

3. This will create a *remote* pointer (in git terms) in the local git
repository called *origin* that points to your source repository on GitHub.
In that repository you will publish your code to be released. But all the releases
will be done from the https://github.com/DIRACGrid/DIRAC repository. You
need to define a *remote* for that repository to be able to pull newly
released changes into your working repo. We will name that repository *release*::

   cd DIRAC
   git remote add release https://github.com/DIRACGrid/DIRAC.git
   git fetch release


Repository structure
====================

Just looking at the root directory::

   ls -al $DEVROOT/DIRAC/

will tell you a lot about the DIRAC code structure. Note that:

* there is a tests/ directory
* there is a docs/ directory
* there are several \*System/ directories, one called Core, one Worfklow... maybe something else depending on the version you are looking at
* there is an \__init__.py file
* there are some base files (README, LICENCE, etc.) and some dotfiles, which will become useful reading further.

Unsurprisingly:

* "tests" contains tests - and specifically, it contains all the non-unit tests
* "docs" contains... documentation (including this very same page!)
* all the \*System/ directories contain the (python) code of the DIRAC systems


Adding an extension
===================

You can add an extension of DIRAC, of course.
The repository structure may be the same of the DIRAC one, or something slightly different.
The only important thing is what you are going to put in the $PYTHONPATH.


Installing the dependencies
===========================

First, make sure that `setuptools` and `pip` are at the latest versions::

   [sudo] pip install --upgrade setuptools
   [sudo] pip install --upgrade pip

We'll use `virtualenv <https://virtualenv.readthedocs.org/en/latest/>`_.
and `virtualenvwrapper <https://virtualenvwrapper.readthedocs.org/en/latest/>`_.
for working in a separate virtual python environment,
and for creating and deleting such environments::

   [sudo] pip install virtualenv
   [sudo] pip install virtualenvwrapper
   export WORKON_HOME=~/Envs
   mkdir -p $WORKON_HOME
   source /usr/local/bin/virtualenvwrapper.sh

Now, let's create the virtual environment, and populate it::

   mkvirtualenv DIRAC # this creates the "DIRAC"
   pip install -r $DEVROOT/DIRAC/requirements.txt

This will create a virtual python environment in which we can install
all python packages that DIRAC use
(this may take a while, and you might need to manually install some package
from your distribution).

Some useful commands::

   "pip install -r requirements.txt --upgrade" will upgrade the packages
   "deactivate" will exit from a virtualenv
   "workon DIRAC" will get you back in DIRAC virtualenv


**NOTE**: A (maybe better) *alternative* to virtualenv is conda,
and specifically `miniconda <https://conda.io/en/latest/miniconda.html>`_.
Use it if you like, if you understand virtualenv you can understand conda environments.

Some useful conda commands::

  conda env create $DEVROOT/DIRAC/environment.yml  # will create a conda environment named
                                                   # dirac-development and install the
						   # prerequiste packages

  conda activate dirac-development #  will get you in the dirac-development environment
  # or on conda versions prior to 4.6
  source activate dirac-development

  conda deactivate  # will exit from the conda environment
  # or on conda versions prior to 4.6 
  source deactivate
     
for other useful conda commands for managing environments, you can check this `link <https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html>`_.


Adding to the PYTHONPATH
========================

You may either add the PATH to the global PYTHONPATH, as following::

   export PYTHONPATH=$PYTHONPATH:$DEVROOT

And repeat for the extension development root,
or use virtualenv for managing the path,
using `add2virtualenv <http://virtualenvwrapper.readthedocs.io/en/latest/command_ref.html#add2virtualenv>`


Ready!
======

You're ready for DIRAC development! (or at least, good part of it). What can you do with what you have just done?

1. Editing code (this is the obvious!)
2. Running unit tests: please refer to :ref:`testing_environment` for more info.
3. Running linters: please refer to :ref:`code_quality` for more info

So, this is "pure code"! And what you CAN'T do (yet)?

- You can't get a proxy
- you can't interact with configuration files nor with the Configuration System
- you can't run services, nor agents (no DIRAC components)


Next?
-----

This depends from you: if you are a casual developer, you can stop here,
and look into sections :ref:`check_your_installation` and the following :ref:`your_first_dirac_code`

Alternatively, if you want to do more, you may proceed to the section :ref:`stuff_that_run`.
