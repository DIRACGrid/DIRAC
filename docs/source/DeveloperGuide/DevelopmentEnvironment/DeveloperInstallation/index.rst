.. _developer_installation:

======================================
Setting up a development installation
======================================

We'll do a step-by-step set up of what we call a developer installation.
A developer installation is a *closed* installation: an installation that
can even be used while being disconnected from the Internet.


What is this for?
-----------------

Here we describe the suggested method for developing DIRAC server and client code, and direct extensions of it.


What is this NOT for?
---------------------

* This method is NOT for the WebAppDIRAC code development.
* This method is NOT for the Pilot code development, although few things may apply.
* This method will NOT work out of box if you need 3rd party python libraries that are not on PyPi.
* This method will NOT work out of box if you need to contact non-DIRAC services



Notes before continuing
------------------------

*OS*: any *nix should be fine
(maybe even windows is fine but I would not know how).
Examples below are given for Linux (and specifically, the writer used Ubuntu)

*shell*: examples below are given in bash, and are proven to work also in zsh.
Any csh like tcsh should not pose a problem.

*repository* and as already explained,
DIRAC's GIT repositories are hosted on `GitHub <https://github.com/DIRACGrid>`_.
for which you need to have an account before continuing.

*OS, more notes*: a DIRAC server can be installed, as of today, only on SLC6/CC7 OS.
The reason being some binaries are proved to work only there, 
and this includes several DMS (Data Management) libraries.
If you have to do many DMS development, you should consider using SLC6 or CC7.


Stuff you need to have installed
---------------------------------

*python*: make sure python 2.7.x is installed and set as default 
(beware: latest Ubuntu use python 3.5 as default).

*python-pip*: the tool for installing python packages hosted
on `PyPi <https://pypi.python.org/pypi>`_.

*git*: DIRAC's version control system of choice is git, so install it.

*basic packages*: you will need at least gcc, python-devel (python all-dev),
openssl-devel (libssl-dev), mysql-client, libmysqlclient-dev,
libfreetype6-dev, libncurses5-dev, libjpeg-dev.
The names above are OS-dependant, distribution dependant, and version dependant,
so you'll need to figure it out by yourself how to install them.
Some equivalent packages for Fedora/CentOS: python-devel, openssl-devel, mysql, ncurses-libs freetype, libjpeg-devel, MySQL-python.

*editor*: get your favourite one.
Examples include IDE like Eclipse or PyCharm, or whatever you prefer
(vim, sublime, atom...) - anyway you'll need some plugins!




Setting up your development installation
==================================================

The following steps will try to guide
you on setting up a development installation for DIRAC


Checking out the source
-------------------------

0. Go to a clean directory, e.g. $HOME/pyDevs/.

From now on we will call that directory *$DEVROOT*

1.

   export DEVROOT=$PWD && export WORKSPACE=$PWD

(persist this in the way you prefer)

2. Check out DIRAC source code. DIRAC source is hosted on *github.com*. Fork it, then::

   git clone https://github.com/YOUR_GITHUB_USERNAME/DIRAC.git

Obviously, we must replace 'YOUR_GITHUB_USERNAME' with the username that we have registered on github. 
This will create a *$DEVROOT/DIRAC* for you and the git repository will be cloned in.

3. This will create a *remote* pointer ( in git terms ) in the local git
repository called *origin* that points to your source repository on GitHub. 
In that repository you will publish your code to be released. But all the releases
will be done from the https://github.com/DIRACGrid/DIRAC repository. You
need to define a *remote* for that repository to be able to pull newly
released changes into your working repo. We will name that repository *release*::

   cd DIRAC
   git remote add release https://github.com/DIRACGrid/DIRAC.git
   git fetch release


Repository structure
--------------------

Just looking at the root directory::

   ls -al $DEVROOT/DIRAC/

will tell you a lot about the DIRAC code structure. Note that:

* there is a tests/ directory
* there is a docs/ directory
* there are several *System/ directories
* there is an \__init__.py file
* there are some base files (README, LICENCE, etc.) and some dotfiles

Unsurprisingly:

* "tests" contains tests - and specifically, it contains all the non-unit tests
* "docs" contains... documentation (including this very same page!)
* all the *System/ directories contain the (python) code of the DIRAC systems


Adding an extension
-------------------------

You can add an extension of DIRAC, of course. 
The repository structure may be the same of the DIRAC, or something slightly different.
The only important thing is what you are going to put in the $PYTHONPATH.


Installing the dependencies
---------------------------

First first, be sure setuptools is at the latest version::

   [sudo] pip install --upgrade setuptools

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

   mkvirtualenv DIRACGrid
   pip install -r $DEVROOT/DIRAC/requirements.txt

This will create a virtual python environment in which we can install
all python packages that DIRAC uses
(this may take a while, and you might need to manually install some package
from your distribution).

Some usuful commands::

   "pip install -r requirements.txt --upgrade" will upgrade the packages
   "deactivate" will exit from a virtualenv
   "workon DIRACGrid" will get you back in DIRACGrid virtualenv


Adding to the PYTHONPATH
-------------------------

Don't forget to::

   export PYTHONPATH=$PYTHONPATH:$DEVROOT

And repeat for the extension development root.

And now you should be able to do::

   ipython
   In [1]: import DIRAC
   In [2]: import GSI

If the above fails, check the log of the pip installations you just done. 


Deploy DIRAC scripts
--------------------

By running::

   $DEVROOT/DIRAC/Core/scripts/dirac-deploy-scripts.py

It is a good idea to add the scripts directory to your $PATH.


Configure DIRAC
---------------

We'll configure DIRAC to work in isolation. At this point, the key
becomes understanding how the DIRAC
`Configuration Service (CS) <http://diracgrid.org/files/docs/AdministratorGuide/Configuration/ConfigurationStructure/index.html>`_
works. I'll explain here briefly. The CS is a layered structure: whenever
you access a CS information (e.g. using a "gConfig" object, see later),
DIRAC will first check into your local "dirac.cfg" file (it can be in your
home as .dirac.cfg, or in etc/ directory, see the link above). If this
will not be found, it will look for such info in the CS servers available.

When you develop locally, you don't need to access any CS server: instead, you need to have total control. 
So, you need to work a bit on the local dirac.cfg file. There is not much else needed, just create your own etc/dirac.cfg.
The example that follows might not be easy to understand at a first sight, but it will become easy soon. 
The syntax is extremely simple, yet verbose: simply, only brackets and equalities are used.

If you want to create an isolated installation just create a
*$DEVROOT/etc/dirac.cfg* file with (create the etc directory first)::

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


Certificates
-------------

DIRAC understands certificates in *pem* format. That means that certificate set will consist of two files. 
Files ending in *cert.pem* can be world readable but just user writable since it contains the certificate and public key.
Files ending in *key.pem* should be only user readable since they contain
the private key. You will need two different sets certificates and the CA certificate that signed the sets.
The following commands should do the trick for you, by creating a fake CA, a fake user certificate, and a fake host certificate::

   cd $DEVROOT/DIRAC
   git checkout release/integration
   source tests/Jenkins/utilities.sh
   generateCertificates
   generateUserCredentials
   mkdir -p ~/.globus/
   cp $DEVROOT/user/*.{pem,key} ~/.globus/
   mv ~/.globus/client.key ~/.globus/userkey.pem
   mv ~/.globus/client.pem ~/.globus/usercert.pem

Now we need to register those certificates in DIRAC. To do you you
must modify *$DEVROOT/etc/dirac.cfg* file and set the correct
certificate DNs for you and your development box. For instance,
to register the host replace "/your/box/dn/goes/here"
(/Registry/Hosts/mydevbox/DN option) with the result of::

   openssl x509 -noout -subject -in $DEVROOT/etc/grid-security/hostcert.pem | sed 's:^subject= ::g'

You're ready for DIRAC development !


