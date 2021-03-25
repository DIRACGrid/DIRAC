============================================
1. Client Installation (for python2 clients)
============================================

The DIRAC client installation procedure consists of several steps. This example is destinated for tutorials.
For more information about various options of installing DIRAC Client see the :ref:`Getting Started guide <dirac_install>`.

1.1 Install script
------------------

Download the *dirac-install* script from `here <https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py>`_::

  curl https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py --output=dirac-install
  chmod +x dirac-install


1.2 Installation
----------------

In most cases you are installing the DIRAC client to work as a member of some particular user community or, in
other words, Virtual Organization (VO). The managers of your VO usually prepare default settings to
be applied for the DIRAC client installation. In this case the installation procedure reduces to the following
assuming the name of the Virtual Organization *vo.formation.idgrilles.fr*::

  ./dirac-install -V formation
  source bashrc

The above command will download also *vo.formation.idgrilles.fr_defaults.cfg* file which contains the VO
default settings. Check with your VO managers if this mode of installation is available.

1.3 Configuration
-----------------

Once the client software is installed, it should be configured in order to access the corresponding DIRAC services.
The minimal necessary configuration is done by the following command::

  dirac-configure --cfg defaults-formation.cfg

When you run this command for the first time you might see some errors messages about a failure to access DIRAC
services. This is normal because at this point the configuration is not yet done and you do not have a valid proxy.
After creating a proxy with *proxy-init* command, just repeat the *dirac-configure* command once again.


1.4 Updating the client installation
------------------------------------

The client software update when a new version is available is simply done by running again the *dirac-install*
command as in p.1.2. You can run the *dirac-install* giving the exact version of the DIRAC software, for example::

  dirac-install -r v6r20p14
