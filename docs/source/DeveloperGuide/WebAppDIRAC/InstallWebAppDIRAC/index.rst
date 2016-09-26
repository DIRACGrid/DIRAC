.. _webappdirac_installwebappdirac:

===================
Install WebAppDIRAC
===================

You have already prepared your eclipse. Now you can try to install DIRAC and the Web portal locally. 
The instruction is given for MAC OS users, but it is similar to Linux users as well. 
I use different directory for developing WebAppDIRAC than the directory where the portal is installed. 
You can link the directory where you develop the WebAppDIRAC to where the Web portal installed or
you can copy the code from the development area to the installed area. 

Install DIRAC & WebAppDIRAC
---------------------------

We propose to read the following documentation and after 
continue to install DIRAC `<https://github.com/DIRACGrid/DIRAC/wiki/GitSetup>`_.

#. Create a directory where you will install DIRAC and WebAppDIRAC: mkdir portal; cd portal
#. git clone git://github.com/zmathe/DIRAC.git. (NOTE: This works when you forked the DIRAC repository) or execute: git clone https://github.com/DIRACGrid/DIRAC.git
#. git clone git://github.com/zmathe/WebAppDIRAC.git (NOTE: This works when you forked the WebAppDIRAC repository on github)  or git clone `<https://github.com/DIRACGrid/WebAppDIRAC.git>`_ ./scripts/dirac-install -r v6r10p15 -X -t server or ./DIRAC/Core/scripts/dirac-install.py -r v6r10p15 -X -t server  (You can use the current production version of DIRAC which can found `<http://diracgrid.org>`_.) If DIRAC properly installed, you can continue to install WebAppDIRAC. NOTE: In case of timeout use: ./DIRAC/Core/scripts/dirac-install.py -r v6r10p15 -X -t server -T 600000
#. python DIRAC/Core/scripts/dirac-deploy-scripts.py
#. ./WebAppDIRAC/dirac-postInstall.py
#. source bashrc (we have to use the correct python in order to install tornado)
#. pip install tornado
#. mkdir etc
#. you need to create: vi etc/dirac.cfg file 

For example:: 
   
   
   DIRAC {
      Setup = LHCb-Development
      #Setup = LHCb-Production
      #Setup = LHCb-Certification
      Configuration {
         Servers = dips://lhcb-conf-dirac.cern.ch:9135/Configuration/Server
         Servers += dips://lhcbprod.pic.es:9135/Configuration/Server
      }
   }


Note: It is an LHCb specific configuration. You have to use your Configuration servers

Quick install
-------------

* python dirac-install -t server $installCfg
* source $installDir/bashrc
* dirac-configure $installCfg $DEBUG
* dirac-setup-site $DEBUG

Start the web framework
-----------------------

#. You need the grid-certificates under etc directory. If you do not known about it, please ask the appropriate developer.
#. python WebAppDIRAC/scripts/dirac-webapp-run.py -ddd Use firefox/safari/chrome… and open the following url: `<https://localhost:8443/DIRAC>`_