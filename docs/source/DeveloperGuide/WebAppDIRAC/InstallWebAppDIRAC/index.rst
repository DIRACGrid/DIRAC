.. _webappdirac_installwebappdirac:

===================
Install WebAppDIRAC
===================

You have already prepared your eclipse. Now you can try to install DIRAC and the Web portal locally. 
The instruction is given for MAC OS users, but it is similar to Linux users as well. 
I use different directory for developing WebAppDIRAC than the directory where the portal is installed. 
You can link the directory where you develop the WebAppDIRAC to where the Web portal installed or
you can copy the code from the development area to the installed area. 

Install WebAppDIRAC
-------------------

#. Create a directory where you will install DIRAC and WebAppDIRAC::
   git clone https://github.com/DIRACGrid/WebAppDIRAC.git
   curl -O -L https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py
   chmod +x dirac-install.py
   ./dirac-install.py -r |version| -X -t server
   source bashrc (we have to use the correct python in order to install tornado)
   pip install tornado
   mkdir etc

You need to create: vi etc/dirac.cfg file 

For example::

   DIRAC {
    #Setup = LHCb-Production
    Setup = LHCb-Certification
    Configuration {
        Servers = dips://lhcb-conf-dirac.cern.ch:9135/Configuration/Server
    }
    Security {
        CertFile = <location of hostcert.pem>
        KeyFile = <location of hostkey.pem>
    }
    Extensions = WebApp
   }


Note: It is an LHCb specific configuration. You have to use your Configuration servers

**NOTE: If you don't want to use Balancer, comment following parameter in configuration: /WebApp/Balancer=None.**


Quick install
-------------

* python dirac-install -t server $installCfg
* source $installDir/bashrc
* dirac-configure $installCfg $DEBUG
* dirac-setup-site $DEBUG
