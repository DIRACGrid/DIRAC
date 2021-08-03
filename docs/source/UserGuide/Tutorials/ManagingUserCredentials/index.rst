.. set highlighting to console input/output
.. highlight:: console

============================
Managing user credentials
============================

This section assumes that the DIRAC client is already installed and configured.


Managing Certificates
-------------------------

Donwloading Certificate from browser
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

- Get the certificate from the browser:

  - Firefox:

      Preferences -> Privacy & Security -> View Certificates -> Select your certificate -> Backup


  As a result you will get the certificate as a file with .p12 extension.

Converting Certificates from P12 to PEM format
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

- Run dirac-cert-convert script to convert your certificate to the appropriate form::

      dirac-cert-convert.sh <USERCERT>.p12

  Output of this command must look like::

      $ dirac-cert-convert.sh usercert.p12
      Creating globus directory
      Converting p12 key to pem format
      Enter Import Password:
      MAC verified OK
      Enter PEM pass phrase:
      Verifying - Enter PEM pass phrase:
      Converting p12 certificate to pem format
      Enter Import Password:
      MAC verified OK
      Information about your certificate:
      subject= /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
      issuer= /C=FR/O=CNRS/CN=GRID2-FR
      Done

  "Enter Import Password:" prompt requires the password given when the certificate was exported from the browser.
  It will be requested twice. The PEM pass phrase is the password associated with the created private key. This
  password will be requested each time you will create a proxy. Do not forget it !

- Check that your certificate was correctly converted and placed in the $HOME/.globus directory, in PEM format
  and with correct permissions::

      $ ls -la ~/.globus
      total 16
      drwxr-xr-x  2 hamar marseill 2048 Oct 19 13:01 .
      drwxr-xr-x 42 hamar marseill 4096 Oct 19 13:00 ..
      -rw-r--r--  1 hamar marseill 6052 Oct 19 13:00 usercert.p12
      -rw-r--r--  1 hamar marseill 1914 Oct 19 13:01 usercert.pem
      -r--------  1 hamar marseill 1917 Oct 19 13:01 userkey.pem


Managing Proxies
--------------------

Before running any command in the grid, it is mandatory to have a valid certificate proxy. The commands to create a
valid proxy using DIRAC commands are shown below.


Creating a user proxy
@@@@@@@@@@@@@@@@@@@@@@@@@@@

- First, in the machine where the DIRAC client is installed setup the DIRAC environment running the following commands::

        cd $DIRAC_PATH (if you set it)
        source bashrc

- After the environment is set up, you are able to create your proxy with the following command::

        dirac-proxy-init

  the above will create a proxy from the certificate in ~/.globus, with a default role.
  The switches below will create a proxy of group "dirac_user" (if defined) and will securely upload such 
  proxy to the DIRAC proxy store (ProxyManager), from where it could later be downloaded::

        dirac-proxy-init --group dirac_user --upload


  The additional "--debug" switch (alias of "-ddd") can be used for debugging purposes,
  and its output would end up being similar to the following::

        $ dirac-proxy-init --group dirac_user --upload --debug
        Generating proxy...
        Enter Certificate password:
        Contacting CS...
        Checking DN /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev
        Username is atsareg
        Creating proxy for atsareg@dirac_user (/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev)
        Uploading proxy for dirac_user...
        Uploading dirac_user proxy to ProxyManager...
        Loading user proxy
        Uploading proxy on-the-fly
        Cert file /home/andrei/.globus/usercert.pem
        Key file  /home/andrei/.globus/userkey.pem
        Loading cert and key
        User credentials loaded
         Uploading...
        Proxy uploaded
        Proxy generated:
        subject      : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev/CN=proxy
        issuer       : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev
        identity     : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev
        timeleft     : 23:59:57
        DIRAC group  : dirac_user
        path         : /tmp/x509up_u501
        username     : atsareg

        Proxies uploaded:
         DN                                                      | Group      | Until (GMT)
         /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev | dirac_user | 2012/02/08 13:05

  As a result of this command, several operations are accomplished:

  - a long user proxy (with the length of the validity of the certificate) is uploaded to the
    DIRAC ProxyManager service, equivalent of the gLite MyProxy service
  - a short user proxy is created with the DIRAC extension carrying the DIRAC group name and with the
    VOMS extension corresponding to the DIRAC group if the gLite UI environment is available. 
    This proxy is stored in the local "/tmp/" directory, as shown.

  If the gLite UI environment is not available, the VOMS extensions will not be loaded into the proxy.
  This is not a serious problem, still most of the operations will be possible.


Getting the proxy information
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

- Check that your proxy was correctly created and the DIRAC group and the VOMS extension are set correctly, running the command::

        dirac-proxy-info

  For example::

        $ dirac-proxy-info
        subject      : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar/CN=proxy/CN=proxy
        issuer       : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar/CN=proxy
        identity     : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
        timeleft     : 23:53:55
        DIRAC group  : dirac_user
        path         : /tmp/x509up_u40885
        username     : vhamar
        VOMS         : True
        VOMS fqan    : ['/vo.formation.idgrilles.fr']


- At this moment, your proxy can be uploaded to the ProxyManager service. To check that::

        dirac-proxy-get-uploaded-info

  In this case the output shows user DN, group, expiration time and persistency flag::


        $ dirac-proxy-get-uploaded-info
        Checking for DNs /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
        --------------------------------------------------------------------------------------------------------
        | UserDN                                          | UserGroup   | ExpirationTime      | PersistentFlag |
        --------------------------------------------------------------------------------------------------------
        | /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar | dirac_user  | 2011-06-29 12:04:25 | True           |
        --------------------------------------------------------------------------------------------------------

- The same can be checked in the Web Portal at the following location::

        Applications -> Proxy Manager

  Using the portal you have the option to delete your proxies.
