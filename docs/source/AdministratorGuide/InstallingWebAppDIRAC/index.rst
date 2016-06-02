.. _installwebappdirac:

===================
Install WebAppDIRAC
===================

The first section describes the install procedure of the web framework. The configuration of the web will be presented in the next section.
Nginx can be used to improve the performance of the web interface. It is not mandatory to use Nginx. The installation and configuration of NGinx will be presented in the last section.

Install WebAppDIRAC
-------------------

Requirements
------------

It is required CERN provided OS (slc5,slc6, etc.) distribution. We recommend you to use a supported Linux distribution. 
Please follow the :ref:`_server_requirements` instructions 
to setup the machine. In principle there is no magic to install the web portal. It has to be installed as another DIRAC component...
When the machine is ready you can start to install the portal. But before that you need the install_site.sh script and a configuration file.  

Getting the install script
~~~~~~~~~~~~~~~~~~~~~~~~~~
You can found the instruction about were to get the install_site.sh at the end of the :ref:`_server_requirements` section.

Configuration file
~~~~~~~~~~~~~~~~~~
You can use a standard configuration file for example :ref:`_install_primary_server`. Please make sure that the following lines are exists in the 
configuration file::
   ExtraModules = WebAppDIRAC
   WebApp = yes
But you can also use the following configuration file to install the web portal.
$installCfg::
   LocalInstallation
   {
     #
     #   These are options for the installation of the DIRAC software
     #
     #  DIRAC release version (this is an example, you should find out the current
     #  production release)
     Release = v6r15
     #  Python version of the installation
     PythonVersion = 27
     #  To install the Server version of DIRAC (the default is client)
     InstallType = server
     #  LCG python bindings for SEs and LFC. Specify this option only if your installation
     #  uses those services
     LcgVer = 2013-09-24
     #  If this flag is set to yes, each DIRAC update will be installed
     #  in a separate directory, not overriding the previous ones
     UseVersionsDir = yes
     #  The directory of the DIRAC software installation
     TargetPath = /opt/dirac
     #  DIRAC extra modules to be installed (Web is required if you are installing the Portal on
     #  this server).
     #  Only modules not defined as default to install in their projects need to be defined here:
     #   i.e. LHCb, LHCbWeb for LHCb for example: ExtraModules = WebAppDIRAC,LHCb,LHCbWeb
     ExtraModules = WebAppDIRAC
     Project = DIRAC
     WebPortal = yes
     WebApp = yes
     Services = Framework/SystemAdministrator
     UseServerCertificate = yes
     SkipCADownload = yes
     Setup = your setup #for example: LHCb-Certification
     ConfigurationMaster = no
     ConfigurationServer = your configuration service
   }
 

Before you start the installation please make sure that you have the host certofocate /opt/dirac/etc directory... More infor in the Server Certificates section in :ref:`_server_requirements` .
 
 Create the configuration file::
   - vim /home/dirac/DIRAC/install.cfg
   - copy the lines above the this file...
   - cd /home/dirac/DIRAC
   - chmod +x install_site.sh
   - ./install_site.sh install.cfg
   - source /opt/dirac/bashrc
 
 Note: If you do not have the /home/dirac/DIRAC directory, please have a look the instructions given in the :ref:`_server_requirements` section. 
   

Checks to be done after the installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the installation is successful, you will see the following lines::
   
   Status of installed components:
   
      Name                          Runit Uptime PID
   ====================================================
    1 Web_WebApp                    Run   6      19887
    2 Framework_SystemAdministrator Run   2      19941


Make sure that the portal is listening in the correct port::

   Without NGinx::

   tail -200f /opt/dirac/runit/Web/WebApp/log/current
   
   2016-06-02 12:44:18 UTC WebApp/Web   INFO: Configuring in developer mode...
   2016-06-02 12:44:18 UTC WebApp/Web NOTICE: Configuring HTTP on port 8080
   2016-06-02 12:44:18 UTC WebApp/Web NOTICE: Configuring HTTPS on port 8443
   2016-06-02 12:44:19 UTC WebApp/Web ALWAYS: Listening on https://0.0.0.0:8443/DIRAC/ and http://0.0.0.0:8080/DIRAC/
   
   
   Using Nginx:: 

   tail -200f /opt/dirac/runit/Web/WebApp/log/current
   
   The output of the command::   

   2016-06-02 12:35:46 UTC WebApp/Web NOTICE: Configuring HTTP on port 8000
   2016-06-02 12:35:46 UTC WebApp/Web ALWAYS: Listening on http://0.0.0.0:8000/DIRAC/
   

If you are not using NGinx and the web server is listening on 8000, please open vim /opt/dirac/pro/WebAppDIRAC/WebApp/web.cfg and add Balancer=None.
Make sure that the configuration /opt/dirac/pro/etc/dirac.cfg file is correct. It contains Extensions = WebApp. For example::

   DIRAC
   {
     Setup = LHCb-Certification
     Configuration
     {
       Servers = 
     }
     Security
     {
     }
     Extensions = WebApp
     Setups
     {
       LHCb-Certification
       {
         Configuration = LHCb-Certification
         Framework = LHCb-Certification
       }
     }
   }
   

* Update using: **dirac-admin-sysadmin-cli**
  
         * dirac-admin-sysadmin-cli -H hostname
         * update version of DIRAC, for example v8r1
         

Web configuration file
----------------------

We use **web.cfg** configuration file. The location of the file is /opt/dirac/pro/WebAppDIRAC/WebApp/web.cfg The structure of the web.cfg file is the following::

      WebApp
      {
        Balancer = None #[nginx] in case you have installed nginx
        #NumProcesses = 1
        #SSLProrocol = "" [PROTOCOL_SSLv2, PROTOCOL_SSLv23, PROTOCOL_SSLv3, PROTOCOL_TLSv1] in case you do not want to use the default protocol
        Theme = tabs #[desktop]
        Schema
        {
          Tools{
           Proxy Upload = DIRAC.ProxyUpload
           Job Launchpad = DIRAC.JobLaunchpad
           Notepad = DIRAC.Notepad
          }
          OldPortal{
            Request Manager = link|https://lhcb-web-dirac.cern.ch/DIRAC/LHCb-Production/lhcb_user/Production/ProductionRequest/display
          }
          Applications
          {
            Public State Manager = DIRAC.PublicStateManager
            Job Monitor = DIRAC.JobMonitor
            Pilot Monitor = DIRAC.PilotMonitor
            Accounting = DIRAC.AccountingPlot
            Configuration Manager = DIRAC.ConfigurationManager
            Registry Manager = DIRAC.RegistryManager
            File Catalog = DIRAC.FileCatalog
            System Administration = DIRAC.SystemAdministration
            Activity Monitor = DIRAC.ActivityMonitor
            Transformation Monitor = DIRAC.TransformationMonitor
            Request Monitor = DIRAC.RequestMonitor
            Pilot Summary = DIRAC.PilotSummary
            Resource Summary = DIRAC.ResourceSummary
            Site Summary = DIRAC.SiteSummary
            Proxy Manager = DIRAC.ProxyManager 
            #ExampleApp = DIRAC.ExampleApp
          }
          DIRAC = link|http://diracgrid.org
        }
      }
 

Define external links::
   
   Web
   {
       Lemon Host Monitor
       {
         volhcb01 = link|https://lemonweb.cern.ch/lemon-web/info.php?entity=lbvobox01&detailed=yes
       }
   }
   
Install and configure NGINX
---------------------------

The official site of NGINX is the following: `<http://nginx.org/>`_ 
The required NGINX version has to be grater than 1.4. 

  * Install Nginx using package manager::
         
         yum install nginx
   
   
  * Manual install
   
      #. wget http://nginx.org/download/nginx-1.6.0.tar.gz

      #. cd nginx-1.6.0

      #. ./configure

      #. make

      #. sudo make install(without sudo you have to specify the installation directory)
  
  * Configure NGINX
  
    In the installed directory of NGINX you have to edit the nginx.conf file. In our installation it is under /usr/local/nginx/conf directory. You have to delete part of the nginx.conf file starting from #gzip on; line ::
      
         #keepalive_timeout  0;
         keepalive_timeout  65;
         
         #gzip  on;
         
         server {
         ....
         
   
   to the end of file. Note: DO NOT delete } You have to add the following line::
   
          include site.conf;
   
  The content of the nginx.conf (/usr/local/nginx/conf/nginx.conf)::
  
      #user  nobody;
       worker_processes 2;
      
       #error_log  logs/error.log;
       #error_log  logs/error.log  notice;
       #error_log  logs/error.log  info;
      
       #pid        logs/nginx.pid;
      
      
       events {
         worker_connections  1024;
       }
      
      
       http {
           include       mime.types;
           default_type  application/octet-stream;
      
           #log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
           #                  '$status $body_bytes_sent "$http_referer" '
           #                  '"$http_user_agent" "$http_x_forwarded_for"';
      
           #access_log  logs/access.log  main;
      
           sendfile        on;
           #tcp_nopush     on;
      
           #keepalive_timeout  0;
           keepalive_timeout  65;
      
           #gzip  on;
      
           include site.conf;
         }
     
   
  You have to copy and paste under /usr/local/nginx/conf directory and please modify the content according to your installation::
      
      upstream tornadoserver {
       #One for every tornado instance you're running that you want to balance
       server 127.0.0.1:8000;
     }
   
     server {
       listen 80;
   
       #Your server name if you have weird network config. Otherwise leave commented
       server_name  volhcb25.cern.ch;
   
       root /opt/dirac/WebPrototype/webRoot;
   
       location ~ ^/[a-zA-Z]+/(s:.*/g:.*/)?static/(.+\.(jpg|jpeg|gif|png|bmp|ico|pdf))$ {
         alias /opt/dirac/WebPrototype/;
         #Add one more for every static path. For instance for LHCbWebDIRAC:
         #try_files LHCbWebDIRAC/WebApp/static/$2 WebAppDIRAC/WebApp/static/$2 /;
         try_files WebAppDIRAC/WebApp/static/$2 /;
         expires 10d;
         gzip_static on;
         gzip_disable "MSIE [1-6]\.";
         add_header Cache-Control public;
         break;
       }
   
       location ~ ^/[a-zA-Z]+/(s:.*/g:.*/)?static/(.+)$ {
         alias /opt/dirac/WebPrototype/;
         #Add one more for every static path. For instance for LHCbWebDIRAC:
         #try_files LHCbWebDIRAC/WebApp/static/$2 WebAppDIRAC/WebApp/static/$2 /;
         try_files WebAppDIRAC/WebApp/static/$2 /;
         expires 1d;
         gzip_static on;
         gzip_disable "MSIE [1-6]\.";
         add_header Cache-Control public;
         break;
       }
   
       location ~ /DIRAC/ {
         proxy_pass_header Server;
         proxy_set_header Host $http_host;
         proxy_redirect off; 
         proxy_set_header X-Real-IP $remote_addr;
         proxy_set_header X-Scheme $scheme; 
         proxy_pass http://tornadoserver;
         proxy_read_timeout 3600;
         proxy_send_timeout 3600;
   
         gzip on;
         gzip_proxied any;
         gzip_comp_level 9;
         gzip_types text/plain text/css application/javascript application/xml application/json;
   
         # WebSocket support (nginx 1.4)
         proxy_http_version 1.1;
         proxy_set_header Upgrade $http_upgrade; 
         proxy_set_header Connection "upgrade";
   
         break;
       }
   
       location / {
         rewrite ^ http://$server_name/DIRAC/ permanent;
       }
   
     }
   
     server {
       listen 443 default ssl; ## listen for ipv4
   
       server_name  volhcb25.cern.ch;
   
       #Certs that will be shown to the user connecting to the web. 
       #Preferably NOT grid certs. Use something that the user cert will not complain about
       ssl_certificate    /opt/dirac/etc/grid-security/hostcert.pem;
       ssl_certificate_key /opt/dirac/etc/grid-security/hostkey.pem;
   
       ssl_client_certificate /opt/dirac/pro/etc/grid-security/allCAs.pem;
       ssl_verify_client on;
       ssl_verify_depth 10;
       ssl_session_cache shared:SSL:10m;
   
       root /opt/dirac/WebPrototype;
   
       location ~ ^/[a-zA-Z]+/(s:.*/g:.*/)?static/(.+\.(jpg|jpeg|gif|png|bmp|ico|pdf))$ {
         alias /opt/dirac/WebPrototype/;
         #Add one more for every static path. For instance for LHCbWebDIRAC:
         #try_files LHCbWebDIRAC/WebApp/static/$2 WebAppDIRAC/WebApp/static/$2 /;
         try_files WebAppDIRAC/WebApp/static/$2 /;
         expires 10d;
         gzip_static on;
         gzip_disable "MSIE [1-6]\.";
         add_header Cache-Control public;
         break;
       }
   
       location ~ ^/[a-zA-Z]+/(s:.*/g:.*/)?static/(.+)$ {
         alias /opt/dirac/WebPrototype/;
         #Add one more for every static path. For instance for LHCbWebDIRAC:
         #try_files LHCbWebDIRAC/WebApp/static/$2 WebAppDIRAC/WebApp/static/$2 /;
         try_files WebAppDIRAC/WebApp/static/$2 /;
         expires 1d;
         gzip_static on;
         gzip_disable "MSIE [1-6]\.";
         add_header Cache-Control public;
         break;
       }
   
       location ~ /DIRAC/ {
         proxy_pass_header Server;
         proxy_set_header Host $http_host;
         proxy_redirect off; 
         proxy_set_header X-Real-IP $remote_addr;
         proxy_set_header X-Scheme $scheme; 
         proxy_pass http://tornadoserver;
         proxy_read_timeout 3600;
         proxy_send_timeout 3600;
   
         proxy_set_header X-Ssl_client_verify $ssl_client_verify;
         proxy_set_header X-Ssl_client_s_dn $ssl_client_s_dn;
         proxy_set_header X-Ssl_client_i_dn $ssl_client_i_dn;
   
         gzip on;
         gzip_proxied any;
         gzip_comp_level 9;
         gzip_types text/plain text/css application/javascript application/xml application/json;
   
         # WebSocket support (nginx 1.4)
         proxy_http_version 1.1;
         proxy_set_header Upgrade $http_upgrade; 
         proxy_set_header Connection "upgrade";
   
         break;
       }
   
       location / {
         rewrite ^ https://$server_name/DIRAC/ permanent;
       }
     }
    
   
  You have to use the genCAsFile.sh to generate the following file: ssl_client_certificate /opt/dirac/pro/etc/grid-security/allCAs.pem; The content of the genCAsFile.sh file is the following::
  
       #!/bin/bash

        gsCerts=/etc/grid-security/certificates
      
        allF="/opt/dirac/etc/grid-security/allCAs.pem"
        copiedCAs=0
        invalidCAs=0
        echo "Copying CA certificates into $allF"
        for cert in $gsCerts/*.0
        do
          ossle="openssl x509 -noout -in ${cert}"
          if ${ossle} -checkend 3600; then
                openssl x509 -in ${cert} >> $allF.gen
                copiedCAs=`expr "${copiedCAs}" + "1"`
          else
            echo " - CA ${cert} is expired"
            invalidCAs=`expr "${invalidCAs}" + "1"`
          fi
        done
        echo " + There are ${invalidCAs} invalid CA certificates in $gsCerts"
        echo " + Copied ${copiedCAs} CA certificates into $allF"
        mv $allF.gen $allF
        
  
  You have to add to the web.cfg the following lines::
  
       DevelopMode = False
       Balancer = nginx
       NumProcesses = 1
   
  The last step is to create /etc/init.d/nginx and add to this file the following lines::
  
       #!/bin/sh
       #
       # nginx - this script starts and stops the nginx daemon
       #
       # chkconfig:   - 85 15 
       # description:  Nginx is an HTTP(S) server, HTTP(S) reverse \
       #               proxy and IMAP/POP3 proxy server
       # processname: nginx
       # config:      /etc/nginx/nginx.conf
       # config:      /etc/sysconfig/nginx
       # pidfile:     /var/run/nginx.pid
   
       # Source function library.
       . /etc/rc.d/init.d/functions
   
       # Source networking configuration.
       . /etc/sysconfig/network
   
       # Check that networking is up.
       [ "$NETWORKING" = "no" ] && exit 0
   
       nginx="/usr/local/nginx/sbin/nginx"
       prog=$(basename $nginx)
   
       NGINX_CONF_FILE="/etc/nginx/nginx.conf"
       NGINX_CONF_FILE="/usr/local/nginx/conf/nginx.conf"
   
       [ -f /etc/sysconfig/nginx ] && . /etc/sysconfig/nginx
   
       lockfile=/var/lock/subsys/nginx
   
       make_dirs() {
          # make required directories
          #user=`$nginx -V 2>&1 | grep "configure arguments:" | sed 's/[^*]*--user=\([^ ]*\).*/\1/g' -`
          #if [ -z "`grep $user /etc/passwd`" ]; then
          #    useradd -M -s /bin/nologin $user
          #fi
          #options=`$nginx -V 2>&1 | grep 'configure arguments:'`
          #for opt in $options; do
          #    if [ `echo $opt | grep '.*-temp-path'` ]; then
          #        value=`echo $opt | cut -d "=" -f 2`
          #        if [ ! -d "$value" ]; then
          #            # echo "creating" $value
          #            mkdir -p $value && chown -R $user $value
          #        fi
          #    fi
          #done
          a=1
       }
   
       start() {
           [ -x $nginx ] || exit 5
           [ -f $NGINX_CONF_FILE ] || exit 6
           make_dirs
           echo -n $"Starting $prog: "
           daemon $nginx -c $NGINX_CONF_FILE
           retval=$?
           echo
           [ $retval -eq 0 ] && touch $lockfile
           return $retval
       }
   
       stop() {
           echo -n $"Stopping $prog: "
           killproc $prog -QUIT
           retval=$?
           echo
           [ $retval -eq 0 ] && rm -f $lockfile
           return $retval
       }
   
       restart() {
           configtest || return $?
           stop
           sleep 1
           start
       }
   
       reload() {
           configtest || return $?
           echo -n $"Reloading $prog: "
           killproc $nginx -HUP
           RETVAL=$?
           echo
       }
   
       force_reload() {
           restart
       }
   
       configtest() {
         $nginx -t -c $NGINX_CONF_FILE
       }
   
       rh_status() {
           status $prog
       }
   
       rh_status_q() {
           rh_status >/dev/null 2>&1
       }
   
       case "$1" in
           start)
               rh_status_q && exit 0
               $1
               ;;
           stop)
               rh_status_q || exit 0
               $1
               ;;
           restart|configtest)
               $1
               ;;
           reload)
               rh_status_q || exit 7
               $1
               ;;
           force-reload)
               force_reload
               ;;
           status)
               rh_status
               ;;
           condrestart|try-restart)
               rh_status_q || exit 0
                   ;;
           *)
               echo $"Usage: $0 {start|stop|status|restart|condrestart|try-restart|reload|force-reload|configtest}"
               exit 2
       esac
   
   
* Start, Stop and restart nginx::
   
   /etc/init.d/nginx start|stop|restart

Nginx and CRLs
--------------

You can configure Nginx to check the certificate revoked list. You have to generate **allRevokedCerts.pem** file. You can use the following simple **bash** script to generate the file::

     #!/bin/bash

     gsCerts=/etc/grid-security/certificates

     allF="/opt/dirac/etc/grid-security/allRevokedCerts.pem"
     copiedCAs=0
     invalidCAs=0
     echo "Copying revoked certificates into $allF"
     for cert in $gsCerts/*.r0
     do
        openssl crl -in ${cert} >> $allF.gen
        copiedCAs=`expr "${copiedCAs}" + "1"`
     done
     echo " + Copied ${copiedCAs} revoked certificates into $allF"
     mv $allF.gen $allF
     
Note: you can use a chron job to generate the **allRevokedCerts.pem** file.

You have to add the **site.conf** the following line::

      ssl_crl file /opt/dirac/pro/etc/grid-security/allRevokedCerts.pem;
      
