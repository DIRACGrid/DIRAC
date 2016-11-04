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
Please follow the :ref:`server_requirements` instructions 
to setup the machine. In principle there is no magic to install the web portal. It has to be installed as another DIRAC component...
When the machine is ready you can start to install the portal. But before that you need the install_site.sh script and a configuration file.  

Getting the install script
~~~~~~~~~~~~~~~~~~~~~~~~~~
You can found the instruction about were to get the install_site.sh at the end of the :ref:`server_requirements` section.

Configuration file
~~~~~~~~~~~~~~~~~~
You can use a standard configuration file for example :ref:`install_primary_server`. Please make sure that the following lines are exists in the 
configuration file::
   Externals = WebApp
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
     #  If this flag is set to yes, each DIRAC update will be installed
     #  in a separate directory, not overriding the previous ones
     UseVersionsDir = yes
     #  The directory of the DIRAC software installation
     TargetPath = /opt/dirac
     #  DIRAC extra modules to be installed (Web is required if you are installing the Portal on
     #  this server).
     #  Only modules not defined as default to install in their projects need to be defined here:
     #   i.e. LHCb, LHCbWeb for LHCb for example: ExtraModules = WebAppDIRAC,LHCb,LHCbWeb
     Externals = WebApp
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
 

Before you start the installation please make sure that you have the host certificate in the /opt/dirac/etc directory... More info in the Server Certificates section in :ref:`server_requirements` .
 
 Create the configuration file::
   - vim /home/dirac/DIRAC/install.cfg
   - copy the lines above the this file...
   - cd /home/dirac/DIRAC
   - chmod +x install_site.sh
   - ./install_site.sh install.cfg
   - source /opt/dirac/bashrc
 
 Note: If you do not have the /home/dirac/DIRAC directory, please have a look the instructions given in the :ref:`server_requirements` section. 
   

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
   
   
If your version is not grater than 1.4 you have to install NGinx manually. 
  
* Manual install
   
     vim /etc/yum.repos.d/nginx.repo
     
     CentOS::

      [nginx]
      name=nginx repo
      baseurl=http://nginx.org/packages/centos/$releasever/$basearch/
      gpgcheck=0
      enabled=1

     RHEL::

      [nginx]
      name=nginx repo
      baseurl=http://nginx.org/packages/rhel/$releasever/$basearch/
      gpgcheck=0
      enabled=1

Due to differences between how CentOS, RHEL, and Scientific Linux populate the $releasever variable, it is necessary to manually replace $releasever with either 5 (for 5.x) or 6 (for 6.x), 
depending upon your OS version. For example::
   [nginx]
   name=nginx repo
   baseurl=http://nginx.org/packages/rhel/6/$basearch/
   gpgcheck=0
   enabled=1
  
 If it is successful installed::
 
    Verifying  : nginx-1.10.1-1.el6.ngx.x86_64                                                                                                                                                                                                                    1/1
   Installed:
      nginx.x86_64 0:1.10.1-1.el6.ngx
  
  
* Configure NGINX
  
    You have to found the nginx.conf file. You can see which configuration used in /etc/init.d/nginx. For example::
    
    vim /etc/nginx/nginx.conf
   
  If the file contains 'include /etc/nginx/conf.d/*.conf;' line, you have to create a site.conf under /etc/nginx/conf.d/ otherwise you have to do: 'include /etc/nginx/site.conf'  
   
 The content of the site.conf (please modify it!!!)::
      
   #Generated by gen.py

   upstream tornadoserver {
     #One for every tornado instance you're running that you want to balance
     server 127.0.0.1:8000;
   }
   
   server {
     listen 80;
   
     #Your server name if you have weird network config. Otherwise leave commented
     #server_name  lbvobox33.cern.ch;
     server_name dzmathe.cern.ch;
   
     root /opt/dirac/pro;
   
     location ~ ^/[a-zA-Z]+/(s:.*/g:.*/)?static/(.+\.(jpg|jpeg|gif|png|bmp|ico|pdf))$ {
       alias /opt/dirac/pro/;
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
       alias /opt/dirac/pro/;
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
   
     #server_name  lbvobox33.cern.ch;
     server_name  dzmathe.cern.ch;
   
     ssl_prefer_server_ciphers On;
     ssl_protocols TLSv1 TLSv1.1 TLSv1.2;
     ssl_ciphers ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+3DES:!aNULL:!MD5:!DSS;
   
     #Certs that will be shown to the user connecting to the web.
     #Preferably NOT grid certs. Use something that the user cert will not complain about
     ssl_certificate    /opt/dirac/etc/grid-security/hostcert.pem;
     ssl_certificate_key /opt/dirac/etc/grid-security/hostkey.pem;
   
     ssl_client_certificate /opt/dirac/pro/etc/grid-security/cas.pem;
   #  ssl_crl /opt/dirac/pro/etc/grid-security/allRevokedCerts.pem;
     ssl_verify_client on;
     ssl_verify_depth 10;
     ssl_session_cache shared:SSL:10m;
   
     root /opt/dirac/pro;
   
     location ~ ^/[a-zA-Z]+/(s:.*/g:.*/)?static/(.+\.(jpg|jpeg|gif|png|bmp|ico|pdf))$ {
       alias /opt/dirac/pro/;
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
       alias /opt/dirac/pro/;
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
   

You can start NGinx now.

* Start, Stop and restart nginx::
   
   /etc/init.d/nginx start|stop|restart
  
  
You have to add to the web.cfg the following lines in order to use NGinx::
  
       DevelopMode = False
       Balancer = nginx
       NumProcesses = 1
 
 You can try to use the web portal. For example: http://dzmathe.cern.ch/DIRAC/
 If you get 502 Bad Gateway error, you need to generate rules for SE linus. 
 You can see the error in tail -200f /var/log/nginx/error.log::
     
     016/06/02 15:55:24 [crit] 20317#20317: *4 connect() to 127.0.0.1:8000 failed (13: Permission denied) while connecting to upstream, client: 128.141.170.23, server: dzmathe.cern.ch, request: "GET /DIRAC/?view=tabs&theme=Grey&url_state=1| HTTP/1.1", upstream: "http://127.0.0.1:8000/DIRAC/?view=tabs&theme=Grey&url_state=1|", host: "dzmathe.cern.ch"

* Generate the the rule::
   - grep nginx /var/log/audit/audit.log | audit2allow -M nginx
   - semodule -i nginx.pp
   - rferesh the page
   
