.. _installwebappdirac:

=======================
Installing WebAppDIRAC
=======================

The first section describes the install procedure of the web framework. The configuration of the web will be presented in the next sections.
While not mandatory, NGINX (nginx.com) can be used to improve the performance of the web framework. 
The installation and configuration of NGINX will be presented in the last section.


Requirements
------------

It is required CERN supported OS (slc6, CentOS 7, etc.) distribution. We recommend you to use the latest official OS version.
Please follow the :ref:`server_requirements` instructions
to setup the machine. In principle there is no magic to install the web portal. It has to be installed as another DIRAC component...
When the machine is ready you can start to install the web portal. But before that you need the install_site.sh script and a minimal configuration file.

Getting the install script
~~~~~~~~~~~~~~~~~~~~~~~~~~
You can found the instruction about were to get the install_site.sh at the end of the :ref:`server_requirements` section.

Configuration file
~~~~~~~~~~~~~~~~~~
You can use a standard configuration file for example :ref:`install_primary_server`. Please make sure that the following lines are exists in the
configuration file::

   Extensions = WebApp
   WebApp = yes

$installCfg::

   LocalInstallation
   {
     #
     #   These are options for the installation of the DIRAC software
     #
     #  DIRAC release version (this is an example, you should find out the current
     #  production release)
     Release = v6r20p14
     #  Python version of the installation
     PythonVersion = 27
     #  To install the Server version of DIRAC (the default is client)
     InstallType = server
     #  If this flag is set to yes, each DIRAC update will be installed
     #  in a separate directory, not overriding the previous ones
     UseVersionsDir = yes
     #  The directory of the DIRAC software installation
     TargetPath = /opt/dirac
     #  DIRAC extension to be installed
     # (WebApp is required if you are installing the Portal on this server).
     #  Only modules not defined as default to install in their projects need to be defined here:
     #   i.e. LHCb, LHCbWeb for LHCb for example: Extensions = WebAppDIRAC,LHCb,LHCbWeb
     Extensions = WebApp
     Project = DIRAC
     WebPortal = yes
     WebApp = yes
     # Note: This service is only needed, if does not exist on the machine used to install the WebApp
     Services = Framework/SystemAdministrator
     UseServerCertificate = yes
     SkipCADownload = yes
     Setup = your setup # for example: LHCb-Certification
     ConfigurationMaster = no
     ConfigurationServer = your configuration service
   }


Before you start the installation please make sure that you have the host certificate in the /opt/dirac/etc directory. 
More info in the Server Certificates section in :ref:`server_requirements` .

Create the configuration file::

   - vim /home/dirac/DIRAC/install.cfg
   - copy the lines above the this file...
   - cd /home/dirac/DIRAC
   - curl -O https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/install_site.sh
   - chmod +x install_site.sh
   - ./install_site.sh install.cfg # use -v for specifying a version
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


If you are not using NGINX and the web server is listening on 8000, please open vim /opt/dirac/pro/WebAppDIRAC/WebApp/web.cfg and add Balancer=None.
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

We use **web.cfg** configuration file, which is used to configure the web framework. It also contains the schema of the menu under Schema section, which is used by the users. 
The structure of the web.cfg file is the following::

      WebApp
      {
        Balancer = None #[nginx] in case you have installed nginx
        #NumProcesses = 1
        #SSLProtocol = "" [PROTOCOL_SSLv2, PROTOCOL_SSLv23, PROTOCOL_SSLv3, PROTOCOL_TLSv1] in case you do not want to use the default protocol
        Theme = tabs #[desktop]

        Schema
        {
          Help = link|http://dirac.readthedocs.io/en/latest/UserGuide/index.html
          Tools
          {
            Application Wizard = DIRAC.ApplicationWizard
            Job Launchpad = DIRAC.JobLaunchpad
            Notepad = DIRAC.Notepad
            Proxy Upload = DIRAC.ProxyUpload
          }
          Applications
          {
            Accounting = DIRAC.Accounting
            Activity Monitor = DIRAC.ActivityMonitor
            Component History = DIRAC.ComponentHistory
            Configuration Manager = DIRAC.ConfigurationManager
            Downtimes = DIRAC.Downtimes
            File Catalog = DIRAC.FileCatalog
            Job Monitor = DIRAC.JobMonitor
            Job Summary = DIRAC.JobSummary
            Pilot Monitor = DIRAC.PilotMonitor
            Pilot Summary = DIRAC.PilotSummary
            Proxy Manager = DIRAC.ProxyManager
            Public State Manager = DIRAC.PublicStateManager
            Registry Manager = DIRAC.RegistryManager
            Request Monitor = DIRAC.RequestMonitor
            Resource Summary = DIRAC.ResourceSummary
            Site Summary = DIRAC.SiteSummary
            Space Occupancy = DIRAC.SpaceOccupancy
            System Administration = DIRAC.SystemAdministration
            Transformation Monitor = DIRAC.TransformationMonitor
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

The default location of the configuration file is /opt/dirac/pro/WebAppDIRAC/WebApp/web.cfg. This is the default configuration file which provided by
by the developer. If you want to change the default configuration file, you have to add the web.cfg to the directory where the dirac.cfg is found, for example: /opt/dirac/etc

If the web.cfg file exists in /opt/dirac/etc directory, this file will be used.

Note: The Web framework uses the Schema section for creating the menu. It shows the Schema content, without manipulating it for example: sorting the applications, or creating some structure. 
Consequently, if you want to sort the menu, you have to create your own configuration file and place the directory where dirac.cfg exists.   

Running multiple web instances
------------------------------

If you want to run more than one instance, you have to use NGIX. The configuration of the NGINX is 
described in the next section.

You can define the number of processes in the configuration file:  /opt/dirac/pro/WebAppDIRAC/WebApp/web.cfg

NumProcesses = x (by default the NumProcesses is 1), where x the number of instances, you want to run
Balancer = nginx

for example::
   NumProcesses = 4, the processes will listen on 8000, 8001, ... 800n

You can check the number of instances in the log file (runit/Web/WebApp/log/current)::

   2018-05-09 13:48:28 UTC WebApp/Web NOTICE: Configuring HTTP on port 8000
   2018-05-09 13:48:28 UTC WebApp/Web NOTICE: Configuring HTTP on port 8001
   2018-05-09 13:48:28 UTC WebApp/Web NOTICE: Configuring HTTP on port 8002
   2018-05-09 13:48:28 UTC WebApp/Web NOTICE: Configuring HTTP on port 8003
   2018-05-09 13:48:28 UTC WebApp/Web ALWAYS: Listening on http://0.0.0.0:8002/DIRAC/
   2018-05-09 13:48:28 UTC WebApp/Web ALWAYS: Listening on http://0.0.0.0:8000/DIRAC/
   2018-05-09 13:48:28 UTC WebApp/Web ALWAYS: Listening on http://0.0.0.0:8001/DIRAC/
   2018-05-09 13:48:28 UTC WebApp/Web ALWAYS: Listening on http://0.0.0.0:8003/DIRAC/

You have to configure NGINX to forward the requests to that ports::

   upstream tornadoserver {
       #One for every tornado instance you're running that you want to balance
       server 127.0.0.1:8000;
       server 127.0.0.1:8001;
       server 127.0.0.1:8002;
       server 127.0.0.1:8003;
   }

Note: you can run NGINX in a separate machine.


Install and configure NGINX
---------------------------

Note: you can run NGINX in a separate machine.

The official site of NGINX is the following: `<http://nginx.org/>`_
The required NGINX version has to be grater than 1.4 and WebDAV nginx module to serve static files.

* Prepare, needed the development repository to compile the WebDAV dynamic module for Nginx::

      yum update -y
      yum groupinstall "Development Tools" -y
      yum install yum-utils pcre-devel zlib-devel libxslt-devel libxml2-devel -y

* Install Nginx using package manager. At this point, you should be able to install the pre-built Nginx package with dynamic module support::

      yum install nginx -y
      systemctl enable nginx
      systemctl start nginx

If your version is not grater than 1.4 you have to install NGINX manually.

* Manual install::

      vim /etc/yum.repos.d/nginx.repo

CentOS::

      [nginx-stable]
      name=nginx stable repo
      baseurl=http://nginx.org/packages/centos/$releasever/$basearch/
      gpgcheck=1
      enabled=1
      gpgkey=https://nginx.org/keys/nginx_signing.key
      module_hotfixes=true

      [nginx-mainline]
      name=nginx mainline repo
      baseurl=http://nginx.org/packages/mainline/centos/$releasever/$basearch/
      gpgcheck=1
      enabled=0
      gpgkey=https://nginx.org/keys/nginx_signing.key
      module_hotfixes=true

RHEL::

      [nginx-stable]
      name=nginx stable repo
      baseurl=http://nginx.org/packages/rhel/$releasever/$basearch/
      gpgcheck=1
      enabled=1
      gpgkey=https://nginx.org/keys/nginx_signing.key
      module_hotfixes=true

      [nginx-mainline]
      name=nginx mainline repo
      baseurl=http://nginx.org/packages/mainline/rhel/$releasever/$basearch/
      gpgcheck=1
      enabled=0
      gpgkey=https://nginx.org/keys/nginx_signing.key
      module_hotfixes=true

Due to differences between how CentOS, RHEL, and Scientific Linux populate the $releasever variable, it is necessary to manually replace $releasever with either 5 (for 5.x) or 6 (for 6.x),
depending upon your OS version. For example::

      [nginx]
      ...
      baseurl=http://nginx.org/packages/rhel/6/$basearch/
      ...

If it is successful installed::

    Verifying: nginx-1.10.1-1.el6.ngx.x86_64                                                                                                                                                                                                                    1/1
    Installed:
      nginx.x86_64 0:1.10.1-1.el6.ngx

* Compile Module::

    Download the Nginx and the module source code, and you need to determine which Nginx version is running on your server.

    Determine running Nginx version:

    nginx -v
    nginx version: nginx/1.16.1

    Download the source code corresponding to the installed version:

    wget http://nginx.org/download/nginx-1.16.1.tar.gz

    Clone the module repository:

    git clone https://github.com/arut/nginx-dav-ext-module

    Change to the Nginx source code directory, compile the module, and copy it to the standard directory for the Nginx modules.

    cd nginx-1.16.1
    ./configure --with-compat --with-http_dav_module --add-dynamic-module=../nginx-dav-ext-module/
    make modules
    cp objs/ngx_http_dav_ext_module.so /etc/nginx/modules/

.. _configure_nginx:

* Configure NGINX

  You have to find the nginx.conf file. You can see which configuration used in /etc/init.d/nginx. For example::

    vim /etc/nginx/nginx.conf

  Make sure there is a line 'include /etc/nginx/conf.d/\*.conf;', then create a site.conf under /etc/nginx/conf.d/.

The content of the site.conf (please modify it!!!)::

   upstream tornadoserver {
     #One for every tornado instance you're running that you want to balance
     server 127.0.0.1:8000;
   }

   server {
     # Use always HTTPS
     listen 80 default_server;
     listen [::]:80 default_server;
     # Your server name if you have weird network config. Otherwise leave commented
     #server_name dzmathe.cern.ch;
     return 301 https://$server_name$request_uri;
   }

   server {
     # Enabling HTTP/2
     listen 443 ssl http2 default_server;      # For IPv4
     listen [::]:443 ssl http2 default_server; # For IPv6
     server_name dzmathe.cern.ch;              # Server domain name

     ssl_prefer_server_ciphers On;
     ssl_protocols TLSv1 TLSv1.1 TLSv1.2;
     ssl_ciphers ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+3DES:!aNULL:!MD5:!DSS;

     # Certs that will be shown to the user connecting to the web.
     # Preferably NOT grid certs. Use something that the user cert will not complain about
     ssl_certificate     /opt/dirac/etc/grid-security/hostcert.pem;
     ssl_certificate_key /opt/dirac/etc/grid-security/hostkey.pem;

     ssl_session_tickets off;

     # Diffie-Hellman parameter for DHE ciphersuites, recommended 2048 bits
     # Generate your DH parameters with OpenSSL:
     # ~ cd /etc/nginx/ssl
     # ~ openssl dhparam -out dhparam.pem 4096 
     ssl_dhparam /etc/nginx/ssl/dhparam.pem;

     # HSTS (ngx_http_headers_module is required) (15768000 seconds = 6 months)
     add_header Strict-Transport-Security max-age=15768000;

     # To secure NGINX from Click-jacking attack
     add_header X-Frame-Options SAMEORIGIN always;

     # OCSP Stapling --- fetch OCSP records from URL in ssl_certificate and cache them
     ssl_stapling on;
     ssl_stapling_verify on;

     # DNS resolver for stapling so that the resolver defaults to Google’s DNS
     resolver 8.8.4.4 8.8.8.8;

     ssl_client_certificate /opt/dirac/pro/etc/grid-security/cas.pem;
     # ssl_crl /opt/dirac/pro/etc/grid-security/allRevokedCerts.pem;
     ssl_verify_client optional;
     ssl_verify_depth 10;
     ssl_session_cache shared:SSL:10m;

     root /opt/dirac/pro;

    # The same directory must be exist with 'rw' permissions for all
    location /files {
      # Access for GET requests without certificate
      if ($request_method = GET) {
        # Webdav sever
        error_page 418 = @webdav;
        return 418;
      }

      # For not GET requests access only with client certificate verification
      if ($ssl_client_verify = NONE) {
        return 403 'certificate not found';
      }
      if ($ssl_client_verify != SUCCESS) {
        return 403 'certificate verify failed';
      }

      # Webdav sever
      error_page 418 = @webdav;
      return 418;
    }

    location @webdav {
        satisfy any;
        # Read access for all
        limit_except GET {
          # Add allowed IP for not GET requests
          #allow XXX.XXX.XXX.XXX;
          deny  all;
        }
        client_max_body_size 1g;
        root /opt/dirac/webRoot/www/;
        # Access settings
        dav_access group:rw all:rw;
        # Allow all posible methods
        dav_methods PUT DELETE MKCOL COPY MOVE;
        # For webdav clients (Cyberduck and Monosnap)
        dav_ext_methods PROPFIND OPTIONS;
        # Clients can create paths
        create_full_put_path on;
        charset utf-8;
        autoindex on;
        break;
    }

     location ~ ^/[a-zA-Z]+/(s:.*/g:.*/)?static/(.+\.(jpg|jpeg|gif|png|bmp|ico|pdf))$ {
       alias /opt/dirac/pro/;
       # Add one more for every static path. For instance for LHCbWebDIRAC:
       # try_files LHCbWebDIRAC/WebApp/static/$2 WebAppDIRAC/WebApp/static/$2 /;
       try_files WebAppDIRAC/WebApp/static/$2 /;
       expires 10d;
       gzip_static on;
       gzip_disable "MSIE [1-6]\.";
       add_header Cache-Control public;
       break;
     }

     location ~ ^/[a-zA-Z]+/(s:.*/g:.*/)?static/(.+)$ {
       alias /opt/dirac/pro/;
       # Add one more for every static path. For instance for LHCbWebDIRAC:
       # try_files LHCbWebDIRAC/WebApp/static/$2 WebAppDIRAC/WebApp/static/$2 /;
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

Make sure the directory exists with the necessary permissions:

   mkdir /opt/dirac/webRoot/www/files
   chmod 666 /opt/dirac/webRoot/www/files
   chown dirac:dirac /opt/dirac/webRoot/www/files

You can start NGINX now.

* Start, Stop and restart nginx::

   /etc/init.d/nginx start|stop|restart


You have to add to the web.cfg the following lines in order to use NGINX::

       DevelopMode = False
       Balancer = nginx
       NumProcesses = 1

In that case one process will be used and this process is listening on 8000 port.
 You can try to use the web portal. For example: http://dzmathe.cern.ch/DIRAC/
 If you get 502 Bad Gateway error, you need to generate rules for SE linus.
 You can see the error in tail -200f /var/log/nginx/error.log::

     016/06/02 15:55:24 [crit] 20317#20317: *4 connect() to 127.0.0.1:8000 failed (13: Permission denied) while connecting to upstream, client: 128.141.170.23, server: dzmathe.cern.ch, request: "GET /DIRAC/?view=tabs&theme=Grey&url_state=1| HTTP/1.1", upstream: "http://127.0.0.1:8000/DIRAC/?view=tabs&theme=Grey&url_state=1|", host: "dzmathe.cern.ch"

* Generate the the rule::
   - grep nginx /var/log/audit/audit.log | audit2allow -M nginx
   - semodule -i nginx.pp
   - rferesh the page
