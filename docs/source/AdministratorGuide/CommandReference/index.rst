====================================
Administrator Command Reference
====================================

In this subsection all the dirac-admin commands available are explained. You can 
get up-to-date documentation by using the -h switch on any of them. The following command line 
flags are common to all DIRAC scripts making use of the *parseCommandLine* method of the base *Script* class::

      General options: 
        -o:  --option=         : Option=value to add 
        -s:  --section=        : Set base section for relative parsed options 
        -c:  --cert=           : Use server certificate to connect to Core Services 
        -d   --debug           : Set debug mode (-dd is extra debug) 
        -h   --help            : Shows this help 

General information:

.. toctree::
    :maxdepth: 2

    dirac-admin-service-ports
    dirac-platform

.. _registry_cmd:

Managing Registry:

.. toctree::
    :maxdepth: 2   
     
    dirac-admin-add-group
    dirac-admin-add-host
    dirac-admin-add-user
    dirac-admin-add-shifter
    dirac-admin-delete-user        
    dirac-admin-list-hosts
    dirac-admin-list-users
    dirac-admin-modify-user
    dirac-admin-sync-users-from-file
    dirac-admin-user-quota
    dirac-admin-users-with-proxy
    dirac-admin-voms-sync

Managing Resources:

.. toctree::
    :maxdepth: 2   
    
    dirac-admin-add-site
    dirac-admin-add-resources
    dirac-admin-allow-se
    dirac-admin-allow-site
    dirac-admin-ban-se
    dirac-admin-ban-site
    dirac-admin-ce-info
    dirac-admin-get-banned-sites
    dirac-admin-get-site-mask
    dirac-admin-set-site-protocols
    dirac-admin-site-info
    dirac-admin-site-mask-logging
    
Workload management commands:

.. toctree::
    :maxdepth: 2

    dirac-admin-get-job-pilot-output
    dirac-admin-get-job-pilots
    dirac-admin-get-pilot-info
    dirac-admin-get-pilot-logging-info
    dirac-admin-get-pilot-output
    dirac-admin-kill-pilot
    dirac-admin-pilot-summary
    dirac-admin-pilot-logging-info
    dirac-admin-reset-job
    dirac-admin-show-task-queues
    dirac-jobexec

Transformation management commands:

.. toctree::
    :maxdepth: 2

    dirac-transformation-add-files
    dirac-transformation-archive
    dirac-transformation-clean
    dirac-transformation-cli
    dirac-transformation-get-files
    dirac-transformation-recover-data
    dirac-transformation-remove-output
    dirac-transformation-replication
    dirac-transformation-verify-outputdata

Managing DIRAC installation:

.. toctree::
    :maxdepth: 2

    dirac-framework-ping-service
    dirac-install-component
    dirac-install-db
    dirac-install-web-portal
    dirac-install-tornado-service
    dirac-install
    dirac-install-extension
    dirac-uninstall-component
    dirac-restart-component
    dirac-start-component
    dirac-status-component
    dirac-stop-component
    dirac-monitoring-get-components-status
    dirac-service
    dirac-setup-site
    dirac-configure
    dirac-admin-get-CAs
    dirac-info
    dirac-version
    dirac-admin-check-config-options
    dirac-populate-component-db

Managing DIRAC software:

.. toctree::
    :maxdepth: 2
    
    dirac-deploy-scripts
    dirac-externals-requirements

User convenience:

.. toctree::
    :maxdepth: 2
    
    dirac-accounting-decode-fileid
    dirac-cert-convert.sh
    dirac-myproxy-upload
    dirac-utils-file-adler
    dirac-utils-file-md5

.. _proxymanager_cmd:

ProxyManager management commands:

.. toctree::
    :maxdepth: 2

    dirac-proxy-info
    dirac-proxy-init
    dirac-proxy-destroy
    dirac-admin-get-proxy
    dirac-admin-proxy-upload
    dirac-proxy-get-uploaded-info

Other commands:

.. toctree::
    :maxdepth: 2

    dirac-admin-accounting-cli
    
    dirac-admin-sysadmin-cli
    dirac-admin-update-instance
    dirac-admin-update-pilot
    dirac-admin-sync-pilot

    dirac-admin-sort-cs-sites

    dirac-configuration-cli
    dirac-configuration-dump-local-cache
    dirac-configuration-shell


    dirac-repo-monitor

    dirac-rss-list-status
    dirac-rss-query-db
    dirac-rss-query-dtcache
    dirac-rss-set-status
    dirac-rss-set-token
    dirac-rss-sync

    dirac-stager-monitor-file
    dirac-stager-monitor-jobs
    dirac-stager-monitor-request
    dirac-stager-monitor-requests
    dirac-stager-show-stats
    dirac-stager-stage-files

    install_site.sh
    
    dirac-agent
    dirac-executor

    dirac-sys-sendmail
