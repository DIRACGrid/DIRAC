.. _VMDIRAC:

=======
VMDIRAC
=======

.. toctree::
   :maxdepth: 2

.. contents:: Table of contents
   :depth: 4

---------------
Install VMDIRAC
---------------

On the server running the WMS:

* Install VMDIRAC extension as any other DIRAC extension using -e option *e.g.*:

  ::

      ./dirac-install -l $release-project -r $release_version -e VMDIRAC

  Note that in the server local configuration you should have the following option set in the LocalInstallation and DIRAC sections:

  ::

      Extensions = VMDIRAC

* Server local configuration

  In the server local configuration add the User/Password information to connect to the Cloud endpoint. Also you should put a valid path for the host certificate, *e.g.*:

  ::

      Resources
      {
        Sites
        {
          Cloud
          {
            Cloud.LUPM.fr
            {
              Cloud
              {
                194.214.86.244
                {
                  User = xxxx
                  Password = xxxx
                  HostCert = /opt/dirac/etc/grid-security/hostcert.pem
                  HostKey = /opt/dirac/etc/grid-security/hostkey.pem
                  CreatePublicIP = True
                }
              }
            }
          }
        }
      }

* Install the following components:
  
  * DB: VirtualMachineDB
  * Service: WorkloadManagement_VirtualMachineManager 
  * Agent: WorkloadManagement_CloudDirector

-------------
Configuration
-------------

* In the CS Resources section, configure the cloud endpoint as in this example

  ::

      Resources
      {
        Sites
        {
          Cloud
          {
            Cloud.LUPM.fr
            {
              CE = 194.214.86.244
              Cloud
              {
                194.214.86.244
                {
                  CEType = Cloud
                  ex_security_groups = default
                  ex_force_auth_url = http://194.214.86.244:5000/v3/auth/tokens
                  ex_force_service_region = LUPM-CLOUD
                  # This is the max number of VM instances that will be running in parallel
                  # Each VM can have multiple cores, each one executing a job
                  MaxInstances = 4
                  ex_force_auth_version = 3.x_password
                  ex_tenant_name = dirac
                  ex_domain_name = msfg.fr
                  networks = dirac-net
                  # This is the public key previously uploaded to the Cloud provider
                  # It's needed to ssh connect to VMs
                  keyname = cta_cloud_lupm
                  # If this option is set, public IP are assigned to VMs 
                  # It's needed to ssh connect to VMs
                  ipPool = ext-net
                }
                Images
                {
                  # It can be a public or a private image
                  Centos6-Officielle
                  {
                    ImageID = 35403255-f5f1-4c61-96dc-e59678942c6d
                    FlavorName = m1.medium
                  }
                }
              }
            }
          }
        }
      }


* CS Operation section

  ::
     
      Operations
      {
        CTA
        {
          Cloud
          {
            GenericCloudGroup = cta_genpilot
            GenericCloudUser = arrabito
            user_data_commands = vm-bootstrap
            user_data_commands += vm-bootstrap-functions
            user_data_commands += vm-pilot
            user_data_commands += vm-monitor-agent
            user_data_commands += pilotCommands.py
            user_data_commands += pilotTools.py
            user_data_commands += dirac-install.py
            user_data_commands += power.sh
            user_data_commands += parse-jobagent-log
            user_data_commands += dirac-pilot.py
            user_data_commands += save-payload-logs
            # url from which command scripts are downloaded. Usually the url of the web server
            user_data_commands_base_url = http://cta-dirac.in2p3.fr/DIRAC/defaults
            Project = CTA
            Version = v1r40
          }
        }
      }

* CS Registry section

  The host where VMDIRAC is installed and the certificate of which is used for the VMs, it should have these 2 properties set (as in the example below):

    * Properties = GenericPilot (needed to make pilots running on the VM matching jobs in the TaskQueue)
    * Properties = VmRpcOperation (needed by the VirtualMachineMonitorAgent running on the VM to be authorized to send Heartbeats to the VirtualMachineManager service)
    
    ::
       
      Registry
      {
        Hosts
        {
          dcta-agents01.pic.es
          {
            DN = /DC=org/DC=terena/DC=tcs/C=ES/ST=Barcelona/L=Bellaterra/O=Institut de Fisica dAltes Energies/CN=dcta-agents01.pic.es
            CA = /C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience SSL CA 3
            Properties = FullDelegation
            Properties += CSAdministrator
            Properties += ProxyManagement
            Properties += SiteManager
            Properties += Operator
            Properties += JobAdministrator
            Properties += CSAdministrator
            Properties += TrustedHost
            Properties += GenericPilot
            Properties += VmRpcOperation
          }
        }
      }

----------------------
Install VMDIRAC WebApp
----------------------

* On the DIRAC web server install VMDIRAC WebApp as a normal extension. In the server local configuration you should set the following option in the LocalInstallation and DIRAC sections:

  ::

      Extensions = VMDIRAC

* Create sym links for the bootstrap scripts

    ::
       
       $ ll /opt/dirac/webRoot/www/defaults/bootstrap
       total 0
       lrwxrwxrwx 1 dirac dirac 50 Feb 21 08:46 dirac-install.py -> /opt/dirac/pro/DIRAC/Core/scripts/dirac-install.py
       lrwxrwxrwx 1 dirac dirac 71 Feb 21 08:49 dirac-pilot.py -> /opt/dirac/pro/DIRAC/WorkloadManagementSystem/PilotAgent/dirac-pilot.py
       lrwxrwxrwx 1 dirac dirac 76 Feb 21 08:50 parse-jobagent-log -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/parse-jobagent-log
       lrwxrwxrwx 1 dirac dirac 73 Feb 21 08:51 pilotCommands.py -> /opt/dirac/pro/DIRAC/WorkloadManagementSystem/PilotAgent/pilotCommands.py
       lrwxrwxrwx 1 dirac dirac 70 Feb 21 08:51 pilotTools.py -> /opt/dirac/pro/DIRAC/WorkloadManagementSystem/PilotAgent/pilotTools.py
       lrwxrwxrwx 1 dirac dirac 66 Feb 21 08:52 power.sh -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/power.sh
       lrwxrwxrwx 1 dirac dirac 75 Feb 21 08:52 save-payload-logs -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/save-payload-logs
       lrwxrwxrwx 1 dirac dirac 70 Feb 21 11:47 vm-bootstrap -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/vm-bootstrap
       lrwxrwxrwx 1 dirac dirac 80 Feb 21 08:52 vm-bootstrap-functions -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/vm-bootstrap-functions
       lrwxrwxrwx 1 dirac dirac 74 Feb 21 08:53 vm-monitor-agent -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/vm-monitor-agent
       lrwxrwxrwx 1 dirac dirac 66 Feb 21 08:53 vm-pilot -> /opt/dirac/pro/VMDIRAC/WorkloadManagementSystem/Bootstrap/vm-pilot

  
